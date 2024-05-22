# SPDX-License-Identifier: Apache-2.0

"""Parameter resolving utils."""

import re

from blue_cwl.core.cwl_types import Directory, File
from blue_cwl.core.exceptions import ReferenceResolutionError

# get the index from an array e.g. foo[1] -> 1
RE_ARRAY_INDEX_PATTERN = r"^.*?\[[^\d]*(\d+)[^\d]*\].*$"
RE_ARRAY_INDEX = re.compile(RE_ARRAY_INDEX_PATTERN)


def resolve_parameter_references(
    expression: str | list | dict,
    inputs: dict | None = None,
    context: dict | None = None,
    runtime: dict | None = None,
):
    """Resolve parameter references."""
    if isinstance(expression, list):
        return [
            resolve_parameter_references(element, inputs, context, runtime)
            for element in expression
        ]

    if isinstance(expression, dict):
        return {
            key: resolve_parameter_references(element, inputs, context, runtime)
            for key, element in expression.items()
        }

    references = {
        "inputs": inputs or {},
        "self": context or {},
        "runtime": runtime or {},
    }

    matches = re.findall(r"\$\((.*?)\)", expression)

    if not matches:
        return expression

    match_keys = _matches_to_keys(matches)
    match_values = _find_reference_values(match_keys, references)

    # also covers a reference to a list
    if len(match_values) == 1:
        value = next(iter(match_values.values()))
        if isinstance(value, list):
            return value

    result = expression
    for match, value in match_values.items():
        result = result.replace(f"$({match})", str(value))

    return result


def _matches_to_keys(matches: list[str]) -> dict[str, list[str | int]]:
    """Split matches to nested keys.

    Example : inputs.v1 -> [inputs, v1]
    """
    return {match: _parse(match) for match in matches}


def _parse(match):
    first_key, *keys = match.split(".")

    if found := RE_ARRAY_INDEX.match(first_key):
        str_index = found.group(1)

        return [first_key.replace(f"[{str_index}]", ""), int(str_index)] + keys

    return [first_key] + keys


def _find_reference_values(
    match_keys: dict[str, list[str | int]], references: dict
) -> dict[str, str]:
    """Return a dictionary with match keys and values to replace the match keys."""

    def walk_dict(keys, dictionary):
        current = dictionary
        for key in keys:
            # list source referenced with self[i]
            if (isinstance(current, list) and isinstance(key, int)) or isinstance(current, dict):
                current = current[key]
            else:
                # e.g. File, Directory
                current = getattr(current, key)

        if isinstance(current, File | Directory):
            raise ValueError(f"Parameter resolution should return a string. Got {type(current)}")

        return current

    reference_values = {}
    for found, keys in match_keys.items():
        try:
            reference_values[found] = walk_dict(keys, references)
        except Exception as e:
            raise ReferenceResolutionError(
                "Reference resolution failed.\n"
                f"Match: {found}\n"
                f"Keys: {keys}\n"
                f"Existing references:\n{references}\n"
            ) from e

    return reference_values
