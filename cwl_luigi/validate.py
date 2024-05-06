"""Validation utils."""

import json
from importlib import resources
from typing import Any

import jsonschema
import yaml

from cwl_luigi.exceptions import CWLError


def validate_schema(data: dict[str, Any], schema_name: str) -> None:
    """Validata data against the schema with 'schema_name'."""
    schema = _read_schema(schema_name)

    cls = jsonschema.validators.validator_for(schema)
    cls.check_schema(schema)
    validator = cls(schema)
    errors = validator.iter_errors(data)

    messages: list[str] = []
    for error in errors:
        if error.context:
            messages.extend(map(_format_error, error.context))
        else:
            messages.append(_format_error(error))

    if messages:
        raise CWLError("\n".join(messages) + f"\ndata:\n{_format_data(data)}")


def _format_data(data: dict) -> str:
    return json.dumps(data, indent=2)


def _read_schema(schema_name: str) -> dict[str, Any]:
    """Load a schema and return the result as a dictionary."""
    resource = resources.files("cwl_luigi") / "schemas" / schema_name
    content = resource.read_text()
    return yaml.safe_load(content)


def _format_error(error) -> str:
    paths = " -> ".join(map(str, error.absolute_path))
    return f"[{paths}]: {error.message}"
