# SPDX-License-Identifier: Apache-2.0

"""Config splitting function."""

from copy import deepcopy

from blue_cwl.exceptions import CWLWorkflowError


def split_config(
    defaults: dict, configuration: dict, canonical_key: str, placeholder_key: str
) -> tuple[dict, dict]:
    """Use the configuration to create a placeholder and a canonical configuration."""
    default_canonicals = defaults[canonical_key]
    default_placeholders = defaults[placeholder_key]
    configuration = configuration[canonical_key]

    _check_consistency(default_canonicals, default_placeholders, configuration)

    canonicals = _isolate_canonicals(default_canonicals, configuration)
    placeholders = _create_placeholder_complement(default_placeholders, canonicals)
    canonicals = _apply_configuration_overrides(canonicals, configuration)

    return placeholders, canonicals


def _check_consistency(
    default_canonicals: dict, default_placeholders: dict, configuration: dict
) -> None:
    canonical_regions = set(default_canonicals["hasPart"].keys())
    placeholder_regions = set(default_placeholders["hasPart"].keys())
    configuration_regions = set(configuration.keys())

    if canonical_regions != placeholder_regions:
        raise CWLWorkflowError(
            "Default canonical and placeholder regions differ:\n"
            f"canonicals - placeholders: {canonical_regions - placeholder_regions}\n"
            f"placeholders - canonicals: {placeholder_regions - canonical_regions}"
        )

    mismatches = []
    for region_id in canonical_regions:
        canonical_mtypes = default_canonicals["hasPart"][region_id]["hasPart"].keys()
        placeholder_mtypes = default_placeholders["hasPart"][region_id]["hasPart"].keys()

        if canonical_mtypes != placeholder_mtypes:
            left = canonical_mtypes - placeholder_mtypes
            rght = placeholder_mtypes - canonical_mtypes

            final = []
            if left:
                final.append(f"\n\tcanonicals - placeholders: {left}")
            if rght:
                final.append(f"\n\tplaceholders - canonicals: {rght}")

            joined_final = "".join(final)
            mismatches.append(f"\nRegion: {region_id}{joined_final}")

    if mismatches:
        str_errors = "".join(sorted(mismatches))
        raise CWLWorkflowError(f"Default canonical and placeholder mtypes differ:{str_errors}")

    if not configuration_regions.issubset(canonical_regions):
        remaining = sorted(configuration_regions - canonical_regions)
        raise CWLWorkflowError(f"Configuration regions not in default canonicals:\n{remaining}")

    mismatches = []
    for region_id, region_data in configuration.items():
        mtypes = set(region_data.keys())
        canonical_mtypes = set(default_canonicals["hasPart"][region_id]["hasPart"].keys())

        if not mtypes.issubset(canonical_mtypes):
            diff = mtypes - canonical_mtypes
            mismatches.append(f"\nRegion: {region_id}\n\tIn config but not in canonicals: {diff}")

    if mismatches:
        str_errors = "\n".join(sorted(mismatches))
        raise CWLWorkflowError(
            f"Mtypes in configuration that are not in the default canonicals:{str_errors}"
        )


def _isolate_canonicals(default_canonicals: dict, configuration: dict) -> dict:
    canonicals = deepcopy(default_canonicals)

    # separate canonical and placeholder regions
    canonicals["hasPart"] = {
        region_id: canonicals["hasPart"][region_id] for region_id in configuration
    }
    for region_id, region_data in canonicals["hasPart"].items():
        region_data["hasPart"] = {
            mtype_id: region_data["hasPart"][mtype_id] for mtype_id in configuration[region_id]
        }

    return canonicals


def _create_placeholder_complement(default_placeholders: dict, canonicals: dict) -> dict:
    placeholders = deepcopy(default_placeholders)
    for region_id, region_data in canonicals["hasPart"].items():
        placeholder_region = placeholders["hasPart"][region_id]
        for mtype_id in region_data["hasPart"]:
            del placeholder_region["hasPart"][mtype_id]
        # if no mtypes left, remove region
        if not placeholder_region["hasPart"]:
            del placeholders["hasPart"][region_id]

    return placeholders


def _apply_configuration_overrides(canonicals, configuration) -> dict:
    canonicals = deepcopy(canonicals)

    for region_key, region_data in configuration.items():
        default_region = canonicals["hasPart"][region_key]
        for mtype_key, entry in region_data.items():
            default_mtype = default_region["hasPart"][mtype_key]
            new_entry_id, new_entry = _create_new_entry(entry, default_mtype["hasPart"])
            default_mtype["hasPart"] = {new_entry_id: new_entry}

    return canonicals


def _create_new_entry(entry, default):
    entry = deepcopy(entry)

    entry_id = entry.pop("@id", None)
    entry_rev = entry.get("_rev", None)

    # use with default if no id
    if not entry_id:
        default_id, default_rev = _get_canonical_entry(default)
        entry_id = default_id
        if default_rev and entry_rev is None:
            entry["_rev"] = default_rev

    return entry_id, entry


def _get_canonical_entry(entry):
    entry_id = list(entry.keys())[0]
    entry_rev = entry[entry_id].get("_rev", None)
    return entry_id, entry_rev
