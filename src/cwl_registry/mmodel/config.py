"""MModel config."""
from copy import deepcopy


def split_config(
    defaults: dict, configuration: dict, canonical_key: str, placeholder_key: str
) -> tuple[dict, dict]:
    """Split config."""
    default_canonicals = defaults[canonical_key]
    default_placeholders = defaults[placeholder_key]

    assert default_canonicals.keys() == default_placeholders.keys()
    assert configuration.keys() == {canonical_key}

    config = configuration[canonical_key]

    canonicals = {"hasPart": {}}
    placeholders = deepcopy(default_placeholders)

    for region_key, region_data in config.items():
        mtypes = {}
        for mtype_key, entry in region_data.items():
            final = entry.dict()

            if not entry.id:
                default = default_canonicals[region_key][mtype_key]
                final["id"] = default["@id"]
                if "rev" in default:
                    final["rev"] = default["rev"]

            if "rev" in final and not final["rev"]:
                del final["rev"]

            mtypes[mtype_key] = {"hasPart": {final.pop("id"): final}}
            del placeholders[region_key][mtype_key]

        canonicals["hasPart"][region_key] = {"hasPart": mtypes}
        if not placeholders[region_key]:
            del placeholders[region_key]

    return placeholders.dict(), canonicals
