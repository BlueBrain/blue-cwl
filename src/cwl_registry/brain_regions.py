"""Brain regions helper functions module."""
import importlib
import json

import voxcell


def all_acronyms() -> list[str]:
    """Return all brain region acronyms in data/brain_regions.json"""
    path = importlib.resources.files("cwl_registry") / "data" / "brain_regions.json"
    data = json.loads(path.read_bytes())
    return data["acronyms"]


def volumes(region_map: voxcell.RegionMap, acronyms: list[str] | None = None) -> dict[str, int]:
    """Create a map of region acronyms and region volumes.

    Args:
        region_map: RegionMap of a parcellation ontology / hierarchy.
        acronyms: A list with region acronyms.

    Returns:
        A dictionary the keys of which are region acronyms and the values volumes in um^3.
        Example:
            {
                "AAA": 494562500.0,
                "CA1": 10209234375.0,
            }

    Note: The ontology has no notion of hemisphere.
    """
    df = region_map.as_dataframe().set_index("acronym")

    if not acronyms:
        acronyms = all_acronyms()

    return df.loc[acronyms]["regionVolume"].astype(float)
