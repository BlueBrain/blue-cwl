from pathlib import Path

import pytest

import voxcell

DATA_DIR = Path(__file__).parent / "data"


from cwl_registry import brain_regions


@pytest.fixture(scope="session")
def region_map():
    return voxcell.RegionMap.load_json(DATA_DIR / "mba_hierarchy_v3l23split.json")


@pytest.fixture(scope="session")
def annotation():
    return voxcell.VoxelData.load_nrrd(DATA_DIR / "atlas/brain_regions.nrrd")


@pytest.fixture
def region_volumes(region_map):
    return brain_regions.volumes(region_map, brain_regions.all_acronyms())
