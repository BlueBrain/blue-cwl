from pathlib import Path
import pytest
import libsonata
from cwl_registry import population_utils as test_module

DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture
def population():
    return libsonata.NodeStorage(DATA_DIR / "nodes_100.h5").open_population("root__neurons")


def test_get_HRM_properties(population):
    res = test_module._get_HRM_properties(population, ["x", "y", "z"])
    assert len(res) == 100
    assert res.columns.tolist() == ["x", "y", "z"]
    assert res.index.names == ["hemisphere", "region", "mtype"]


def test_get_HRM_counts(population):
    res = test_module.get_HRM_counts(population)

    assert sum(res == 0) == 0
    assert sum(res) == 100
