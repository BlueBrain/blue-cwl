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


def test_get_pathways():
    node_population = libsonata.NodeStorage(DATA_DIR / "circuit/nodes.h5").open_population(
        "SSp__neurons"
    )
    edge_population = libsonata.EdgeStorage(DATA_DIR / "circuit/edges.h5").open_population(
        "SSp__neurons__SSp__neurons__chemical"
    )

    res = test_module.get_pathways(
        edge_population, node_population, node_population, ["hemisphere", "region", "mtype"]
    )

    assert res.columns.tolist() == [
        "source_hemisphere",
        "source_region",
        "source_mtype",
        "target_hemisphere",
        "target_region",
        "target_mtype",
    ]
