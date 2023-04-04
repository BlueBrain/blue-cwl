import json
import libsonata
from pathlib import Path
from tests.functional.utils import WrapperBuild

from entity_management.simulation import DetailedCircuit

import pytest


@pytest.fixture(scope="module")
def output_dir(tmpdir_factory):
    return Path(tmpdir_factory.mktemp("cell-position"))


@pytest.fixture(scope="module")
def cell_position(output_dir):
    inputs = {
        "region": "http://api.brain-map.org/api/v2/data/Structure/322?rev=16",
        "variant-config": "https://bbp.epfl.ch/neurosciencegraph/data/0f3562ba-c393-4b7b-96c6-7d1a4259d8a7?rev=1",
        "cell-composition": "https://bbp.epfl.ch/neurosciencegraph/data/c5c3f334-0ba4-4149-b1dd-0abc799cf23a",
        "output-dir": str(output_dir),
    }
    command = [
        "-vv",
        "execute",
        "neurons-cell-position",
    ]
    return WrapperBuild(command=command, inputs=inputs)


@pytest.fixture(scope="module")
def circuit_resource(cell_position):
    """Return output circuit resource."""
    return DetailedCircuit.from_id(cell_position.output_id)


def test_detailed_circuit_compatibility(circuit_resource):
    assert circuit_resource.circuitConfigPath is not None


@pytest.fixture(scope="module")
def circuit_config_path(circuit_resource):
    """Return output circuit config path."""
    return circuit_resource.circuitConfigPath.url[7:]


def test_circuit_config_layout(circuit_config_path, output_dir):
    config_data = json.loads(Path(circuit_config_path).read_bytes())
    assert config_data == {
        "version": 2,
        "manifest": {"$BASE_DIR": "."},
        "node_sets_file": str(output_dir / "build/node_sets.json"),
        "networks": {
            "nodes": [
                {
                    "nodes_file": str(output_dir / "build/nodes.h5"),
                    "populations": {
                        "SSp__neurons": {
                            "type": "biophysical",
                            "partial": ["cell-properties"],
                        }
                    },
                }
            ],
            "edges": [],
        },
        "metadata": {"status": "partial"},
    }


@pytest.fixture(scope="module")
def circuit_config(circuit_config_path):
    return libsonata.CircuitConfig.from_file(circuit_config_path)


def test_node_sets(circuit_config):
    data = json.loads(Path(circuit_config.node_sets_path).read_bytes())
    assert data == {
        "All": {"population": "SSp__neurons"},
        "Excitatory": {"synapse_class": "EXC"},
        "Inhibitory": {"synapse_class": "INH"},
        "L23_LBC": {"mtype": "L23_LBC"},
        "bAC": {"etype": "bAC"},
        "SSp-bfd2": {"region": "SSp-bfd2"},
        "SSp-bfd3": {"region": "SSp-bfd3"},
        "SSp-ll2": {"region": "SSp-ll2"},
        "SSp-ll3": {"region": "SSp-ll3"},
        "SSp-m2": {"region": "SSp-m2"},
        "SSp-m3": {"region": "SSp-m3"},
        "SSp-n2": {"region": "SSp-n2"},
        "SSp-n3": {"region": "SSp-n3"},
        "SSp-tr2": {"region": "SSp-tr2"},
        "SSp-tr3": {"region": "SSp-tr3"},
        "SSp-ul2": {"region": "SSp-ul2"},
        "SSp-ul3": {"region": "SSp-ul3"},
        "SSp-un2": {"region": "SSp-un2"},
        "SSp-un3": {"region": "SSp-un3"},
        "SSp-ll2/3": ["SSp-ll2", "SSp-ll3"],
        "SSp-ll": ["SSp-ll2/3"],
        "SSp": ["SSp-bfd", "SSp-ll", "SSp-m", "SSp-n", "SSp-tr", "SSp-ul", "SSp-un"],
        "SS": ["SSp"],
        "Isocortex": ["SS"],
        "CTXpl": ["Isocortex"],
        "CTX": ["CTXpl"],
        "CH": ["CTX"],
        "grey": ["CH"],
        "root": ["grey"],
        "SSp-m2/3": ["SSp-m2", "SSp-m3"],
        "SSp-m": ["SSp-m2/3"],
        "SSp-un2/3": ["SSp-un2", "SSp-un3"],
        "SSp-un": ["SSp-un2/3"],
        "SSp-n2/3": ["SSp-n2", "SSp-n3"],
        "SSp-n": ["SSp-n2/3"],
        "SSp-tr2/3": ["SSp-tr2", "SSp-tr3"],
        "SSp-tr": ["SSp-tr2/3"],
        "SSp-ul2/3": ["SSp-ul2", "SSp-ul3"],
        "SSp-ul": ["SSp-ul2/3"],
        "SSp-bfd2/3": ["SSp-bfd2", "SSp-bfd3"],
        "SSp-bfd": ["SSp-bfd2/3"],
    }


@pytest.fixture(scope="module")
def node_population(circuit_config):
    population_names = list(circuit_config.node_populations)
    assert len(population_names) == 1
    return circuit_config.node_population(population_names[0])


def test_node_population_properties(node_population):
    assert node_population.attribute_names == {
        "subregion",
        "synapse_class",
        "region",
        "mtype",
        "etype",
        "morph_class",
        "hemisphere",
        "x",
        "y",
        "z",
    }
