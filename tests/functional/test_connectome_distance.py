import json
from pathlib import Path

import pytest
import libsonata
from tests.functional.utils import WrapperBuild

from entity_management.simulation import DetailedCircuit


GPFS_DIR = Path("/gpfs/bbp.cscs.ch/project/proj12/NSE/cwl-registry/data/")


@pytest.fixture(scope="module")
def output_dir(tmpdir_factory):
    return Path(tmpdir_factory.mktemp("dd-connectome"))


@pytest.fixture(scope="module")
def dd_connectome(output_dir):
    inputs = {
        "configuration": "https://bbp.epfl.ch/neurosciencegraph/data/91f750ff-3281-4e98-8ee4-5102c6aa0090?rev=6",
        "partial-circuit": "https://bbp.epfl.ch/neurosciencegraph/data/e240bfc9-d4fc-4935-8242-f2eec41e83c8?rev=2",
        "variant-config": "https://bbp.epfl.ch/neurosciencegraph/data/65ee5ee9-c640-421c-a618-484489c90d82?rev=1",
        "output-dir": str(output_dir),
    }
    command = ["cwl-registry", "-vv", "execute", "connectome-distance-dependent"]
    return WrapperBuild(command=command, inputs=inputs)


def test_completes(dd_connectome):
    pass


@pytest.fixture(scope="module")
def circuit_resource(dd_connectome):
    """Return output circuit resource."""
    return DetailedCircuit.from_id(dd_connectome.output_id)


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
        "networks": {
            "nodes": [
                {
                    "nodes_file": str(GPFS_DIR / "placeholder-morphology-assignment/nodes.h5"),
                    "populations": {
                        "SSp__neurons": {
                            "type": "biophysical",
                            "morphologies_dir": str(
                                GPFS_DIR / "placeholder-morphology-assignment/morphologies"
                            ),
                            "partial": ["cell-properties", "morphologies"],
                        }
                    },
                }
            ],
            "edges": [
                {
                    "edges_file": str(output_dir / "build/edges.h5"),
                    "populations": {
                        "SSp__neurons__SSp__neurons__chemical": {
                            "type": "chemical",
                        }
                    },
                }
            ],
        },
        "metadata": {"status": "partial"},
    }


@pytest.fixture(scope="module")
def circuit_config(circuit_config_path):
    return libsonata.CircuitConfig.from_file(circuit_config_path)


@pytest.fixture(scope="module")
def edge_population(circuit_config):
    population_names = list(circuit_config.edge_populations)
    assert len(population_names) == 1
    return circuit_config.edge_population(population_names[0])


def test_expected_edge_properties(edge_population):
    assert edge_population.attribute_names == {
        "afferent_center_x",
        "afferent_center_y",
        "afferent_center_z",
        "afferent_section_id",
        "afferent_section_pos",
        "afferent_section_type",
        "delay",
        "efferent_section_type",
        "syn_type_id",
    }
