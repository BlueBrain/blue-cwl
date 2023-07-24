import os
import json
import pytest
import libsonata
from pathlib import Path
from tests.functional.utils import WrapperBuild
from entity_management.simulation import DetailedCircuit

import subprocess

GPFS_DIR = Path("/gpfs/bbp.cscs.ch/project/proj12/NSE/cwl-registry/data/")


@pytest.fixture(scope="module")
def output_dir(tmpdir_factory):
    return Path(tmpdir_factory.mktemp("connectome-filtering"))


@pytest.fixture(scope="module")
def connectome_filtering(output_dir):
    inputs = {
        "configuration": "https://bbp.epfl.ch/neurosciencegraph/data/991cb27f-61c3-42ce-9848-74d2d76a6357?rev=2",
        "partial-circuit": "https://bbp.epfl.ch/neurosciencegraph/data/93e2a8de-073e-4fb9-93f1-629af0fcf2a1",
        "variant-config": "https://bbp.epfl.ch/neurosciencegraph/data/8fa64bf6-90e3-491d-98ff-18552510b3d2",
        "output-dir": str(output_dir),
    }
    base_command_list = [
        "cwl-registry",
        "-vv",
        "execute",
        "connectome-filtering-synapses",
    ]
    salloc_cmd = (
        "source /etc/profile.d/modules.sh && "
        "module load unstable spykfunc parquet-converters && "
        f"salloc --account={os.environ['SALLOC_ACCOUNT']} --partition=prod --nodes=1 "
        "--constraint=nvme --exclusive --time=1:00:00 srun --mpi=none sh -c '{cmd}'"
    )
    return WrapperBuild(command=base_command_list, inputs=inputs, salloc_cmd=salloc_cmd)


def test_completes(connectome_filtering):
    pass


@pytest.fixture(scope="module")
def circuit_resource(connectome_filtering):
    """Return output circuit resource."""
    return DetailedCircuit.from_id(connectome_filtering.output_id)


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
        "conductance",
        "conductance_scale_factor",
        "decay_time",
        "depression_time",
        "facilitation_time",
        "n_rrp_vesicles",
        "syn_property_rule",
        "u_hill_coefficient",
        "u_syn",
    }
