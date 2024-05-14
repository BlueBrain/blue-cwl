import os
import pytest
from pathlib import Path
from unittest.mock import patch, Mock
from blue_cwl.wrappers.mmodel import _app
from blue_cwl.wrappers import mmodel as test_module
from blue_cwl.testing import WrapperBuild
import pytest

out_dir = Path(
    "/gpfs/bbp.cscs.ch/project/proj30/tickets/NSETM-1760-wrap-snakemake-with-luigi/zisis/tmp/out"
)

POP = (
    "/gpfs/bbp.cscs.ch/project/proj134/workflow-outputs/58391eb1-61c2-44bc-b78d-e16b6f685b39/cellPositionConfig/root/build/nodes.h5",
    "root__neurons",
)

CONFIG_PATH = "/gpfs/bbp.cscs.ch/project/proj134/workflow-outputs/58391eb1-61c2-44bc-b78d-e16b6f685b39/cellPositionConfig/root/build/config.json"


@pytest.fixture(scope="module")
def output_dir(tmpdir_factory):
    return Path(tmpdir_factory.mktemp("connectome-filtering"))


def _test_mmodel():
    config_id = "https://bbp.epfl.ch/neurosciencegraph/data/165faedc-ba54-4456-a811-c1f2168d87be"
    circuit_id = "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/b58a3e95-5e46-43ba-b21e-e861d09e4151"
    variant_id = "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/ae98bb83-dd32-4e72-a3c1-dfc658a8226a"

    # test_module._app(config_id, circuit_id, variant_id, out_dir, parallel=False)
    inputs = {
        "configuration": config_id,
        "partial-circuit": circuit_id,
        "variant-config": variant_id,
        "output-dir": out_dir,
    }
    base_command_list = [
        "blue-cwl",
        "-vv",
        "execute",
        "mmodel-neurons",
    ]
    return WrapperBuild(command=base_command_list, inputs=inputs)


def _test_run_topological_synthesis():
    test_module._execute_synthesis_command(
        input_nodes_file=out_dir / "build/initial_canonical_nodes.h5",
        tmd_parameters_file=out_dir / "build/tmd_parameters.json",
        tmd_distributions_file=out_dir / "build/tmd_distributions.json",
        region_structure_file=out_dir / "build/region_structure.yaml",
        atlas_dir=out_dir / "stage/atlas",
        output_dir=out_dir,
        output_nodes_file=out_dir / "build/final_canonical_nodes.h5",
        output_morphologies_dir=out_dir / "build/morphologies",
        seed=0,
    )


if __name__ == "__main__":
    test_mmodel()
    # test_run_topological_synthesis()
