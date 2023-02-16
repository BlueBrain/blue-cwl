import os
import json
import subprocess
from pathlib import Path

from click.testing import CliRunner

from cwl_registry.app.__main__ import main
from cwl_registry.nexus import get_forge

import pytest

CWD = Path(__file__).parent
EXECUTABLE_PATH = CWD / "run.sh"
OUTPUT_DIR = CWD / "out"


def _print_details(command, inputs):
    forge = get_forge()

    input_details = {}
    for key, value in inputs.items():
        if key == "output-dir":
            input_details[key] = str(value)
        else:
            r = forge.retrieve(value, cross_bucket=True)
            input_details[key] = {
                "id": value,
                "type": r.type,
                "url": r._store_metadata._self,
            }

    details = {
        "inputs": input_details,
        "env": {
            "NEXUS_BASE": os.getenv("NEXUS_BASE"),
            "NEXUS_ORG": os.getenv("NEXUS_ORG"),
            "NEXUS_PROJ": os.getenv("NEXUS_PROJ"),
        },
    }

    print(f"Test Command:\ncwl-registry {' '.join(command)}")
    print(json.dumps(details, indent=2))


@pytest.fixture(scope="module")
def inputs(tmpdir_factory):
    output_dir = tmpdir_factory.mktemp("cell-composition")

    return {
        "region": "http://api.brain-map.org/api/v2/data/Structure/322?rev=16",
        "atlas-release": "https://bbp.epfl.ch/neurosciencegraph/data/8586fff5-8212-424c-bb52-73b514e93422?rev=1",
        "base-density-distribution": "https://bbp.epfl.ch/neurosciencegraph/data/1f678feb-6d4b-4917-83a0-8966b27a1dc2?rev=2",
        "base-composition-summary": "https://bbp.epfl.ch/neurosciencegraph/data/f5094a3a-5dff-45d7-9b04-5db1a162e01a",
        "recipe": "https://bbp.epfl.ch/neurosciencegraph/data/99f0f32c-5757-4dfa-af70-539e079972bb?rev=4",
        "variant-config": "https://bbp.epfl.ch/neurosciencegraph/data/a5c5d83c-4f02-455d-87c1-17f75401d7d7?rev=1",
        "output-dir": output_dir,
    }


@pytest.fixture(scope="module")
def cell_composition(inputs):
    runner = CliRunner()

    command = [
        "-vv",
        "execute",
        "cell-composition-manipulation",
    ]
    arguments = [f"--{key}={value}" for key, value in inputs.items()]

    result = runner.invoke(
        main, command + arguments, env=os.environ, catch_exceptions=False, color=True
    )

    if result.exit_code != 0:
        _print_details(command + arguments, inputs)
    assert result.exit_code == 0, result.output


def test_cell_composition_completes(cell_composition):
    pass
