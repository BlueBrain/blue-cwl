import re
import tempfile
import pytest
from pathlib import Path
from cwl_registry.exceptions import CWLRegistryError
from cwl_registry import variant as tested


_VERSION = "v0.3.1"
EXPECTED_DEFINITION_DATA = {
    "cwlVersion": "v1.2",
    "class": "CommandLineTool",
    "id": "me_type_property",
    "label": "Morph-Electric type property generator",
    "stdout": "stdout.txt",
    "baseCommand": ["cwl-registry", "execute", "neurons-me-type-property"],
    "environment": {
        "env_type": "VENV",
        "path": "/gpfs/bbp.cscs.ch/project/proj134/workflows/environments/venv-config",
        "enable_internet": True,
    },
    "resources": {
        "default": {
            "partition": "prod_small",
            "nodes": 1,
            "exclusive": True,
            "time": "1:00:00",
            "ntasks": 1,
            "ntasks_per_node": 1,
            "cpus_per_task": 1,
        },
        "region": {
            "http://api.brain-map.org/api/v2/data/Structure/997": {
                "partition": "prod",
                "time": "2:00:00",
            }
        },
    },
    "inputs": [
        {"id": "region", "type": "string", "inputBinding": {"prefix": "--region"}},
        {"id": "atlas", "type": "NexusType", "inputBinding": {"prefix": "--atlas"}},
        {
            "id": "me_type_densities",
            "type": "NexusType",
            "inputBinding": {"prefix": "--me-type-densities"},
        },
        {
            "id": "variant_config",
            "type": "NexusType",
            "inputBinding": {"prefix": "--variant-config"},
        },
        {"id": "output_dir", "type": "Directory", "inputBinding": {"prefix": "--output-dir"}},
    ],
    "outputs": [
        {
            "id": "circuit_me_type_bundle",
            "type": "NexusType",
            "doc": "Circuit bundle with me-types and soma positions.",
            "outputBinding": {"glob": "me-type-property-partial-circuit.json"},
        }
    ],
}


@pytest.fixture
def variant():
    return tested.Variant.from_registry(
        generator_name="testing",
        variant_name="position",
        version=_VERSION,
    )


def test_variant__repr(variant):
    assert repr(variant) == f"Variant(testing, position, {_VERSION})"


def test_variant__attributes(variant):
    assert variant.variant_name == "position"
    assert variant.generator_name == "testing"
    assert variant.version == _VERSION
    assert variant.content == EXPECTED_DEFINITION_DATA


def test_variant__tool_definition(variant):
    assert variant.tool_definition is not None


def test_check_directory_exists():
    with pytest.raises(CWLRegistryError, match="Directory 'asdf' does not exist."):
        tested._check_directory_exists(Path("asdf"))


def test_check_directory_names():
    with tempfile.TemporaryDirectory() as tdir:
        Path(tdir, "dir1").mkdir()
        Path(tdir, "dir2").mkdir()
        Path(tdir, "file1").touch()
        Path(tdir, "file2").touch()

        expected = "Directory 'dir3' does not exist. Available names: ['dir1', 'dir2']"
        with pytest.raises(CWLRegistryError, match=re.escape(expected)):
            tested._check_directory_names(Path(tdir, "dir3"))
