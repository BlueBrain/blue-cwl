import pytest

from tests.functional.utils import WrapperBuild

from entity_management.simulation import DetailedCircuit


@pytest.fixture(scope="module")
def morphology_assignment(tmpdir_factory):
    inputs = {
        "region": "http://api.brain-map.org/api/v2/data/Structure/322?rev=16",
        "mtype-morphologies": "https://bbp.epfl.ch/neurosciencegraph/data/69b30218-ca93-4b77-881a-80fccde2af1e?rev=1",
        "partial-circuit": "https://bbp.epfl.ch/neurosciencegraph/data/8aef7c10-6cf3-44ee-8e51-ddedb4f3dab5?rev=1",
        "variant-config": "https://bbp.epfl.ch/neurosciencegraph/data/eca1b2aa-0af9-4fe7-9492-bf7c0428f8a8?rev=1",
        "output-dir": tmpdir_factory.mktemp("placeholder-morphology-assignment"),
    }
    command = [
        "cwl-registry",
        "-vv",
        "execute",
        "placeholder-morphology-assignment",
    ]
    return WrapperBuild(command, inputs)


def test_placeholder_morphology_assignment_completes(morphology_assignment):
    pass


def test_detailed_circuit_compatibility(morphology_assignment):
    circuit = DetailedCircuit.from_id(morphology_assignment.output_id)
    assert circuit.circuitConfigPath is not None
