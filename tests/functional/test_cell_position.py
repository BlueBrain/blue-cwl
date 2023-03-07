from tests.functional.utils import WrapperBuild

from entity_management.simulation import DetailedCircuit

import pytest


@pytest.fixture(scope="module")
def cell_position(tmpdir_factory):
    inputs = {
        "region": "http://api.brain-map.org/api/v2/data/Structure/322?rev=16",
        "variant-config": "https://bbp.epfl.ch/neurosciencegraph/data/0f3562ba-c393-4b7b-96c6-7d1a4259d8a7?rev=1",
        "cell-composition": "https://bbp.epfl.ch/neurosciencegraph/data/c5c3f334-0ba4-4149-b1dd-0abc799cf23a?rev=1",
        "output-dir": tmpdir_factory.mktemp("cell-position"),
    }
    command = [
        "-vv",
        "execute",
        "neurons-cell-position",
    ]
    return WrapperBuild(command=command, inputs=inputs)


def test_cell_position_completes(cell_position):
    pass


def test_detailed_circuit_compatibility(cell_position):
    circuit = DetailedCircuit.from_id(cell_position.output_id)
    assert circuit.circuitConfigPath is not None
