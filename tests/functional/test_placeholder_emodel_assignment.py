import pytest

from tests.functional.utils import WrapperBuild


@pytest.fixture(scope="module")
def emodel_assignment(tmpdir_factory):
    inputs = {
        "region": "http://api.brain-map.org/api/v2/data/Structure/322?rev=16",
        "partial-circuit": "https://bbp.epfl.ch/neurosciencegraph/data/8aef7c10-6cf3-44ee-8e51-ddedb4f3dab5?rev=1",
        "variant-config": "https://bbp.epfl.ch/neurosciencegraph/data/31cdc430-fd03-4669-844c-f398f5a72f5e?rev=1",
        "etype-emodels": "https://bbp.epfl.ch/neurosciencegraph/data/8fb6c394-b544-47fa-9f29-17defbdff09c?rev=1",
        "output-dir": tmpdir_factory.mktemp("placeholder-emodel-assignment"),
    }
    command = [
        "-vv",
        "execute",
        "placeholder-emodel-assignment",
    ]
    return WrapperBuild(command, inputs)


def test_placeholder_emodel_assignment_completes(emodel_assignment):
    pass
