import pytest
from tests.functional.utils import WrapperBuild


@pytest.fixture(scope="module")
def dd_connectome(tmpdir_factory):
    inputs = {
        "configuration": "https://bbp.epfl.ch/neurosciencegraph/data/91f750ff-3281-4e98-8ee4-5102c6aa0090?rev=6",
        "partial-circuit": "https://bbp.epfl.ch/neurosciencegraph/data/e240bfc9-d4fc-4935-8242-f2eec41e83c8?rev=2",
        "variant-config": "https://bbp.epfl.ch/neurosciencegraph/data/65ee5ee9-c640-421c-a618-484489c90d82?rev=1",
        "output-dir": tmpdir_factory.mktemp("dd-connectome"),
    }
    command = ["-vv", "execute", "connectome-distance-dependent"]
    return WrapperBuild(command=command, inputs=inputs)


def test_completes(dd_connectome):
    pass
