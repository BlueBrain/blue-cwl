from cwl_registry.testing import WrapperBuild


import pytest


@pytest.fixture(scope="module")
def cell_composition(tmpdir_factory):
    inputs = {
        "region": "http://api.brain-map.org/api/v2/data/Structure/322?rev=16",
        "base-cell-composition": "https://bbp.epfl.ch/neurosciencegraph/data/8ed7e1d5-a76d-4974-b522-1d962a0b6a6d",
        "configuration": "https://bbp.epfl.ch/neurosciencegraph/data/99f0f32c-5757-4dfa-af70-539e079972bb?rev=4",
        "variant-config": "https://bbp.epfl.ch/neurosciencegraph/data/a5c5d83c-4f02-455d-87c1-17f75401d7d7?rev=1",
        "output-dir": tmpdir_factory.mktemp("cell-composition"),
    }
    command = [
        "cwl-registry",
        "-vv",
        "execute",
        "cell-composition-manipulation",
    ]
    return WrapperBuild(command=command, inputs=inputs)


def test_output_resource_registered(cell_composition):
    input_composition = cell_composition.retrieve_input("base-cell-composition")
    output_composition = cell_composition.output
    assert output_composition.atlasRelease.id == input_composition.atlasRelease.id


def test_cell_composition_completes(cell_composition):
    pass
