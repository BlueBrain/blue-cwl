import tempfile
import pytest
from unittest.mock import Mock
from pathlib import Path
from cwl_registry import nexus as tested
from kgforge.core import Resource


def _create_test_file(path, text):
    with open(path, "w", encoding="utf-8") as fd:
        fd.write(text)
    return path


@pytest.fixture
def mock_forge():

    with tempfile.TemporaryDirectory() as tdir:

        tdir = Path(tdir)

        parameters1_path = _create_test_file(tdir / "parameters1.yml", "foo1")
        parameters2_path = _create_test_file(tdir / "parameters2.yml", "foo2")
        definitions1_path = _create_test_file(tdir / "definitions1.cwl", "foo3")

        mock_variant = Resource(
            type="VariantConfig",
            generator_name="foo",
            variant_name="bar",
            version="0.1.0",
            configs=Resource(id="variant-parameters-id", type="VariantParameters"),
            allocation_resources=Resource(id="variant-resources-id", type="VariantResources"),
            definitions=Resource(id="variant-definitions-id", type="VariantDefinitions"),
        )
        mock_parameters = Resource(
            generator_name="foo",
            variant_name="bar",
            version="0.1.0",
            type="VariantParameters",
            hasPart=[
                Resource(
                    distribution=Resource(
                        atLocation=Resource(location=f"file://{str(parameters1_path)}"),
                        name="parameters1.yml",
                    ),
                ),
                Resource(
                    distribution=Resource(
                        atLocation=Resource(location=f"file://{str(parameters2_path)}"),
                        name="parameters2.yml",
                    ),
                ),
            ],
        )
        mock_definitions = Resource(
            generator_name="foo",
            variant_name="bar",
            version="0.1.0",
            type="VariantDefinitions",
            hasPart=[
                Resource(
                    distribution=Resource(
                        atLocation=Resource(location=f"file://{str(definitions1_path)}"),
                        name="definitions1.cwl",
                    ),
                ),
            ],
        )
        mock_resources = Resource(
            generator_name="foo",
            variant_name="bar",
            version="0.1.0",
            type="VariantDefinitions",
            hasPart=[],
        )
        mock_kg = {
            "variant-config-id": mock_variant,
            "variant-parameters-id": mock_parameters,
            "variant-resources-id": mock_resources,
            "variant-definitions-id": mock_definitions,
        }

        mock = Mock()
        mock.tdir = tdir
        mock.retrieve = lambda resource_id, cross_bucket: mock_kg[resource_id]

        yield mock


def test_retrieve_variant_data(mock_forge):

    data = tested.retrieve_variant_data(mock_forge, "variant-config-id")

    assert data["configs"] == {
        "parameters1.yml": mock_forge.tdir / "parameters1.yml",
        "parameters2.yml": mock_forge.tdir / "parameters2.yml",
    }
    assert data["definitions"] == {
        "definitions1.cwl": mock_forge.tdir / "definitions1.cwl",
    }
    assert data["resources"] == {}


def test_retrieve_variant_data__with_staging(mock_forge):

    with tempfile.TemporaryDirectory() as tdir:
        tdir = Path(tdir)
        data = tested.retrieve_variant_data(mock_forge, "variant-config-id", staging_dir=tdir)

        assert data["configs"] == {
            "parameters1.yml": tdir / "configs/parameters1.yml",
            "parameters2.yml": tdir / "configs/parameters2.yml",
        }
        assert data["configs"]["parameters1.yml"].is_symlink()
        assert data["configs"]["parameters2.yml"].is_symlink()

        assert data["configs"]["parameters1.yml"].resolve() == mock_forge.tdir / "parameters1.yml"
        assert data["configs"]["parameters2.yml"].resolve() == mock_forge.tdir / "parameters2.yml"

        assert data["definitions"] == {
            "definitions1.cwl": tdir / "definitions/definitions1.cwl",
        }
        assert data["definitions"]["definitions1.cwl"].is_symlink()
        assert (
            data["definitions"]["definitions1.cwl"].resolve()
            == mock_forge.tdir / "definitions1.cwl"
        )

        assert data["resources"] == {}
