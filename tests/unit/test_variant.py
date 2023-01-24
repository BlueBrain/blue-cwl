import pytest
from pathlib import Path
from cwl_registry.exceptions import CWLRegistryError
from cwl_registry import variant as tested


VERSION = "v0.3.0"


@pytest.fixture
def variant():
    return tested.Variant.from_registry(
        generator_name="me_type_property",
        variant_name="neurons_me_type_property",
        version=VERSION,
    )


def test_variant__attributes(variant):

    assert variant.name == "neurons_me_type_property"
    assert variant.generator_name == "me_type_property"
    assert variant.version == VERSION


def test_variant__get_config_file(variant):

    filepath = variant.get_config_file("parameters.yml")
    assert filepath == variant.configs["parameters.yml"]

    with pytest.raises(KeyError):
        variant.get_config_file("nonexistent.duh")


def test_variant__get_definition_file(variant):

    filepath = variant.get_definition_file("execute.cwl")
    assert filepath == variant.definitions["execute.cwl"]
    assert filepath == variant.execute_definition_file

    with pytest.raises(KeyError):
        variant.get_definition_file("nonexistent.duh")


def test_variant__get_resources_file(variant):

    filepath = variant.get_resources_file("variant_config.yml")
    assert filepath == variant.resources["variant_config.yml"]

    with pytest.raises(KeyError):
        variant.get_resources_file("nonexistent.duh")


def test_get_variant(variant):

    cwl_file = variant.execute_definition_file
    assert cwl_file.name == "execute.cwl"
    assert cwl_file.exists()

    configs = variant.configs
    assert set(configs) == {"parameters.yml"}

    resources = variant.resources
    assert set(resources) == {"variant_config.yml"}


def test_get_variant_spec():

    spec = tested._get_variant_spec("me_type_property", "neurons_me_type_property", "latest")
    assert spec == "generators/me_type_property/neurons_me_type_property/latest"


def test_check_directory_exists():

    with pytest.raises(CWLRegistryError, match="Directory 'asdf' does not exist."):
        tested._check_directory_exists(Path("asdf"))
