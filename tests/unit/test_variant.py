import re
import tempfile
import pytest
from pathlib import Path
from cwl_registry.exceptions import CWLRegistryError
from cwl_registry import variant as tested


_VERSION = "v0.3.1"


@pytest.fixture
def variant():
    return tested.Variant.from_registry(
        generator_name="testing",
        variant_name="position",
        version=_VERSION,
    )


def test_variant__attributes(variant):
    assert variant.name == "position"
    assert variant.generator_name == "testing"
    assert variant.version == _VERSION


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


def test_get_variant__latest():
    variant = tested.Variant.from_registry("testing", "position")
    assert variant.version == "v0.3.1"


def test_check_directory_exists():
    with pytest.raises(CWLRegistryError, match="Directory 'asdf' does not exist."):
        tested._check_directory_exists(Path("asdf"))


def test_get_latest_release_dir():
    with tempfile.TemporaryDirectory() as tdir:
        Path(tdir, "v0.3.1").mkdir()
        Path(tdir, "v1.2.0").mkdir()
        Path(tdir, "v0.0.2").mkdir()

        res = tested._get_latest_release_dir(Path(tdir))

        assert res.name == "v1.2.0"


def test_check_directory_names():
    with tempfile.TemporaryDirectory() as tdir:
        Path(tdir, "dir1").mkdir()
        Path(tdir, "dir2").mkdir()
        Path(tdir, "file1").touch()
        Path(tdir, "file2").touch()

        expected = "Directory 'dir3' does not exist. Available names: ['dir1', 'dir2']"
        with pytest.raises(CWLRegistryError, match=re.escape(expected)):
            tested._check_directory_names(Path(tdir, "dir3"))


def test_sorted_versions():
    versions = [
        "v0.0.1",
        "v2",
        "v0.0.10",
        "v10.0.0",
        "v1.2",
        "v0.1.0",
        "v1.0.0",
        "v0.10.0",
        "v0.0.0",
    ]

    paths = [Path(f"generators/my-gen/my-var/{v}") for v in versions]

    res = [p.name for p in tested._sorted_versions(paths)]

    assert res == [
        "v0.0.0",
        "v0.0.1",
        "v0.0.10",
        "v0.1.0",
        "v0.10.0",
        "v1.0.0",
        "v1.2",
        "v2",
        "v10.0.0",
    ]


@pytest.mark.parametrize(
    "version, expected",
    [
        ("v0", True),
        ("v0.1", True),
        ("v0.1.dev0", False),
        ("v0.3.1", True),
        ("v0.3.1.dev0", False),
        ("v0.3.1.2", False),
        ("v10.0.0v0.1.2", False),
    ],
)
def test_is_release(version, expected):
    assert tested._is_release(version) is expected
