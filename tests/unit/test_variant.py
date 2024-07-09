import re
from pathlib import Path
from unittest.mock import patch
import tempfile
import pytest
from pathlib import Path

from entity_management import nexus

from blue_cwl.exceptions import CWLRegistryError
from blue_cwl import variant as tested
from blue_cwl.utils import load_json


DATA_DIR = Path(__file__).parent / "data"


_VERSION = "v0.3.1"
EXPECTED_DEFINITION_DATA = {
    "cwlVersion": "v1.2",
    "class": "CommandLineTool",
    "id": "me_type_property",
    "label": "Morph-Electric type property generator",
    "stdout": "stdout.txt",
    "baseCommand": ["blue-cwl", "execute", "neurons-me-type-property"],
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
def variant_metadata():
    return load_json(DATA_DIR / "variant_metadata.json")


@pytest.fixture
def variant_file():
    return tested._get_variant_file("testing", "position", _VERSION)


@pytest.fixture
def variant_from_registry():
    return tested.Variant.from_registry(
        generator_name="testing",
        variant_name="position",
        version=_VERSION,
    )


@pytest.fixture
def variant_from_file():
    return tested.Variant.from_file(
        filepath=tested._get_variant_file("testing", "position", _VERSION),
        generator_name="testing",
        variant_name="position",
        version=_VERSION,
    )


@pytest.fixture
def variant_from_id(monkeypatch, variant_metadata):
    with patch("entity_management.nexus.load_by_id", return_value=variant_metadata):
        return tested.Variant.from_id(None)


@pytest.fixture
def variant_from_search(monkeypatch, variant_metadata):
    query_response = {"results": {"bindings": [{"id": {"value": "not-None"}}]}}

    with (
        patch("blue_cwl.variant.sparql_query", return_value=query_response),
        patch("entity_management.nexus.load_by_id", return_value=variant_metadata),
    ):
        return tested.Variant.from_search("testing", "position", _VERSION)


@pytest.mark.parametrize(
    "variant",
    ["variant_from_id", "variant_from_registry", "variant_from_file", "variant_from_search"],
)
def test_variant__repr(variant, request):
    variant = request.getfixturevalue(variant)
    assert repr(variant) == f"Variant(testing, position, {_VERSION})"


@pytest.mark.parametrize(
    "variant",
    ["variant_from_id", "variant_from_registry", "variant_from_file", "variant_from_search"],
)
def test_variant__attributes(variant, request):
    variant = request.getfixturevalue(variant)

    assert variant.variantName == "position"
    assert variant.generatorName == "testing"
    assert variant.version == _VERSION


@pytest.mark.parametrize(
    "variant",
    ["variant_from_registry", "variant_from_file"],
)
def test_variant__local_variants__get_id(variant, request):
    variant = request.getfixturevalue(variant)
    assert variant.get_id() is None


@pytest.mark.parametrize(
    "variant",
    ["variant_from_id", "variant_from_search"],
)
def test_variant__remote_variants__get_id(variant, request):
    variant = request.getfixturevalue(variant)
    assert variant.get_id() is not None


@pytest.mark.parametrize(
    "variant",
    ["variant_from_id", "variant_from_registry", "variant_from_file", "variant_from_search"],
)
def test_variant__get_content(variant, request, monkeypatch, variant_file):
    if variant in {"variant_from_id", "variant_from_search"}:
        monkeypatch.setattr(nexus, "download_file", lambda *args, **kwargs: variant_file)

    variant = request.getfixturevalue(variant)
    assert variant.get_content() == EXPECTED_DEFINITION_DATA


@pytest.mark.parametrize(
    "variant",
    ["variant_from_id", "variant_from_registry", "variant_from_file", "variant_from_search"],
)
def test_variant__tool_definition(variant, request, monkeypatch, variant_file):
    if variant in {"variant_from_id", "variant_from_search"}:
        monkeypatch.setattr(nexus, "download_file", lambda *args, **kwargs: variant_file)
    variant = request.getfixturevalue(variant)
    assert variant.tool_definition is not None


@pytest.mark.parametrize(
    "variant",
    ["variant_from_id", "variant_from_search", "variant_from_file", "variant_from_registry"],
)
def test_variant__evolve(variant, request, variant_file):
    variant = request.getfixturevalue(variant)
    new_variant = variant.evolve(variantName="foo", path=variant_file)

    assert variant.generatorName == "testing"
    assert variant.variantName == "position"
    assert variant.version == _VERSION
    assert type(variant).__name__ == "Variant"
    assert variant.name == f"testing|position|{_VERSION}"

    if variant.distribution.url:
        assert variant.distribution.contentUrl is None
    else:
        assert variant.distribution.contentUrl is not None

    assert new_variant.generatorName == "testing"
    assert new_variant.variantName == "foo"
    assert new_variant.version == _VERSION
    assert type(new_variant).__name__ == "Variant"
    assert new_variant.name == f"testing|foo|{_VERSION}"
    assert new_variant.distribution.url == f"file://{variant_file}"
    assert new_variant.distribution.contentUrl is None


def test_variant__publish__remote_variant__raise_exists(variant_from_id, monkeypatch):
    with pytest.raises(CWLRegistryError):
        variant_from_id.publish()


def test_variant__publish__remote_variant(variant_from_id, monkeypatch):
    with patch("entity_management.nexus.update") as patched:
        variant_from_id.publish(update=True)

        payload = patched.call_args.args[2]

        assert payload == {
            "generatorName": "testing",
            "variantName": "position",
            "version": "v0.3.1",
            "name": "testing|position|v0.3.1",
            "distribution": {
                "name": "definition.cwl",
                "contentUrl": "https://bbp.epfl.ch/nexus/v1/files/bbp/mmb-point-neuron-framework-model/https:%2F%2Fbbp.epfl.ch%2Fdata%2Fbbp%2Fmmb-point-neuron-framework-model%2Fc61bfb87-dc9f-4c2b-aa41-2a7c8935cbc8",
                "contentSize": {"unitCode": "bytes", "value": 963},
                "digest": {
                    "algorithm": "SHA-256",
                    "value": "52fb32031f9b6ea1bf31715d11c66aeec5d59e0ae25e9f45f437f0ce615d26e0",
                },
                "encodingFormat": "application/cwl",
                "@type": "DataDownload",
            },
            "@context": [
                "https://bluebrain.github.io/nexus/contexts/metadata.json",
                "https://bbp.neuroshapes.org",
            ],
            "@type": "Variant",
        }


def test_variant__publish__remote_variant__local_distr(variant_from_id, monkeypatch, variant_file):
    variant = variant_from_id.evolve(path=variant_file)

    resp_upload_file = {
        "@context": [
            "https://bluebrain.github.io/nexus/contexts/files.json",
            "https://bluebrain.github.io/nexus/contexts/metadata.json",
        ],
        "@id": "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/13466787-0fbd-4bbb-b17b-a453b72608a4",
        "@type": "File",
        "_bytes": 886,
        "_digest": {
            "_algorithm": "SHA-256",
            "_value": "e840abb9299bcbe24e660f4ad668589aa5bfee759ab835dc63a5d10a7a96d8ca",
        },
        "_filename": "definition.cwl",
        "_mediaType": "application/cwl",
        "_self": "https://bbp.epfl.ch/nexus/v1/files/bbp/mmb-point-neuron-framework-model/https:%2F%2Fbbp.epfl.ch%2Fdata%2Fbbp%2Fmmb-point-neuron-framework-model%2F13466787-0fbd-4bbb-b17b-a453b72608a4",
    }

    with (
        patch("entity_management.nexus.upload_file", return_value=resp_upload_file),
        patch("entity_management.nexus.update") as patched,
    ):
        variant.publish(update=True)

        payload = patched.call_args.args[2]

        assert payload == {
            "generatorName": "testing",
            "variantName": "position",
            "version": "v0.3.1",
            "name": "testing|position|v0.3.1",
            "distribution": {
                "name": "definition.cwl",
                "contentUrl": "https://bbp.epfl.ch/nexus/v1/files/bbp/mmb-point-neuron-framework-model/https:%2F%2Fbbp.epfl.ch%2Fdata%2Fbbp%2Fmmb-point-neuron-framework-model%2F13466787-0fbd-4bbb-b17b-a453b72608a4",
                "contentSize": {"unitCode": "bytes", "value": 886},
                "digest": {
                    "algorithm": "SHA-256",
                    "value": "e840abb9299bcbe24e660f4ad668589aa5bfee759ab835dc63a5d10a7a96d8ca",
                },
                "encodingFormat": "application/cwl",
                "@type": "DataDownload",
            },
            "@context": [
                "https://bluebrain.github.io/nexus/contexts/metadata.json",
                "https://bbp.neuroshapes.org",
            ],
            "@type": "Variant",
        }


def test_variant__publish__local_variant(
    variant_from_file, monkeypatch, variant_file, variant_metadata
):
    resp_upload_file = {
        "@context": [
            "https://bluebrain.github.io/nexus/contexts/files.json",
            "https://bluebrain.github.io/nexus/contexts/metadata.json",
        ],
        "@id": "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/13466787-0fbd-4bbb-b17b-a453b72608a4",
        "@type": "File",
        "_bytes": 886,
        "_digest": {
            "_algorithm": "SHA-256",
            "_value": "e840abb9299bcbe24e660f4ad668589aa5bfee759ab835dc63a5d10a7a96d8ca",
        },
        "_filename": "definition.cwl",
        "_mediaType": "application/cwl",
        "_self": "https://bbp.epfl.ch/nexus/v1/files/bbp/mmb-point-neuron-framework-model/https:%2F%2Fbbp.epfl.ch%2Fdata%2Fbbp%2Fmmb-point-neuron-framework-model%2F13466787-0fbd-4bbb-b17b-a453b72608a4",
    }
    query_response = {"results": {"bindings": None}}

    with (
        patch("blue_cwl.variant.sparql_query", return_value=query_response),
        patch("entity_management.nexus.upload_file", return_value=resp_upload_file),
        patch("entity_management.nexus.load_by_id", return_value=variant_metadata),
        patch("entity_management.nexus.create") as patched,
    ):
        variant_from_file.publish(update=True)

        payload = patched.call_args.args[1]

        assert payload == {
            "generatorName": "testing",
            "variantName": "position",
            "version": "v0.3.1",
            "name": "testing|position|v0.3.1",
            "distribution": {
                "name": "definition.cwl",
                "contentUrl": "https://bbp.epfl.ch/nexus/v1/files/bbp/mmb-point-neuron-framework-model/https:%2F%2Fbbp.epfl.ch%2Fdata%2Fbbp%2Fmmb-point-neuron-framework-model%2F13466787-0fbd-4bbb-b17b-a453b72608a4",
                "contentSize": {"unitCode": "bytes", "value": 886},
                "digest": {
                    "algorithm": "SHA-256",
                    "value": "e840abb9299bcbe24e660f4ad668589aa5bfee759ab835dc63a5d10a7a96d8ca",
                },
                "encodingFormat": "application/cwl",
                "@type": "DataDownload",
            },
            "@context": ["https://bbp.neuroshapes.org"],
            "@type": "Variant",
        }


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
