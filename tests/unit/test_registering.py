import tempfile
from pathlib import Path
from unittest.mock import patch, Mock

from cwl_registry import registering as test_module
from cwl_registry.testing import patchenv
from kgforge.core import Resource

from entity_management import nexus
from entity_management.atlas import AtlasBrainRegion, AtlasRelease
from entity_management.base import BrainLocation
from entity_management.core import DataDownload


def test_brain_location(monkeypatch):
    mock_region = Mock()
    mock_region.get_id.return_value = "foo"
    mock_region.label = "bar"

    monkeypatch.setattr(AtlasBrainRegion, "from_id", lambda *args, **kwargs: mock_region)

    payload = {
        "@id": "foo",
        "@type": "Class",
        "label": "bar",
        "notation": "myr",
        "identifier": 420,
        "prefLabel": "my-region",
    }

    with patch("entity_management.nexus.load_by_id", return_value=payload):
        res = test_module._brain_location("foo")

    assert isinstance(res, BrainLocation)
    assert res.brainRegion.url == "foo"
    assert res.brainRegion.label == "bar"


def test_register_partial_circuit():
    def load_by_url(url, *args, **kwargs):
        if "brain-region-id" in url:
            return {
                "@id": "brain-region-id",
                "@type": "Class",
                "label": "my-region",
                "notation": "myr",
                "identifier": 420,
                "prefLabel": "my-region",
            }
        if "atlas-release-id" in url:
            return {
                "@id": "atlas-release-id",
                "@type": "AtlasRelease",
                "label": "my-atlas",
                "name": "foo",
                "brainTemplateDataLayer": {"@id": "template-id", "@type": "braintemplatedatalayer"},
                "parcellationOntology": {"@id": "ontology-id", "@type": "parcellationontology"},
                "parcellationVolume": {"@id": "volume-id", "@type": "parcellationvolume"},
                "subject": {"@type": "subject"},
                "spatialReferenceSystem": {"@id": "ref-id", "@type": "spatialreferencesystem"},
            }
        raise

    def create(base_url, payload, *args, **kwargs):
        return payload

    with (
        patch("entity_management.nexus.load_by_url", side_effect=load_by_url),
        patch("entity_management.nexus.create", side_effect=create),
    ):
        res = test_module.register_partial_circuit(
            name="my-circuit",
            brain_region_id="brain-region-id",
            atlas_release_id="atlas-release-id",
            sonata_config_path="my-sonata-path",
            description="my-description",
        )

    assert isinstance(res.brainLocation, BrainLocation)
    assert res.brainLocation.brainRegion.url == "brain-region-id"
    assert res.brainLocation.brainRegion.label == "my-region"

    assert isinstance(res.atlasRelease, AtlasRelease)
    assert res.atlasRelease.get_id() == "atlas-release-id"

    assert isinstance(res.circuitConfigPath, DataDownload)
    assert res.circuitConfigPath.url == f"file://{Path('my-sonata-path').resolve()}"


def test_register_cell_composition_summary():
    def mock_load_by_id(resource_id, *args, **kwargs):
        if resource_id == "brain-region-id":
            return {
                "@id": "brain-region-id",
                "@type": "Class",
                "label": "my-region",
                "notation": "myr",
            }

        if resource_id == "atlas-release-id":
            return {
                "@id": "atlas-release-id",
                "@type": "AtlasRelease",
                "label": "my-atlas",
                "name": "foo",
                "brainTemplateDataLayer": {"@id": "template-id", "@type": "braintemplatedatalayer"},
                "parcellationOntology": {"@id": "ontology-id", "@type": "parcellationontology"},
                "parcellationVolume": {"@id": "volume-id", "@type": "parcellationvolume"},
                "subject": {"@type": "Subject"},
                "spatialReferenceSystem": {"@id": "ref-id", "@type": "spatialreferencesystem"},
            }

        if resource_id == "circuit-id":
            return {
                "@id": "circuit-id",
                "@type": "DetailedCircuit",
            }

        raise ValueError(resource_id)

    file_metadata = {
        "@id": "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/5bea6348-9e59-4fc3-9a33-bcefa3264461",
        "@type": "File",
        "_bytes": 35052232,
        "_digest": {
            "_algorithm": "SHA-256",
            "_value": "3cb2ab9350f5a69f7e070b061d0f8cd2f4948350bd51dd87f3353262e0c4ef91",
        },
        "_filename": "summary_file.json",
        "_location": "file:///gpfs/cell_composition_summary_distribution.json",
        "_mediaType": "application/json",
        "_rev": 1,
        "_self": "https://bbp.epfl.ch/nexus/v1/files/bbp/mmb-point-neuron-framework-model/https:%2F%2Fbbp.epfl.ch%2Fdata%2Fbbp%2Fmmb-point-neuron-framework-model%2F5bea6348-9e59-4fc3-9a33-bcefa3264461",
    }

    def create(base_url, payload, *args, **kwargs):
        return payload

    with tempfile.TemporaryDirectory() as tdir:
        tdir = Path(tdir)

        summary_file = tdir / "summary_file.json"
        summary_file.touch()

        with (
            patch("entity_management.nexus.load_by_id", side_effect=mock_load_by_id),
            patch("entity_management.nexus.upload_file", return_value=file_metadata),
            patch("entity_management.nexus.create", side_effect=create),
        ):
            res = test_module.register_cell_composition_summary(
                name="my-summary",
                summary_file=summary_file,
                atlas_release_id="atlas-release-id",
                derivation_entity_id="circuit-id",
            )

        assert res.name == "my-summary"
        assert res._type == "CellCompositionSummary"
        assert res.description == "Statistical summary of the model cell composition."
        assert res.about == ["nsg:Neuron", "nsg:Glia"]

        assert res.atlasRelease.get_id() == "atlas-release-id"
        assert res.atlasRelease._type == "AtlasRelease"

        assert res.distribution.name == "summary_file.json"
        assert res.distribution._type == "DataDownload"
        assert res.distribution.encodingFormat == "application/json"

        assert res.derivation.entity.get_id() == "circuit-id"
        assert res.derivation.entity._type == "DetailedCircuit"
