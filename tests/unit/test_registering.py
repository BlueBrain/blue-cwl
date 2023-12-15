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
from tests.unit.mocking import LocalForge


def test_as_reference():
    forge = LocalForge()
    forge.storage["entity-id"] = Resource.from_json(
        {"id": "entity-id", "type": "MyType", "label": "MyLabel"}
    )

    res = test_module._as_reference(forge, "entity-id")

    assert res.id == "entity-id"
    assert res.type == "MyType"
    assert not hasattr(res, "label")

    res = test_module._as_reference(forge, "entity-id", properties=["label"])

    assert res.label == "MyLabel"
    assert not hasattr(res, "id")
    assert not hasattr(res, "type")


def test_circuit_config_path():
    path = Path("my-path")
    res = test_module._circuit_config_path(path)

    expected_path = str(path.resolve())

    assert res.type == "DataDownload"
    assert res.url == f"file://{expected_path}"


def test_brain_location(monkeypatch):
    mock_region = Mock()
    mock_region.get_id.return_value = "foo"
    mock_region.label = "bar"

    monkeypatch.setattr(AtlasBrainRegion, "from_id", lambda *args, **kwargs: mock_region)

    res = test_module._brain_location(None)

    assert isinstance(res, BrainLocation)
    assert res.brainRegion.url == "foo"
    assert res.brainRegion.label == "bar"


def test_subject():
    forge = LocalForge()
    forge.storage["entity-id"] = Resource.from_json(
        {"id": "entity-id", "type": "MyType", "label": "my-label"}
    )

    res = test_module._subject(forge, None)
    assert res.type == "Subject"
    assert res.species.id == "http://purl.obolibrary.org/obo/NCBITaxon_10090"
    assert res.species.label == "Mus musculus"

    res = test_module._subject(forge, "entity-id")
    assert res.type == "Subject"
    assert res.species.id == "entity-id"
    assert res.species.label == "my-label"


def test_register_partial_circuit(monkeypatch):
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
                "brainTemplateDataLayer": {"@id": "template-id", "@type": "BrainTemplateDataLayer"},
                "parcellationOntology": {"@id": "ontology-id", "@type": "ParcellationOntology"},
                "parcellationVolume": {"@id": "volume-id", "@type": "ParcellationVolume"},
                "subject": {"@id": "subject-id", "@type": "Subject"},
                "spatialReferenceSystem": {"@id": "ref-id", "@type": "SpatialReferenceSystem"},
            }
        if "foo" in url:
            breakpoint()
        raise

    def create(base_url, payload, *args, **kwargs):
        return payload

    monkeypatch.setattr(nexus, "load_by_url", load_by_url)
    monkeypatch.setattr(nexus, "create", create)

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
    with tempfile.TemporaryDirectory() as tdir:
        tdir = Path(tdir)

        forge = LocalForge(output_dir=tdir)

        forge.storage["brain-region-id"] = Resource.from_json(
            {
                "id": "brain-region-id",
                "type": "Class",
                "label": "my-region",
                "notation": "myr",
            }
        )
        forge.storage["atlas-release-id"] = Resource.from_json(
            {
                "id": "atlas-release-id",
                "type": "AtlasRelease",
                "label": "my-atlas",
                "name": "foo",
                "brainTemplateDataLayer": "foo",
                "parcellationOntology": "bar",
                "parcellationVolume": "zoo",
                "spatialReferenceSystem": "tak",
            }
        )
        forge.storage["circuit-id"] = Resource.from_json(
            {
                "id": "circuit-id",
                "type": "DetailedCircuit",
            }
        )

        summary_file = tdir / "summary_file.json"
        summary_file.touch()

        res = test_module.register_cell_composition_summary(
            forge=forge,
            name="my-summary",
            summary_file=summary_file,
            atlas_release_id="atlas-release-id",
            derivation_entity_id="circuit-id",
        )

        assert res.name == "my-summary"
        assert res.type == ["CellCompositionSummary", "Entity"]
        assert res.description == "Statistical summary of the model cell composition."
        assert res.about == ["nsg:Neuron", "nsg:Glia"]

        assert res.atlasRelease.id == "atlas-release-id"
        assert res.atlasRelease.type == "AtlasRelease"

        assert res.distribution.name == "summary_file.json"
        assert res.distribution.type == "DataDownload"
        assert res.distribution.encodingFormat == "application/json"
        assert Path(res.distribution.atLocation.location[7:]).exists()

        assert res.derivation.type == "Derivation"
        assert res.derivation.entity.id == "circuit-id"
        assert res.derivation.entity.type == "DetailedCircuit"


@patchenv(NEXUS_WORKFLOW="my-id")
def test_add_workflow_influence():
    res = Resource()
    test_module._add_workflow_influence(res)

    assert hasattr(res, "wasInfluencedBy")
    assert res.wasInfluencedBy.id == "my-id"
    assert res.wasInfluencedBy.type == "WorkflowExecution"


@patchenv()
def test_add_workflow_influeces__no_workflow():
    res = Resource()
    test_module._add_workflow_influence(res)

    assert not hasattr(res, "wasInfluencedBy")
