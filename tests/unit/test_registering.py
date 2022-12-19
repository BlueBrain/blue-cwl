import tempfile
from pathlib import Path
from cwl_registry import registering as test_module
from kgforge.core import Resource

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


def test_brain_location():

    forge = LocalForge()
    forge.storage["entity-id"] = Resource.from_json(
        {"id": "entity-id", "type": "MyType", "label": "my-label"}
    )

    res = test_module._brain_location(forge, "entity-id")

    assert res.type == "BrainLocation"
    assert res.brainRegion.id == "entity-id"
    assert res.brainRegion.label == "my-label"


def test_subject():

    forge = LocalForge()
    forge.storage["entity-id"] = Resource.from_json(
        {"id": "entity-id", "type": "MyType", "label": "my-label"}
    )

    res = test_module._subject(forge, None)
    assert res.type == "Subject"
    assert res.species.id == "http://purl.obolibrary.org/obo/NCBITaxon_10090"
    assert res.species.label == ["Mus musculus", "Mus Musculus"]

    res = test_module._subject(forge, "entity-id")
    assert res.type == "Subject"
    assert res.species.id == "entity-id"
    assert res.species.label == "my-label"


def test_register_partial_circuit():

    forge = LocalForge()
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
        }
    )
    res = test_module.register_partial_circuit(
        forge,
        name="my-circuit",
        brain_region_id="brain-region-id",
        atlas_release_id="atlas-release-id",
        sonata_config_path="my-sonata-path",
        target_digest="0",
        description="my-description",
    )

    assert res.brainLocation.type == "BrainLocation"
    assert res.brainLocation.brainRegion.id == "brain-region-id"
    assert res.brainLocation.brainRegion.label == "my-region"
    assert res.brainLocation.brainRegion.notation == "myr"

    assert res.atlasRelease.id == "atlas-release-id"
    assert res.atlasRelease.type == "AtlasRelease"

    assert res.circuitConfigPath.type == "DataDownload"
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
