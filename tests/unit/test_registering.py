import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, Mock

from blue_cwl import registering as test_module
from blue_cwl.testing import patchenv
from blue_cwl.utils import write_json, load_json
from kgforge.core import Resource

from entity_management import nexus
from entity_management.atlas import AtlasBrainRegion, AtlasRelease
from entity_management.base import BrainLocation
from entity_management.core import DataDownload
from entity_management.simulation import DetailedCircuit


@pytest.fixture
def atlas_release():
    metadata = {
        "@id": "atlas-release-id",
        "_rev": 1,
        "@type": "AtlasRelease",
        "label": "my-atlas",
        "name": "foo",
        "brainTemplateDataLayer": {
            "@id": "template-id",
            "@type": "braintemplatedatalayer",
        },
        "parcellationOntology": {"@id": "ontology-id", "@type": "parcellationontology"},
        "parcellationVolume": {"@id": "volume-id", "@type": "parcellationvolume"},
        "subject": {"@type": "subject"},
        "spatialReferenceSystem": {
            "@id": "ref-id",
            "@type": "spatialreferencesystem",
            "_rev": 1,
        },
        "brainLocation": {"brainRegion": {"@id": "mba:997", "label": "root"}},
    }

    with patch("entity_management.nexus.load_by_url", return_value=metadata):
        return AtlasRelease.from_url("atlas-release-id")


@pytest.fixture
def circuit():
    metadata = {
        "@id": "circuit-id",
        "@type": "DetailedCircuit",
    }

    with patch("entity_management.nexus.load_by_url", return_value=metadata):
        return DetailedCircuit.from_url("circuit-id")


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


def test_register_partial_circuit(atlas_release):
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
                "brainTemplateDataLayer": {
                    "@id": "template-id",
                    "@type": "braintemplatedatalayer",
                },
                "parcellationOntology": {
                    "@id": "ontology-id",
                    "@type": "parcellationontology",
                },
                "parcellationVolume": {
                    "@id": "volume-id",
                    "@type": "parcellationvolume",
                },
                "subject": {"@type": "subject"},
                "spatialReferenceSystem": {
                    "@id": "ref-id",
                    "@type": "spatialreferencesystem",
                },
            }
        if "mock-user-id" in url:
            return {"preferred_username": "mock-user"}
        raise

    def create(base_url, payload, *args, **kwargs):
        return payload

    with (
        patch("entity_management.nexus.load_by_url", side_effect=load_by_url),
        patch("entity_management.nexus.create", side_effect=create),
        patch("entity_management.state.get_user_id", return_value="mock-user-id"),

    ):
        res = test_module.register_partial_circuit(
            name="my-circuit",
            brain_region_id="brain-region-id",
            atlas_release=atlas_release,
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


def test_register_cell_composition_summary(atlas_release, circuit):
    file_metadata = {
        "@id": "file-id",
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
        "_self": "file-self",
    }

    def create(base_url, payload, *args, **kwargs):
        return payload

    with tempfile.TemporaryDirectory() as tdir:
        tdir = Path(tdir)

        summary_file = tdir / "summary_file.json"
        summary_file.touch()

        with (
            patch("entity_management.nexus.upload_file", return_value=file_metadata),
            patch("entity_management.nexus.create", side_effect=create),
            patch("entity_management.state.get_user_id", return_value="mock-user-id"),
            patch("entity_management.nexus.load_by_id", return_value={"preferred_username": "mock-user"}),
        ):
            res = test_module.register_cell_composition_summary(
                name="my-summary",
                description="my-summary-description",
                distribution_file=summary_file,
                atlas_release=atlas_release,
                derivation_entity=circuit,
            )

        assert res.name == "my-summary"
        assert res._type == "CellCompositionSummary"
        assert res.description == "my-summary-description"
        assert res.about == ["nsg:Neuron", "nsg:Glia"]

        assert res.atlasRelease.get_id() == "atlas-release-id"
        assert res.atlasRelease._type == "AtlasRelease"

        assert res.distribution.name == "summary_file.json"
        assert res.distribution._type == "DataDownload"
        assert res.distribution.encodingFormat == "application/json"

        assert res.derivation.entity.get_id() == "circuit-id"
        assert res.derivation.entity._type == "DetailedCircuit"


def test_register_cell_composition_volume(atlas_release, circuit):
    file_metadata = {
        "@id": "file-id",
        "@type": "File",
        "_bytes": 35052232,
        "_digest": {
            "_algorithm": "SHA-256",
            "_value": "3cb2ab9350f5a69f7e070b061d0f8cd2f4948350bd51dd87f3353262e0c4ef91",
        },
        "_filename": "volume_file.json",
        "_location": "file:///gpfs/cell_composition_volume_distribution.json",
        "_mediaType": "application/json",
        "_rev": 1,
        "_self": "file-self",
    }

    def create(base_url, payload, *args, **kwargs):
        return payload

    with tempfile.TemporaryDirectory() as tdir:
        tdir = Path(tdir)

        summary_file = tdir / "summary_file.json"
        summary_file.touch()

        with (
            patch("entity_management.nexus.upload_file", return_value=file_metadata),
            patch("entity_management.nexus.create", side_effect=create),
            patch("entity_management.state.get_user_id", return_value="mock-user-id"),
            patch("entity_management.nexus.load_by_id", return_value={"preferred_username": "mock-user"}),
        ):
            res = test_module.register_cell_composition_volume(
                name="my-volume",
                description="my-volume-description",
                distribution_file=summary_file,
                atlas_release=atlas_release,
                derivation_entity=circuit,
            )

        assert res.name == "my-volume"
        assert res._type == "CellCompositionVolume"
        assert res.description == "my-volume-description"
        assert res.about == ["nsg:Neuron", "nsg:Glia"]

        assert res.atlasRelease.get_id() == "atlas-release-id"
        assert res.atlasRelease._type == "AtlasRelease"

        assert res.distribution.name == "volume_file.json"
        assert res.distribution._type == "DataDownload"
        assert res.distribution.encodingFormat == "application/json"

        assert res.derivation.entity.get_id() == "circuit-id"
        assert res.derivation.entity._type == "DetailedCircuit"


def test_register_cell_composition(atlas_release):
    summary_file_metadata = {
        "@id": "file-id",
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
        "_self": "file-self",
    }
    volume_file_metadata = {
        "@id": "file-id",
        "@type": "File",
        "_bytes": 35052232,
        "_digest": {
            "_algorithm": "SHA-256",
            "_value": "3cb2ab9350f5a69f7e070b061d0f8cd2f4948350bd51dd87f3353262e0c4ef91",
        },
        "_filename": "volume_file.json",
        "_location": "file:///gpfs/cell_composition_volume_distribution.json",
        "_mediaType": "application/json",
        "_rev": 1,
        "_self": "file-self",
    }

    def create(base_url, payload, *args, **kwargs):
        return payload

    def upload_file(name, data, *args, **kwargs):
        return {
            "summary_file.json": summary_file_metadata,
            "volume_file.json": volume_file_metadata,
        }[name]

    with tempfile.TemporaryDirectory() as tdir:
        tdir = Path(tdir)

        summary_file = tdir / "summary_file.json"
        summary_file.touch()

        volume_file = tdir / "volume_file.json"
        volume_file.touch()

        with (
            patch("entity_management.nexus.upload_file", side_effect=upload_file),
            patch("entity_management.nexus.create", side_effect=create),
            patch("entity_management.state.get_user_id", return_value="mock-user-id"),
            patch("entity_management.nexus.load_by_id", return_value={"preferred_username": "mock-user"}),
        ):
            res = test_module.register_cell_composition(
                name="cell-composition",
                description="cell-composition-description",
                atlas_release=atlas_release,
                cell_composition_volume_file=volume_file,
                cell_composition_summary_file=summary_file,
            )

        assert res.name == "cell-composition"
        assert res._type == "CellComposition"
        assert res.description == "cell-composition-description"
        assert res.about == ["nsg:Neuron", "nsg:Glia"]

        assert res.atlasRelease.get_id() == "atlas-release-id"
        assert res.atlasRelease._type == "AtlasRelease"

        assert res.brainLocation == atlas_release.brainLocation

        volume = res.cellCompositionVolume

        assert volume.atlasRelease == atlas_release
        assert volume.brainLocation == atlas_release.brainLocation
        assert volume.distribution.name == "volume_file.json"

        summary = res.cellCompositionSummary
        assert summary.atlasRelease == atlas_release
        assert summary.brainLocation == atlas_release.brainLocation
        assert summary.distribution.name == "summary_file.json"


@pytest.fixture
def mixed_densities_file(tmp_path):
    nrrd_file = tmp_path / "foo.nrrd"
    nrrd_file.touch()

    data = {
        "hasPart": [
            {
                "@id": "http://uri.interlex.org/base/ilx_0381367",
                "label": "L2_TPC:B",
                "about": ["https://neuroshapes.org/MType"],
                "hasPart": [
                    {
                        "@id": "http://bbp.epfl.ch/neurosciencegraph/ontologies/etypes/Foo",
                        "label": "Foo",
                        "about": ["https://neuroshapes.org/EType"],
                        "hasPart": [
                            {
                                "path": str(nrrd_file),
                                "@type": "METypeDensity",
                            }
                        ],
                    },
                    {
                        "@id": "http://bbp.epfl.ch/neurosciencegraph/ontologies/etypes/Bar",
                        "label": "Bar",
                        "about": ["https://neuroshapes.org/EType"],
                        "hasPart": [
                            {
                                "@id": "bar-id",
                                "_rev": 1,
                                "@type": "METypeDensity",
                            }
                        ],
                    },
                ],
            }
        ]
    }
    densities_file = tmp_path / "mixed_densities.json"
    write_json(data=data, filepath=densities_file)
    return str(densities_file)


def test_regigster_densities(tmp_path, mixed_densities_file, atlas_release):
    output_file = tmp_path / "registered_densities.json"

    foo_metadata = {
        "@id": "file-id",
        "@type": "File",
        "_bytes": 35052232,
        "_digest": {
            "_algorithm": "SHA-256",
            "_value": "3cb2ab9350f5a69f7e070b061d0f8cd2f4948350bd51dd87f3353262e0c4ef91",
        },
        "_filename": "foo.nrrd",
        "_location": "file:///gpfs/foo.nrrd",
        "_mediaType": "application/nrrd",
        "_rev": 1,
        "_self": "file-self",
    }

    def create(base_url, payload, *args, **kwargs):
        return payload | {"@id": "foo-id", "_rev": 2}

    def upload_file(name, data, *args, **kwargs):
        return {
            "foo.nrrd": foo_metadata,
        }[name]

    with (
        patch("entity_management.nexus.create", side_effect=create),
        patch("entity_management.nexus.upload_file", side_effect=upload_file),
        patch("entity_management.state.get_user_id", return_value="mock-user-id"),
        patch("entity_management.nexus.load_by_id", return_value={"preferred_username": "mock-user"}),
    ):
        res = test_module.register_densities(
            atlas_release=atlas_release,
            distribution_file=mixed_densities_file,
            output_file=output_file,
        )

    result = load_json(output_file)
    assert result == res

    assert result == {
        "hasPart": [
            {
                "@id": "http://uri.interlex.org/base/ilx_0381367",
                "label": "L2_TPC:B",
                "about": ["https://neuroshapes.org/MType"],
                "hasPart": [
                    {
                        "@id": "http://bbp.epfl.ch/neurosciencegraph/ontologies/etypes/Foo",
                        "label": "Foo",
                        "about": ["https://neuroshapes.org/EType"],
                        "hasPart": [{"@type": "METypeDensity", "@id": "foo-id", "_rev": 2}],
                    },
                    {
                        "@id": "http://bbp.epfl.ch/neurosciencegraph/ontologies/etypes/Bar",
                        "label": "Bar",
                        "about": ["https://neuroshapes.org/EType"],
                        "hasPart": [{"@id": "bar-id", "_rev": 1, "@type": "METypeDensity"}],
                    },
                ],
            }
        ]
    }
