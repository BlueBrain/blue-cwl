from unittest.mock import patch
from pathlib import Path
from functools import partial

import pytest
from entity_management.state import get_base_url
from blue_cwl.utils import url_without_revision, write_json

DATA_DIR = Path(__file__).parent / "data"

REGION_ID = "region-id"

ATLAS_RELEASE_ID = "atlas-release-id"
HIERARCHY_ID = "parcellation-ontology-id"
HIERARCHY_DISTRIBUTION_URL = "parcellation-ontology-distribution-url"
ANNOTATION_ID = "parcellation-volume-id"
ANNOTATION_DISTRIBUTION_URL = "parcellation-volume-distribution-url"
HEMISPHERE_ID = "hemisphere-id"
HEMISPHERE_DISTRIBUTION_URL = "hemisphere-distribution-url"

CELL_COMPOSITION_ID = "cell-composition-id"
VOLUME_ID = "volume-id"
SUMMARY_ID = "summary-id"
VOLUME_DISTRIBUTION_URL = "volume-distribution-url"
SUMMARY_DISTRIBUTION_URL = "summary-distrubution-url"
NRRD_ID = "nrrd-id"
NRRD_URL = "nrrd-url"

CONFIG_CELL_POSITION_ID = "config-neurons-cell-position-config-id"
CONFIG_CELL_POSITION_DISTRIBUTION_URL = "config-neurons-cell-position-distribution-url"


def _metadata(entity_id, entity_type, file_url, encoding_format):
    return {
        "@id": entity_id,
        "@type": entity_type,
        "distribution": {
            "@type": "DataDownload",
            "contentSize": {"unitCode": "bytes", "value": 3999534},
            "contentUrl": file_url,
            "digest": {
                "algorithm": "SHA-256",
                "value": "5ded8d6b5df2a71780c3c4e72ecec13bc67b69b828b7c0605acec6ccf4457b4c",
            },
            "encodingFormat": encoding_format,
        },
    }


@pytest.fixture(scope="session")
def conftest_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("test_data")


@pytest.fixture(scope="session")
def region_id():
    return REGION_ID


@pytest.fixture(scope="session")
def cell_composition_id():
    return CELL_COMPOSITION_ID


@pytest.fixture(scope="session")
def config_cell_position_id():
    return CONFIG_CELL_POSITION_ID


def _resp(data):
    res = {
        "@context": [
            "https://bluebrain.github.io/nexus/contexts/metadata.json",
            "https://bbp.neuroshapes.org",
        ],
        "_rev": 1,
        "_project": "my-project",
        "_self": "my-self",
        "_constrainedBy": "https://bluebrain.github.io/nexus/schemas/unconstrained.json",
        "_createdAt": "2024-01-22T10:07:16.052123Z",
        "_createdBy": "https://bbp.epfl.ch/nexus/v1/realms/bbp/users/zisis",
        "_deprecated": False,
        "_updatedAt": "2024-01-22T10:07:16.052123Z",
        "_updatedBy": "https://bbp.epfl.ch/nexus/v1/realms/bbp/users/zisis",
    }
    res.update(data)
    return res


@pytest.fixture(scope="session")
def region_metadata():
    return {
        "@context": [
            "https://bluebrain.github.io/nexus/contexts/metadata.json",
            "https://neuroshapes.org",
        ],
        "@id": "mba:997",
        "@type": "Class",
        "notation": "root",
    }


@pytest.fixture(scope="session")
def cell_composition_metadata():
    return {
        "@id": CELL_COMPOSITION_ID,
        "@type": "CellComposition",
        "atlasRelease": {
            "@id": ATLAS_RELEASE_ID,
            "@type": "AtlasRelease",
        },
        "cellCompositionVolume": {
            "@id": VOLUME_ID,
            "@type": "CellCompositionVolume",
        },
        "cellCompositionSummary": {
            "@id": SUMMARY_ID,
            "@type": "CellCompositionSummary",
        },
    }


@pytest.fixture(scope="session")
def volume_metadata():
    return _metadata(
        entity_id=VOLUME_ID,
        entity_type="CellCompositionVolume",
        file_url=VOLUME_DISTRIBUTION_URL,
        encoding_format="application/json",
    )


@pytest.fixture(scope="session")
def volume_distribution():
    return {
        "hasPart": [
            {
                "@id": "http://uri.interlex.org/base/ilx_0381367",
                "label": "L2_TPC:B",
                "about": ["https://neuroshapes.org/MType"],
                "hasPart": [
                    {
                        "@id": "http://bbp.epfl.ch/neurosciencegraph/ontologies/etypes/cADpyr",
                        "label": "cADpyr",
                        "about": ["https://neuroshapes.org/EType"],
                        "hasPart": [{"@id": NRRD_ID, "@type": "METypeDensity", "_rev": 2}],
                    }
                ],
            }
        ]
    }


@pytest.fixture(scope="session")
def summary_metadata():
    return {
        "@id": SUMMARY_ID,
        "@type": "CellCompositionSummary",
        "distribution": {
            "@type": "DataDownload",
            "encodingFormat": "application/json",
            "contentUrl": SUMMARY_DISTRIBUTION_URL,
        },
    }


@pytest.fixture(scope="session")
def summary_distribution():
    return {
        "version": 1,
        "unitCode": {"density": "mm^-3"},
        "hasPart": {
            "http://api.brain-map.org/api/v2/data/Structure/614454292": {
                "label": "Primary somatosensory area, barrel field, layer 2",
                "about": "BrainRegion",
                "hasPart": {
                    "http://uri.interlex.org/base/ilx_0381367": {
                        "label": "L2_TPC:B",
                        "about": "MType",
                        "hasPart": {
                            "http://bbp.epfl.ch/neurosciencegraph/ontologies/etypes/cADpyr": {
                                "label": "cADpyr",
                                "about": "EType",
                                "composition": {
                                    "neuron": {"density": 41061.716478599694, "count": 8059}
                                },
                            }
                        },
                    }
                },
            }
        },
    }


@pytest.fixture(scope="session")
def me_type_density_metadata():
    return {
        "@id": "me-type-density-id",
        "@type": "METypeDensity",
        "distribution": {
            "@type": "DataDownload",
            "encodingFormat": "application/nrrd",
            "contentUrl": NRRD_URL,
        },
    }


@pytest.fixture(scope="session")
def me_type_density_distribution_file():
    return str(DATA_DIR / "L2_TPC:B_cADpyr_v3__SSp-bfd2.nrrd")


@pytest.fixture(scope="session")
def atlas_release_metadata():
    return {
        "@id": ATLAS_RELEASE_ID,
        "@type": "AtlasRelease",
        "brainTemplateDataLayer": {
            "@id": "brain-template-data-layer-id",
            "@type": "BrainTemplateDataLayer",
        },
        "parcellationOntology": {
            "@id": HIERARCHY_ID,
            "@type": "ParcellationOntology",
        },
        "parcellationVolume": {
            "@id": ANNOTATION_ID,
            "@type": "ParcellationVolume",
        },
        "hemisphereVolume": {
            "@id": HEMISPHERE_ID,
            "@type": "HemisphereAnnotationDataLayer",
        },
        "spatialReferenceSystem": {
            "@id": "spatial-reference-system-id",
            "@type": "SpatialReferenceSystem",
        },
        "brainLocation": {"brainRegion": {"@id": "mba:997", "label": "root"}},
    }


@pytest.fixture(scope="session")
def parcellation_ontology_metadata():
    return {
        "@id": HIERARCHY_ID,
        "@type": "ParcellationOntology",
        "distribution": {
            "@type": "DataDownload",
            "contentSize": {"unitCode": "bytes", "value": 3999534},
            "contentUrl": HIERARCHY_DISTRIBUTION_URL,
            "digest": {
                "algorithm": "SHA-256",
                "value": "5ded8d6b5df2a71780c3c4e72ecec13bc67b69b828b7c0605acec6ccf4457b4c",
            },
            "encodingFormat": "application/json",
            "name": "hierarchy.json",
        },
    }


@pytest.fixture(scope="session")
def parcellation_volume_metadata():
    return _metadata(
        entity_id=ANNOTATION_ID,
        entity_type="BrainParcellationDataLayer",
        file_url=ANNOTATION_DISTRIBUTION_URL,
        encoding_format="application/nrrd",
    )


@pytest.fixture(scope="session")
def parcellation_volume_distribution_file():
    return str(DATA_DIR / "brain_regions.nrrd")


@pytest.fixture(scope="session")
def hemisphere_volume_metadata():
    return _metadata(
        entity_id=HEMISPHERE_ID,
        entity_type="HemisphereAnnotationDataLayer",
        file_url=HEMISPHERE_DISTRIBUTION_URL,
        encoding_format="application/nrrd",
    )


@pytest.fixture(scope="session")
def hemisphere_volume_distribution_file():
    return str(DATA_DIR / "hemisphere.nrrd")


@pytest.fixture(scope="session")
def parcellation_ontology_distribution_file():
    return str(DATA_DIR / "hierarchy.json")


@pytest.fixture(scope="session")
def config_neurons_cell_position_distribution():
    return {
        "place_cells": {
            "soma_placement": "basic",
            "density_factor": 1,
            "sort_by": ["region", "mtype"],
            "seed": 0,
            "mini_frequencies": False,
        }
    }


@pytest.fixture(scope="session")
def config_neurons_cell_position_distribution_file(
    conftest_dir, config_neurons_cell_position_distribution
):
    output_file = conftest_dir / "config_neurons_cell_position.json"
    write_json(filepath=output_file, data=config_neurons_cell_position_distribution)
    return str(output_file)


def _variant_task_parametrization_metadata(resource_id, file_url):
    return {
        "@context": [
            "https://bluebrain.github.io/nexus/contexts/metadata.json",
            "https://bbp.neuroshapes.org",
        ],
        "@id": resource_id,
        "@type": "VariantTaskParameterization",
        "distribution": {
            "@type": "DataDownload",
            "contentSize": {"unitCode": "bytes", "value": 135},
            "contentUrl": file_url,
            "digest": {
                "algorithm": "SHA-256",
                "value": "9f7e7d44487116743329616ee3d22c185b18ca464124a399206a474ec3e538c7",
            },
            "encodingFormat": "application/json",
            "name": "variant_task_parameterization.json",
        },
        "name": "VariantTaskParameterization",
    }


@pytest.fixture(scope="session")
def load_by_id(
    region_metadata,
    cell_composition_metadata,
    volume_metadata,
    summary_metadata,
    me_type_density_metadata,
    atlas_release_metadata,
    parcellation_ontology_metadata,
    parcellation_volume_metadata,
    hemisphere_volume_metadata,
):
    def _load_by_id(resource_id, *args, **kwargs):
        resource_id = url_without_revision(resource_id)
        return {
            REGION_ID: _resp(region_metadata),
            CELL_COMPOSITION_ID: _resp(cell_composition_metadata),
            VOLUME_ID: _resp(volume_metadata),
            NRRD_ID: _resp(me_type_density_metadata),
            ATLAS_RELEASE_ID: _resp(atlas_release_metadata),
            HIERARCHY_ID: _resp(parcellation_ontology_metadata),
            ANNOTATION_ID: _resp(parcellation_volume_metadata),
            HEMISPHERE_ID: _resp(hemisphere_volume_metadata),
            CONFIG_CELL_POSITION_ID: _resp(
                _variant_task_parametrization_metadata(
                    CONFIG_CELL_POSITION_ID, CONFIG_CELL_POSITION_DISTRIBUTION_URL
                )
            ),
        }[resource_id]

    return _load_by_id


from blue_cwl.utils import load_json
from blue_cwl.staging import stage_file


@pytest.fixture(scope="session")
def file_as_dict(volume_distribution, summary_distribution):
    def _file_as_dict(url, *args, **kwargs):
        return {
            VOLUME_DISTRIBUTION_URL: volume_distribution,
            SUMMARY_DISTRIBUTION_URL: summary_distribution,
        }[url]

    return _file_as_dict


@pytest.fixture(scope="session")
def get_unquoted_uri_path(me_type_density_distribution_file):
    def _get_unquoted_uri_path(url, *args, **kwargs):
        return {
            NRRD_URL: me_type_density_distribution_file,
        }[url]

    return _get_unquoted_uri_path


@pytest.fixture(scope="session")
def download_file(
    parcellation_ontology_distribution_file,
    parcellation_volume_distribution_file,
    hemisphere_volume_distribution_file,
    me_type_density_distribution_file,
    config_neurons_cell_position_distribution_file,
):
    def _download_file(url, path, file_name, *args, **kwargs):
        source_file = {
            HIERARCHY_DISTRIBUTION_URL: parcellation_ontology_distribution_file,
            ANNOTATION_DISTRIBUTION_URL: parcellation_volume_distribution_file,
            HEMISPHERE_DISTRIBUTION_URL: hemisphere_volume_distribution_file,
            NRRD_URL: me_type_density_distribution_file,
            CONFIG_CELL_POSITION_DISTRIBUTION_URL: config_neurons_cell_position_distribution_file,
        }[url]

        target_file = Path(path, file_name)
        stage_file(source=source_file, target=target_file)
        return str(target_file)

    return _download_file


@pytest.fixture(scope="session")
def patch_nexus_calls(load_by_id, download_file, file_as_dict, get_unquoted_uri_path):
    with (
        patch("entity_management.nexus.load_by_id", side_effect=load_by_id),
        patch("entity_management.nexus.file_as_dict", side_effect=file_as_dict),
        patch("entity_management.nexus.download_file", side_effect=download_file),
        patch("entity_management.nexus.get_unquoted_uri_path", side_effect=get_unquoted_uri_path),
    ):
        yield
