from pathlib import Path
import inspect

import pytest
import pandas as pd
import pandas.testing as pdt
from unittest.mock import patch

from blue_cwl.wrappers import cell_composition_manipulation as test_module
from blue_cwl.staging import AtlasInfo

from blue_cwl.utils import load_json, write_json

DATA_DIR = Path(__file__).parent / "data"

CONFIGURATION_ID = "configuration-d"
CONFIGURATION_DISTRIBUTION_URL = "configuration-distribution-url"

BASE_CELL_COMPOSITION_ID = "base-cell-composition-id"
BASE_VOLUME_ID = "base-volume-id"
BASE_SUMMARY_ID = "base-summary-id"
BASE_VOLUME_DISTRIBUTION_URL = "base-volume-distribution-url"
BASE_SUMMARY_DISTRIBUTION_URL = "base-summary-distrubution-url"

NRRD_ID = "nrrd-id?rev=2"
NRRD_URL = "nrrd-url"
NRRD_PATH = str(DATA_DIR / "L2_TPC:B_cADpyr_v3__SSp-bfd2.nrrd")
ATLAS_RELEASE_ID = "atlas-release-id"


@pytest.fixture(scope="module")
def expected_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("expected")


@pytest.fixture(scope="module")
def stage_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("stage")


@pytest.fixture(scope="module")
def build_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("build")


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


@pytest.fixture(scope="module")
def atlas_release_metadata():
    return {
        "@id": ATLAS_RELEASE_ID,
        "@type": "AtlasRelease",
        "brainTemplateDataLayer": {
            "@id": "brain-template-data-layer-id",
            "_rev": 1,
            "@type": "BrainTemplateDataLayer",
        },
        "parcellationOntology": {
            "@id": "parcellation-ontology-id",
            "_rev": 1,
            "@type": "ParcellationOntology",
        },
        "parcellationVolume": {
            "@id": "parcellation-volume-id",
            "_rev": 1,
            "@type": "ParcellationVolume",
        },
        "spatialReferenceSystem": {
            "@id": "spatial-reference-system-id",
            "_rev": 1,
            "@type": "SpatialReferenceSystem",
        },
        "brainLocation": {"brainRegion": {"@id": "mba:997", "label": "root"}},
    }


@pytest.fixture(scope="module")
def configuration_metadata():
    return {
        "@id": CONFIGURATION_ID,
        "@type": "VariantTaskParameterization",
        "distribution": {
            "@type": "DataDownload",
            "encodingFormat": "application/json",
            "contentUrl": CONFIGURATION_DISTRIBUTION_URL,
        },
    }


@pytest.fixture(scope="module")
def configuration_distribution():
    return load_json(DATA_DIR / "configuration_distribution.json")


@pytest.fixture(scope="module")
def base_cell_composition_metadata():
    return {
        "@id": BASE_CELL_COMPOSITION_ID,
        "@type": "CellComposition",
        "atlasRelease": {
            "@id": ATLAS_RELEASE_ID,
            "@type": "AtlasRelease",
        },
        "cellCompositionVolume": {
            "@id": BASE_VOLUME_ID,
            "@type": "CellCompositionVolume",
        },
        "cellCompositionSummary": {
            "@id": BASE_SUMMARY_ID,
            "@type": "CellCompositionSummary",
        },
    }


@pytest.fixture(scope="module")
def base_volume_metadata():
    return {
        "@id": BASE_VOLUME_ID,
        "@type": "CellCompositionVolume",
        "distribution": {
            "@type": "DataDownload",
            "encodingFormat": "application/json",
            "contentUrl": BASE_VOLUME_DISTRIBUTION_URL,
        },
    }


@pytest.fixture(scope="module")
def base_volume_distribution_file():
    return DATA_DIR / "base_cell_composition_volume_distribution.json"


@pytest.fixture(scope="module")
def base_volume_distribution(base_volume_distribution_file):
    return load_json(base_volume_distribution_file)


@pytest.fixture(scope="module")
def base_summary_metadata():
    return {
        "@id": BASE_SUMMARY_ID,
        "@type": "CellCompositionSummary",
        "distribution": {
            "@type": "DataDownload",
            "encodingFormat": "application/json",
            "contentUrl": BASE_SUMMARY_DISTRIBUTION_URL,
        },
    }


@pytest.fixture(scope="module")
def base_summary_distribution():
    return load_json(DATA_DIR / "base_cell_composition_summary_distribution.json")


@pytest.fixture(scope="module")
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


@pytest.fixture(scope="module")
def load_by_id(
    configuration_metadata,
    base_cell_composition_metadata,
    base_volume_metadata,
    base_summary_metadata,
    me_type_density_metadata,
    atlas_release_metadata,
):
    def _load_by_id(resource_id, *args, **kwargs):
        if CONFIGURATION_ID in resource_id:
            return _resp(configuration_metadata)
        if BASE_CELL_COMPOSITION_ID in resource_id:
            return _resp(base_cell_composition_metadata)
        if BASE_VOLUME_ID in resource_id:
            return _resp(base_volume_metadata)
        if BASE_SUMMARY_ID in resource_id:
            return _resp(base_summary_metadata)
        if NRRD_ID in resource_id:
            return _resp(me_type_density_metadata)
        if ATLAS_RELEASE_ID in resource_id:
            return _resp(atlas_release_metadata)
        if "updated-cell-composition-id" in resource_id:
            return _resp(base_cell_composition_metadata)
        if "mock-user-id" in resource_id:
            return _resp({"preferred_username": "mock-user"})
        raise ValueError(resource_id)

    return _load_by_id


@pytest.fixture(scope="module")
def file_as_dict(base_volume_distribution, base_summary_distribution, configuration_distribution):
    def _file_as_dict(url, *args, **kwargs):
        return {
            BASE_VOLUME_DISTRIBUTION_URL: base_volume_distribution,
            BASE_SUMMARY_DISTRIBUTION_URL: base_summary_distribution,
            CONFIGURATION_DISTRIBUTION_URL: configuration_distribution,
        }[url]

    return _file_as_dict


@pytest.fixture(scope="module")
def download_file(file_as_dict):
    def _download_file(url, path, file_name=None, *args, **kwargs):
        if url == BASE_VOLUME_DISTRIBUTION_URL:
            data = file_as_dict(url=url)
            filepath = Path(path, file_name or "volume.json")
            write_json(data=data, filepath=filepath)
            return filepath

        if url == BASE_SUMMARY_DISTRIBUTION_URL:
            data = file_as_dict(url=url)
            filepath = Path(path, file_name or "summary.json")
            write_json(data=data, filepath=filepath)
            return filepath

        if url == NRRD_URL:
            return NRRD_PATH

        if url == CONFIGURATION_DISTRIBUTION_URL:
            data = file_as_dict(url=url)
            filepath = Path(path, file_name)
            write_json(data=data, filepath=filepath)
            return filepath

        raise ValueError(url)

    return _download_file


@pytest.fixture(scope="module")
def get_unquoted_uri_path():
    def _get_unquoted_uri_path(url, *args, **kwargs):
        if url == NRRD_URL:
            return NRRD_PATH

        raise

    return _get_unquoted_uri_path


def _check_arg_consistency(cli_command, function):
    """Check that command has the same arguments as the function."""

    cmd_args = set(p.name for p in cli_command.params)
    func_args = set(inspect.signature(function).parameters.keys())

    assert cmd_args == func_args, (
        "Command arguments are not matching function ones:\n"
        f"Command args : {sorted(cmd_args)}\n"
        f"Function args: {sorted(func_args)}"
    )


def test_stage_cli():
    _check_arg_consistency(test_module.stage_cli, test_module.stage)


@pytest.fixture(scope="module")
def materialized_region_selection_file(expected_dir):
    filepath = expected_dir / "materialized_region_selection.json"
    write_json(data=[], filepath=filepath)
    return filepath


@pytest.fixture(scope="module")
def materialized_densities_file(expected_dir):
    data = pd.DataFrame.from_records(
        [
            (
                "L2_TPC:B",
                "cADpyr",
                "http://uri.interlex.org/base/ilx_0381367",
                "http://bbp.epfl.ch/neurosciencegraph/ontologies/etypes/cADpyr",
                NRRD_PATH,
            ),
        ],
        columns=["mtype", "etype", "mtype_url", "etype_url", "path"],
    )

    filepath = Path(expected_dir, "expected_densities.parquet")
    data.to_parquet(filepath)
    return filepath


@pytest.fixture(scope="module")
def materialized_recipe_file(expected_dir):
    data = pd.DataFrame.from_records(
        [
            (
                614454292,
                "http://api.brain-map.org/api/v2/data/Structure/614454292",
                "L2_TPC:B",
                "http://uri.interlex.org/base/ilx_0381367",
                "cADpyr",
                "http://bbp.epfl.ch/neurosciencegraph/ontologies/etypes/cADpyr",
                "density",
                203.913221,
            )
        ],
        columns=[
            "region_id",
            "region_url",
            "mtype",
            "mtype_url",
            "etype",
            "etype_url",
            "operation",
            "value",
        ],
    )
    filepath = Path(expected_dir, "materialized_recipe.parquet")
    data.to_parquet(filepath)
    return filepath


def test_stage(
    stage_dir,
    load_by_id,
    file_as_dict,
    download_file,
    get_unquoted_uri_path,
    materialized_region_selection_file,
    materialized_densities_file,
    materialized_recipe_file,
):
    with (
        patch("entity_management.nexus.load_by_id", side_effect=load_by_id),
        patch("entity_management.nexus.file_as_dict", side_effect=file_as_dict),
        patch("entity_management.nexus.download_file", side_effect=download_file),
        patch(
            "entity_management.nexus.get_unquoted_uri_path",
            side_effect=get_unquoted_uri_path,
        ),
        patch("blue_cwl.staging.stage_atlas"),
    ):
        test_module.stage(
            configuration_id=CONFIGURATION_ID,
            base_cell_composition_id=BASE_CELL_COMPOSITION_ID,
            stage_dir=stage_dir,
        )

    # region selection should be created empty
    res_region_selection_file = Path(stage_dir, "region_selection.json")
    assert res_region_selection_file.exists()
    assert load_json(res_region_selection_file) == load_json(materialized_region_selection_file)

    # staged volume composition should be identical to distribution
    cell_composition_volume_file = Path(stage_dir, "cell_composition_volume.json")
    assert cell_composition_volume_file.exists()
    assert load_json(cell_composition_volume_file) == file_as_dict(url=BASE_VOLUME_DISTRIBUTION_URL)

    # volume distribution is materialized into a parquet dataframe
    densities_file = Path(stage_dir, "cell_composition_volume.parquet")
    assert densities_file.exists()
    res_densities = pd.read_parquet(densities_file)
    expected_densities = pd.read_parquet(materialized_densities_file)
    pdt.assert_frame_equal(res_densities, expected_densities)

    # manipulation recipe is materialized into a parquet dataframe
    recipe_file = Path(stage_dir, "recipe.parquet")
    assert recipe_file.exists()
    res_recipe = pd.read_parquet(recipe_file)
    expected_recipe = pd.read_parquet(materialized_recipe_file)
    pdt.assert_frame_equal(res_recipe, expected_recipe)


def test_manipulate_cell_composition_cli():
    _check_arg_consistency(
        test_module.manipulate_cell_composition_cli,
        test_module.manipulate_cell_composition,
    )


@pytest.fixture(scope="module")
def atlas_file(expected_dir):
    atlas = AtlasInfo(
        ontology_path=str(DATA_DIR / "hierarchy.json"),
        annotation_path=str(DATA_DIR / "brain_regions.nrrd"),
        hemisphere_path="",
        cell_orientation_field_path="",
        ph_catalog=None,
        directory="",
    )
    filepath = Path(expected_dir, "atlas.json")
    atlas.to_file(filepath)
    return filepath


@pytest.fixture(scope="module")
def updated_density_file():
    return str(DATA_DIR / "updated_density.nrrd")


@pytest.fixture(scope="module")
def manipulated_volume_file(expected_dir, updated_density_file):
    data = {
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
                        "hasPart": [
                            {
                                "path": updated_density_file,
                                "@type": [
                                    "METypeDensity",
                                    "NeuronDensity",
                                    "VolumetricDataLayer",
                                    "CellDensityDataLayer",
                                ],
                            }
                        ],
                    }
                ],
            }
        ]
    }

    filepath = Path(expected_dir, "manipulated_cell_composition_volume.json")
    write_json(data=data, filepath=filepath)
    return filepath


@pytest.fixture(scope="module")
def manipulated_summary_file(expected_dir):
    data = {
        "version": 1,
        "unitCode": {"density": "mm^-3"},
        "hasPart": {
            "http://api.brain-map.org/api/v2/data/Structure/614454292": {
                "label": "Primary somatosensory area, barrel field, layer 2",
                "notation": "SSp-bfd2",
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
                                    "neuron": {
                                        "density": 203.9132210000376,
                                        "count": 40,
                                    }
                                },
                            }
                        },
                    }
                },
            }
        },
    }

    filepath = Path(expected_dir, "manipulated_cell_composition_summary.json")
    write_json(data=data, filepath=filepath)
    return filepath


def test_manipulate_cell_composition(
    build_dir,
    atlas_file,
    materialized_region_selection_file,
    base_volume_distribution_file,
    materialized_densities_file,
    materialized_recipe_file,
    manipulated_volume_file,
    manipulated_summary_file,
):
    test_module.manipulate_cell_composition(
        atlas_file=atlas_file,
        manipulation_file=materialized_recipe_file,
        materialized_cell_composition_volume_file=materialized_densities_file,
        cell_composition_volume_file=base_volume_distribution_file,
        region_selection_file=materialized_region_selection_file,
        output_dir=build_dir,
    )

    updated_volume_file = Path(build_dir / "cell_composition_volume.json")
    assert updated_volume_file.exists()
    res_volumes = load_json(updated_volume_file)
    expected_volumes = load_json(manipulated_volume_file)

    res_mtypes = res_volumes["hasPart"]
    expected_mtypes = expected_volumes["hasPart"]
    assert len(res_mtypes) == len(expected_mtypes) == 1

    res_etypes = res_mtypes[0]["hasPart"]
    expected_etypes = expected_mtypes[0]["hasPart"]
    assert len(res_etypes) == len(expected_etypes) == 1

    res_nrrds = res_etypes[0]["hasPart"]
    expected_nrrds = expected_etypes[0]["hasPart"]
    assert len(res_nrrds) == len(expected_nrrds) == 1

    updated_summary_file = Path(build_dir / "cell_composition_summary.json")
    assert updated_summary_file.exists()
    res_summary = load_json(updated_summary_file)
    expected_summary = load_json(manipulated_summary_file)
    assert res_summary == expected_summary

    nrrd_dir = Path(build_dir, "nrrds")
    assert set(nrrd_dir.iterdir()) == {nrrd_dir / Path(NRRD_PATH).name}


def test_register_cli():
    _check_arg_consistency(test_module.register_cli, test_module.register)


def _file_resp(filepath):
    filename = Path(filepath).name
    return {
        "@id": "file-id",
        "@type": "File",
        "_bytes": 35052232,
        "_digest": {
            "_algorithm": "SHA-256",
            "_value": "3cb2ab9350f5a69f7e070b061d0f8cd2f4948350bd51dd87f3353262e0c4ef91",
        },
        "_filename": filename,
        "_location": "file:///gpfs/cell_composition_volume_distribution.json",
        "_mediaType": "application/json",
        "_rev": 1,
        "_self": "file-self",
    }


def test_register(
    build_dir, manipulated_volume_file, manipulated_summary_file, load_by_id, updated_density_file
):
    out_resource_file = build_dir / "resource.json"

    def create(base_url, payload, *args, **kwargs):
        if payload["@type"] == "METypeDensity":
            return payload | {"@id": "updated-density-id", "_rev": 2}
        if payload["@type"] == "CellCompositionVolume":
            return payload | {"@id": "updated-volume-id", "_rev": 2}
        if payload["@type"] == "CellCompositionSummary":
            return payload | {"@id": "updated-summary-id", "_rev": 2}
        if payload["@type"] == "CellComposition":
            return payload | {"@id": "updated-cell-composition-id", "_rev": 2}
        raise ValueError(payload)

    def upload_file(name, data, *args, **kwargs):
        return {
            "updated_density.nrrd": _file_resp(updated_density_file),
            "registered_cell_composition_volume.json": _file_resp(manipulated_volume_file),
            "manipulated_cell_composition_summary.json": _file_resp(manipulated_summary_file),
        }[name]

    with (
        patch("entity_management.nexus.load_by_id", side_effect=load_by_id),
        patch("entity_management.nexus.upload_file", side_effect=upload_file),
        patch("entity_management.nexus.create", side_effect=create),
        patch("blue_cwl.wrappers.cell_composition_manipulation._validate_cell_composition_schemas"),
        patch("entity_management.state.get_user_id", return_value="mock-user-id"),
    ):
        test_module.register(
            base_cell_composition_id=BASE_CELL_COMPOSITION_ID,
            cell_composition_volume_file=manipulated_volume_file,
            cell_composition_summary_file=manipulated_summary_file,
            output_dir=build_dir,
            output_resource_file=out_resource_file,
        )

    # check registered file has only ids
    res_volume_file = build_dir / "registered_cell_composition_volume.json"
    assert res_volume_file.exists()
    res_densities = load_json(res_volume_file)

    assert res_densities == {
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
                        "hasPart": [
                            {
                                "@type": [
                                    "METypeDensity",
                                    "NeuronDensity",
                                    "VolumetricDataLayer",
                                    "CellDensityDataLayer",
                                ],
                                "@id": "updated-density-id",
                                "_rev": 2,
                            }
                        ],
                    }
                ],
            }
        ]
    }

    assert out_resource_file.exists()
    res_resource = load_json(out_resource_file)
    assert res_resource == {"@id": "updated-cell-composition-id", "@type": "CellComposition"}
