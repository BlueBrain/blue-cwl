from unittest.mock import patch
import voxcell
import numpy as np
import tempfile
import pytest

from pathlib import Path

from voxcell.nexus.voxelbrain import Atlas
from cwl_registry import density_manipulation as test_module
from cwl_registry import statistics, utils
import pandas as pd
import pandas.testing as pdt

DENSITY_MANIPULATION_RECIPE = {
    "version": 1,
    "overrides": {
        "http://api.brain-map.org/api/v2/data/Structure/23": {
            "hasPart": {
                "https://bbp.epfl.ch/ontologies/core/bmo/GenericExcitatoryNeuronMType?rev=1": {
                    "label": "GEN_mtype",
                    "about": "MType",
                    "hasPart": {
                        "https://bbp.epfl.ch/ontologies/core/bmo/GenericExcitatoryNeuronEType": {
                            "label": "GEN_etype",
                            "about": "EType",
                            "density": 10,
                        }
                    },
                },
                "https://bbp.epfl.ch/ontologies/core/bmo/GenericInhibitoryNeuronMType": {
                    "label": "GIN_mtype",
                    "about": "MType",
                    "hasPart": {
                        "https://bbp.epfl.ch/ontologies/core/bmo/GenericInhibitoryNeuronEType?rev=0": {
                            "label": "GIN_etype",
                            "about": "EType",
                            "density": 20,
                        }
                    },
                },
            },
        },
        "http://api.brain-map.org/api/v2/data/Structure/935": {
            "hasPart": {
                "L23_LBC_ID": {
                    "label": "L23_LBC",
                    "about": "MType",
                    "hasPart": {"bAC_ID": {"label": "bAC", "about": "EType", "density_ratio": 30}},
                }
            },
        },
        "http://api.brain-map.org/api/v2/data/Structure/222": {
            "hasPart": {
                "L23_LBC_ID": {
                    "label": "L23_LBC",
                    "about": "MType",
                    # includes manipulation of something with zero density
                    "hasPart": {"bAC_ID": {"label": "bAC", "about": "EType", "density_ratio": 20}},
                }
            },
        },
    },
}

MTYPE_URLS = {
    "GIN_mtype": "https://bbp.epfl.ch/ontologies/core/bmo/GenericInhibitoryNeuronMType",
    "GEN_mtype": "https://bbp.epfl.ch/ontologies/core/bmo/GenericExcitatoryNeuronMType",
    "L23_LBC": "http://uri.interlex.org/base/ilx_0383202",
}

ETYPE_URLS = {
    "bAC": "http://uri.interlex.org/base/ilx_0738199",
    "GIN_etype": "https://bbp.epfl.ch/ontologies/core/bmo/GenericInhibitoryNeuronEType",
    "GEN_etype": "https://bbp.epfl.ch/ontologies/core/bmo/GenericExcitatoryNeuronEType",
}
MTYPE_URLS_INVERSE = {v: k for k, v in MTYPE_URLS.items()}
ETYPE_URLS_INVERSE = {v: k for k, v in ETYPE_URLS.items()}


DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture
def atlas():
    return Atlas.open(str(DATA_DIR / "atlas"))


@pytest.fixture
def region_map(atlas):
    return atlas.load_region_map()


@pytest.fixture
def brain_regions(atlas):
    return atlas.load_data("brain_regions")


@pytest.fixture
def cell_composition_volume(tmpdir, brain_regions):
    ones = brain_regions.with_data(np.ones_like(brain_regions.raw, dtype=float))

    # make "Nucleus raphe obscurus" have no density
    ones.raw[brain_regions.raw == 222] = 0
    for i in range(3):
        ones.save_nrrd(tmpdir / f"{i}.nrrd")

    return {
        "mtypes": {
            "http://uri.interlex.org/base/ilx_0383202": {  # L23_LBC
                "etypes": {"http://uri.interlex.org/base/ilx_0738199": {"path": tmpdir / "0.nrrd"}}
            },
            "https://bbp.epfl.ch/ontologies/core/bmo/GenericExcitatoryNeuronMType": {
                "etypes": {
                    "https://bbp.epfl.ch/ontologies/core/bmo/GenericExcitatoryNeuronEType": {
                        "path": tmpdir / "1.nrrd"
                    }
                }
            },
            "https://bbp.epfl.ch/ontologies/core/bmo/GenericInhibitoryNeuronMType": {
                "etypes": {
                    "https://bbp.epfl.ch/ontologies/core/bmo/GenericInhibitoryNeuronEType": {
                        "path": tmpdir / "2.nrrd"
                    }
                }
            },
        }
    }


def test__read_density_manipulation_recipe():
    res = test_module._read_density_manipulation_recipe(DENSITY_MANIPULATION_RECIPE)
    expected = pd.DataFrame(
        [
            (23, "GEN_mtype", "GEN_etype", "density", 10),
            (23, "GIN_mtype", "GIN_etype", "density", 20),
            (
                935,
                "L23_LBC",
                "bAC",
                "density_ratio",
                30,
            ),
            (
                222,
                "L23_LBC",
                "bAC",
                "density_ratio",
                20,
            ),
        ],
        columns=[
            "region",
            "mtype",
            "etype",
            "operation",
            "value",
        ],
    )
    pdt.assert_frame_equal(res, expected)


def test__cell_composition_volume_to_df(tmpdir, cell_composition_volume):
    res = test_module._cell_composition_volume_to_df(
        cell_composition_volume, MTYPE_URLS_INVERSE, ETYPE_URLS_INVERSE
    )
    expected = pd.DataFrame(
        [
            ("L23_LBC", "bAC", tmpdir / "0.nrrd"),
            ("GEN_mtype", "GEN_etype", tmpdir / "1.nrrd"),
            ("GIN_mtype", "GIN_etype", tmpdir / "2.nrrd"),
        ],
        columns=[
            "mtype",
            "etype",
            "path",
        ],
    ).set_index(["mtype", "etype"])

    pdt.assert_frame_equal(res, expected)


def test__create_updated_densities(tmpdir, brain_regions, cell_composition_volume):
    all_operations = test_module._read_density_manipulation_recipe(DENSITY_MANIPULATION_RECIPE)
    materialized_densities = test_module._cell_composition_volume_to_df(
        cell_composition_volume, MTYPE_URLS_INVERSE, ETYPE_URLS_INVERSE
    )
    updated_densities = test_module._create_updated_densities(
        tmpdir, brain_regions, all_operations, materialized_densities
    )

    nrrd_files = set(Path(tmpdir).glob("*"))
    assert nrrd_files == {Path(tmpdir) / f"{i}.nrrd" for i in range(3)}

    # updated L23_LBC in ACAd1 / "Anterior cingulate area, dorsal part, layer 1"
    data = voxcell.VoxelData.load_nrrd(tmpdir / "0.nrrd")
    assert ((data.raw == 30) == (brain_regions.raw == 935)).all()

    # updated GEN_mtype in  AAA / "Anterior amygdalar area"
    data = voxcell.VoxelData.load_nrrd(tmpdir / "1.nrrd")
    assert ((data.raw == 10) == (brain_regions.raw == 23)).all()

    # updated GIN_mtype in  AAA / "Anterior amygdalar area"
    data = voxcell.VoxelData.load_nrrd(tmpdir / "2.nrrd")
    assert ((data.raw == 20) == (brain_regions.raw == 23)).all()

    # updated GIN_mtype RO / "Nucleus raphe obscurus"
    data = voxcell.VoxelData.load_nrrd(tmpdir / "2.nrrd")
    assert ((data.raw == 0) == (brain_regions.raw == 222)).all()


def test__update_density_summary_statistics(tmpdir, region_map, brain_regions):
    original_density_release = None

    path = tmpdir / "L23_LBC.nrrd"
    raw = 0.1 * np.ones_like(brain_regions.raw)
    brain_regions.with_data(raw).save_nrrd(path)
    updated_densities = pd.DataFrame([["L23_LBC", "bAC", path]], columns=["mtype", "etype", "path"])

    original_cell_composition_summary = utils.load_json(DATA_DIR / "cell_density_summary.json")
    original_cell_composition_summary = statistics.cell_composition_summary_to_df(
        original_cell_composition_summary, region_map, MTYPE_URLS_INVERSE, ETYPE_URLS_INVERSE
    )
    res = test_module._update_density_summary_statistics(
        original_cell_composition_summary,
        brain_regions,
        region_map,
        updated_densities,
    )
    assert np.allclose(res.loc[("RSPagl2", "L23_LBC", "bAC")][["count", "density"]], (0.0, 0.1))
    assert np.allclose(res.loc[("RSPagl3", "L23_LBC", "bAC")][["count", "density"]], (0.0, 0.1))


# def density_manipulation(output_dir,
#                         region_map,
#                         brain_regions,
#                         manipulation_recipe,
#                         materialized_cell_composition_volume,
#                         original_density_release,
#                         mtype_urls,
#                         etype_urls):
#
# def update_composition_summary_statistics(
#    original_stats, brain_regions, region_map, updated_densities, mtype_urls, etype_urls
