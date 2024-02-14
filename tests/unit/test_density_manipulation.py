from unittest.mock import patch
import voxcell
import numpy as np
import numpy.testing as npt
import tempfile
import pytest

from pathlib import Path

from voxcell.nexus.voxelbrain import Atlas
from cwl_registry import density_manipulation as test_module
from cwl_registry import statistics, utils
from cwl_registry.staging import materialize_cell_composition_summary
from cwl_registry.statistics import _get_nrrd_statistics
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
def manipulation_recipe():
    return test_module.read_density_manipulation_recipe(DENSITY_MANIPULATION_RECIPE)


@pytest.fixture
def empty_manipulation_recipe():
    return pd.DataFrame([], columns=["region_id", "mtype", "etype", "operation", "value"])


def test__read_density_manipulation_recipe(manipulation_recipe):
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
            "region_id",
            "mtype",
            "etype",
            "operation",
            "value",
        ],
    )
    pdt.assert_frame_equal(
        manipulation_recipe.loc[:, ["region_id", "mtype", "etype", "operation", "value"]], expected
    )


@pytest.fixture
def materialized_cell_composition_volume(tmpdir, brain_regions):
    ones = brain_regions.with_data(np.ones_like(brain_regions.raw, dtype=float))

    def _write_nrrd(path):
        ones.save_nrrd(path)
        return str(path)

    # make "Nucleus raphe obscurus" have no density
    ones.raw[brain_regions.raw == 222] = 0

    df_rows = [
        (
            "L23_LBC",
            "http://uri.interlex.org/base/ilx_0383202",
            "bAC",
            "http://uri.interlex.org/base/ilx_0738199",
            str(_write_nrrd(tmpdir / "L23_LBC__bAC.nrrd")),
        ),
        (
            "GEN_mtype",
            "https://bbp.epfl.ch/ontologies/core/bmo/GenericExcitatoryNeuronMType",
            "GEN_etype",
            "https://bbp.epfl.ch/ontologies/core/bmo/GenericExcitatoryNeuronEType",
            str(_write_nrrd(tmpdir / "GEN_mtype__GEN_etype.nrrd")),
        ),
        (
            "GIN_mtype",
            "https://bbp.epfl.ch/ontologies/core/bmo/GenericInhibitoryNeuronMType",
            "GIN_etype",
            "https://bbp.epfl.ch/ontologies/core/bmo/GenericInhibitoryNeuronEType",
            str(_write_nrrd(tmpdir / "GIN_mtype__GIN_etype.nrrd")),
        ),
    ]
    return pd.DataFrame(df_rows, columns=["mtype", "mtype_url", "etype", "etype_url", "path"])


def test_flat_groups_mapper():
    values = np.array([10, 20, 10, 30, 30, 30, 20, 20, 10])

    g = test_module._FlatGroupsMapper(values)

    ids = g.get_group_indices_by_value(10)
    npt.assert_array_equal(ids, [0, 2, 8])

    ids = g.get_group_indices_by_value(20)
    npt.assert_array_equal(ids, [1, 6, 7])

    ids = g.get_group_indices_by_value(30)
    npt.assert_array_equal(ids, [3, 4, 5])


def test__create_updated_densities(
    tmpdir, brain_regions, manipulation_recipe, materialized_cell_composition_volume
):
    updated_densities = test_module._create_updated_densities(
        output_dir=tmpdir,
        brain_regions=brain_regions,
        all_operations=manipulation_recipe,
        materialized_densities=materialized_cell_composition_volume,
    )

    p0 = "L23_LBC__bAC"
    p1 = "GEN_mtype__GEN_etype"
    p2 = "GIN_mtype__GIN_etype"

    nrrd_files = set(Path(tmpdir).glob("*"))
    assert nrrd_files == {Path(tmpdir) / f"{i}.nrrd" for i in [p0, p1, p2]}

    # updated L23_LBC in ACAd1 / "Anterior cingulate area, dorsal part, layer 1"
    data = voxcell.VoxelData.load_nrrd(tmpdir / f"{p0}.nrrd")
    assert ((data.raw == 30) == (brain_regions.raw == 935)).all()

    # updated GEN_mtype in  AAA / "Anterior amygdalar area"
    data = voxcell.VoxelData.load_nrrd(tmpdir / f"{p1}.nrrd")
    assert ((data.raw == 10) == (brain_regions.raw == 23)).all()

    # updated GIN_mtype in  AAA / "Anterior amygdalar area"
    data = voxcell.VoxelData.load_nrrd(tmpdir / f"{p2}.nrrd")
    assert ((data.raw == 20) == (brain_regions.raw == 23)).all()

    # updated GIN_mtype RO / "Nucleus raphe obscurus"
    data = voxcell.VoxelData.load_nrrd(tmpdir / f"{p2}.nrrd")
    assert ((data.raw == 0) == (brain_regions.raw == 222)).all()


def test__create_updated_densities__empty_manipulations(
    tmpdir,
    brain_regions,
    manipulation_recipe,
    materialized_cell_composition_volume,
    empty_manipulation_recipe,
):
    updated_densities = test_module._create_updated_densities(
        tmpdir, brain_regions, empty_manipulation_recipe, materialized_cell_composition_volume
    )

    pd.testing.assert_frame_equal(
        left=materialized_cell_composition_volume[["mtype", "etype", "path"]],
        right=updated_densities[["mtype", "etype", "path"]],
    )

    # check that no density was flagged as updated
    assert all(updated_densities.updated == False)


@pytest.fixture
def region_selection(region_map):
    return sorted([region_map.find(r, attr="acronym").pop() for r in ["VISp2", "VISp3", "ACAd1"]])


def _non_zero_density_ids(density, brain_regions):
    return sorted(pd.unique(brain_regions.raw[density.raw != 0.0]))


def test__create_updated_densities__selection(
    tmpdir,
    brain_regions,
    manipulation_recipe,
    materialized_cell_composition_volume,
    region_selection,
):
    updated_densities = test_module._create_updated_densities(
        output_dir=tmpdir,
        brain_regions=brain_regions,
        all_operations=manipulation_recipe,
        materialized_densities=materialized_cell_composition_volume,
        region_selection=region_selection,
    )

    p0 = "L23_LBC__bAC"
    p1 = "GEN_mtype__GEN_etype"
    p2 = "GIN_mtype__GIN_etype"

    nrrd_files = set(Path(tmpdir).glob("*"))
    assert nrrd_files == {Path(tmpdir) / f"{i}.nrrd" for i in [p0, p1, p2]}

    # L23_LBC

    # assert nrrd_files == {Path(tmpdir) / f"{i}.nrrd" for i in range(3)}
    # updated L23_LBC in ACAd1 / "Anterior cingulate area, dorsal part, layer 1"
    v0 = voxcell.VoxelData.load_nrrd(tmpdir / "L23_LBC__bAC.nrrd")
    assert _non_zero_density_ids(v0, brain_regions) == region_selection

    # the ACAd1 is manipulated to a density_ratio 30. x 1. = 30.
    assert ((v0.raw == 30) == (brain_regions.raw == 935)).all()

    # we expect that apart from the manipulation the rest of the regions in the selection
    # that are not manipulated will have density 1.
    assert pd.unique(brain_regions.raw[v0.raw == 1.0]).tolist() == [614454330, 614454331]

    # GEN_mtype
    v1 = voxcell.VoxelData.load_nrrd(tmpdir / "GEN_mtype__GEN_etype.nrrd")
    assert _non_zero_density_ids(v1, brain_regions) == region_selection

    # no manipulations in the region selection -> default density 1.
    assert np.all(v1.raw[v1.raw != 0.0] == 1.0)

    # GIN_mtype
    v2 = voxcell.VoxelData.load_nrrd(tmpdir / "GIN_mtype__GIN_etype.nrrd")
    assert _non_zero_density_ids(v2, brain_regions) == region_selection

    # no manipulation in the region selection
    assert np.all(v2.raw[v1.raw != 0.0] == 1.0)


def test__create_updated_densities__selection__empty_overrides(
    tmpdir,
    region_map,
    brain_regions,
    materialized_cell_composition_volume,
    empty_manipulation_recipe,
    region_selection,
):
    updated_densities = test_module._create_updated_densities(
        tmpdir,
        brain_regions,
        empty_manipulation_recipe,
        materialized_cell_composition_volume,
        region_selection,
    )

    pd.testing.assert_frame_equal(
        left=materialized_cell_composition_volume[["mtype", "etype", "path"]],
        right=updated_densities[["mtype", "etype", "path"]],
    )

    # check that no density was flagged as updated
    assert all(updated_densities.updated == True)


@pytest.fixture
def materialized_cell_composition_summary(cell_composition_summary):
    return materialize_cell_composition_summary(cell_composition_summary)


@pytest.fixture
def updated_densities(tmpdir, brain_regions):
    path = tmpdir / "L23_LBC.nrrd"
    raw = 0.1 * np.ones_like(brain_regions.raw)
    brain_regions.with_data(raw).save_nrrd(path)
    updated_densities = pd.DataFrame(
        [
            [
                "L23_LBC",
                "http://uri.interlex.org/base/ilx_0383202",
                "bAC",
                "http://uri.interlex.org/base/ilx_0738199",
                str(path),
            ]
        ],
        columns=["mtype", "mtype_url", "etype", "etype_url", "path"],
    )
    return updated_densities


def test__update_density_summary_statistics(
    tmpdir, region_map, brain_regions, materialized_cell_composition_summary, updated_densities
):
    original_density_release = None

    res = _get_nrrd_statistics(
        brain_regions=brain_regions,
        region_map=region_map,
        densities=updated_densities,
        map_function=map,
    )

    assert np.allclose(res.loc[("RSPagl2", "L23_LBC", "bAC")][["count", "density"]], (0.0, 0.1))
    assert np.allclose(res.loc[("RSPagl3", "L23_LBC", "bAC")][["count", "density"]], (0.0, 0.1))
