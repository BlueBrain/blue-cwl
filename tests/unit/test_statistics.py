import tempfile
import libsonata
import voxcell
from pathlib import Path

import pytest
import numpy as np
import numpy.testing as npt

import pandas as pd
from pandas import testing as pdt

from voxcell.nexus.voxelbrain import Atlas
from cwl_registry import statistics as test_module


DATA_DIR = Path(__file__).parent / "data"


MTYPE_URIS = {
    "L23_BP": "http://uri.interlex.org/base/ilx_0383198",
    "L5_TPC:A": "http://uri.interlex.org/base/ilx_0381365",
}


ETYPE_URIS = {
    "cADpyr": "http://bbp.epfl.ch/neurosciencegraph/ontologies/etypes/cADpyr",
    "dSTUT": "http://uri.interlex.org/base/ilx_0738202",
}


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
def mtype_urls():
    return {mtype: MTYPE_URIS[mtype] for mtype in ("L23_BP", "L5_TPC:A")}


@pytest.fixture
def etype_urls():
    return {etype: ETYPE_URIS[etype] for etype in ("cADpyr", "dSTUT")}


@pytest.fixture
def density_distribution(brain_regions):
    with tempfile.TemporaryDirectory() as tdir:
        tdir = Path(tdir)

        v1_raw = np.zeros_like(brain_regions.raw)
        # mask selected so that it includes 0 and two regions: 320, 656
        v1_raw[100:120, 100:102, 100:120] = 20000.0
        path1 = tdir / "L23_BP__dSTUT_density.nrrd"
        brain_regions.with_data(v1_raw).save_nrrd(path1)

        v2_raw = np.zeros_like(brain_regions.raw)
        # empty
        path2 = tdir / "L5_TPCA__cADpyr_density.nrrd"
        brain_regions.with_data(v2_raw).save_nrrd(path2)

        distribution = {
            "mtypes": {
                MTYPE_URIS["L23_BP"]: {
                    "label": "L23_BP",
                    "etypes": {ETYPE_URIS["dSTUT"]: {"label": "dSTUT", "path": path1}},
                },
                MTYPE_URIS["L5_TPC:A"]: {
                    "label": "L5_TPC:A",
                    "etypes": {ETYPE_URIS["cADpyr"]: {"label": "cADpyr", "path": path2}},
                },
            },
        }

        yield distribution


@pytest.fixture
def population():
    positions = np.array(
        [
            [5990.0, 6224.0, 3419.0],
            [5700.0, 6964.0, 7553.0],
            [6046.0, 6353.0, 7915.0],
            [6081.0, 6119.0, 7869.0],
            [6210.0, 6214.0, 8048.0],
            [6019.0, 1225.0, 5414.0],
            [3013.0, 3992.0, 3381.0],
            [12301.0, 2749.0, 7787.0],
        ]
    )
    mtypes = ["L23_BP", "L5_TPC:A", "L5_TPC:A", "L23_BP", "L23_BP", "L23_BP", "L5_TPC:A", "L23_BP"]
    etypes = ["dSTUT", "cADpyr", "cADpyr", "dSTUT", "dSTUT", "dSTUT", "cADpyr", "dSTUT"]

    regions = ["AAA", "AAA", "AAA", "AAA", "AAA", "ACAd5", "AId5", "ANcr2"]

    cells = voxcell.CellCollection()
    cells.positions = positions
    cells.add_properties({"mtype": mtypes, "etype": etypes, "region": regions})

    n_cells = len(positions)

    with tempfile.NamedTemporaryFile(suffix=".h5") as tfile:
        path = Path(tfile.name)

        cells.save_sonata(path, forced_library=["mtype", "etype", "region"])

        yield libsonata.NodeStorage(path).open_population("default")


def test_mtype_etype_url_mapping(density_distribution):
    res_mtype_urls, res_etype_urls = test_module.mtype_etype_url_mapping(density_distribution)

    assert res_mtype_urls == {"L23_BP": MTYPE_URIS["L23_BP"], "L5_TPC:A": MTYPE_URIS["L5_TPC:A"]}
    assert res_etype_urls == {"dSTUT": ETYPE_URIS["dSTUT"], "cADpyr": ETYPE_URIS["cADpyr"]}


def test_node_population_composition_summary(population, atlas, mtype_urls, etype_urls):
    res = test_module.node_population_composition_summary(population, atlas, mtype_urls, etype_urls)

    # regions
    AAA = "http://api.brain-map.org/api/v2/data/Structure/23"
    ACAd5 = "http://api.brain-map.org/api/v2/data/Structure/1015"
    AId5 = "http://api.brain-map.org/api/v2/data/Structure/1101"
    ANcr2 = "http://api.brain-map.org/api/v2/data/Structure/1064"

    # mtypes
    L23_BP = "http://uri.interlex.org/base/ilx_0383198"
    L5_TPC_A = "http://uri.interlex.org/base/ilx_0381365"

    # etypes
    dSTUT = "http://uri.interlex.org/base/ilx_0738202"
    cADpyr = "http://bbp.epfl.ch/neurosciencegraph/ontologies/etypes/cADpyr"

    uri_to_label = {
        AAA: "Anterior amygdalar area",
        ACAd5: "Anterior cingulate area, dorsal part, layer 5",
        AId5: "Agranular insular area, dorsal part, layer 5",
        ANcr2: "Crus 2",
        L23_BP: "L23_BP",
        L5_TPC_A: "L5_TPC:A",
        dSTUT: "dSTUT",
        cADpyr: "cADpyr",
    }

    expected = {
        AAA: {
            L23_BP: {
                dSTUT: {
                    "neuron": {
                        "density": 6.0659673954252495,
                        "count": 3,
                    },
                },
            },
            L5_TPC_A: {
                cADpyr: {
                    "neuron": {
                        "density": 4.043978263616833,
                        "count": 2,
                    },
                },
            },
        },
        ACAd5: {
            L23_BP: {
                dSTUT: {
                    "neuron": {
                        "density": 0.8345612685331282,
                        "count": 1,
                    },
                },
            },
        },
        AId5: {
            L5_TPC_A: {
                cADpyr: {
                    "neuron": {
                        "density": 0.6623064823246958,
                        "count": 1,
                    },
                },
            },
        },
        ANcr2: {
            L23_BP: {
                dSTUT: {
                    "neuron": {
                        "density": 0.19559543529152887,
                        "count": 1,
                    },
                },
            },
        },
    }

    assert res["unitCode"] == {"density": "mm^-3"}

    regions = res["hasPart"]
    assert set(regions) == set(expected)

    for region_id, expected_mtypes in expected.items():
        region = regions[region_id]
        assert region["about"] == "BrainRegion"
        assert region["label"] == uri_to_label[region_id]

        mtypes = region["hasPart"]
        assert set(mtypes) == set(expected_mtypes)

        for mtype_id, expected_etypes in expected_mtypes.items():
            mtype = mtypes[mtype_id]
            assert mtype["about"] == "MType"
            assert mtype["label"] == uri_to_label[mtype_id]

            etypes = mtype["hasPart"]
            assert set(etypes) == set(expected_etypes)

            for etype_id, expected_composition in expected_etypes.items():
                etype = etypes[etype_id]
                assert etype["about"] == "EType"
                assert etype["label"] == uri_to_label[etype_id]

                density = etype["composition"]["neuron"]["density"]
                count = etype["composition"]["neuron"]["count"]

                npt.assert_almost_equal(density, expected_composition["neuron"]["density"])
                assert count == expected_composition["neuron"]["count"]


def test_get_statistics_from_nrrd_volume(region_map, brain_regions):
    raw = np.zeros_like(brain_regions.raw, dtype=float)

    # mask selected so that it includes 0 and two regions: 320, 656
    raw[100:120, 100:102, 100:120] = 20000.0

    density = brain_regions.with_data(raw)

    mtype = "my-mtype"
    etype = "my-etype"

    with tempfile.NamedTemporaryFile(suffix=".nrrd") as tfile:
        path = Path(tfile.name)
        density.save_nrrd(path)

        result = test_module.get_statistics_from_nrrd_volume(
            region_map, brain_regions, mtype, etype, path
        )

        mop1_density = density.raw[brain_regions.raw == 320]
        expected_mop1_density = np.mean(mop1_density)
        expected_mop1_counts = np.round(np.sum(mop1_density * brain_regions.voxel_volume * 1e-9))

        mos1_density = density.raw[brain_regions.raw == 656]
        expected_mos1_density = np.mean(mos1_density)
        expected_mos1_counts = np.round(np.sum(mos1_density * brain_regions.voxel_volume * 1e-9))

        assert result == [
            {
                "region": "MOp1",
                "mtype": "my-mtype",
                "etype": "my-etype",
                "density": expected_mop1_density,
                "count": expected_mop1_counts,
            },
            {
                "region": "MOs1",
                "mtype": "my-mtype",
                "etype": "my-etype",
                "density": expected_mos1_density,
                "count": expected_mos1_counts,
            },
        ]


def test_atlas_densities_composition_summary(density_distribution, region_map, brain_regions):
    result = test_module.atlas_densities_composition_summary(
        density_distribution, region_map, brain_regions
    )

    MOp1 = "MOp1"
    MOp1_URI = "http://api.brain-map.org/api/v2/data/Structure/320"

    MOs1 = "MOs1"
    MOs1_URI = "http://api.brain-map.org/api/v2/data/Structure/656"

    assert result["unitCode"] == {"density": "mm^-3"}

    assert set(result["hasPart"]) == {MOp1_URI, MOs1_URI}

    r1 = result["hasPart"][MOp1_URI]

    r1["label"] == "Primary motor area, layer 1"
    r1["notation"] == "MOp1"
    r1["about"] == "BrainReigon"

    r1_mtypes = r1["hasPart"]
    assert set(r1_mtypes) == {MTYPE_URIS["L23_BP"]}

    r1_etypes = r1_mtypes[MTYPE_URIS["L23_BP"]]["hasPart"]
    assert set(r1_etypes) == {ETYPE_URIS["dSTUT"]}

    r1_composition = r1_etypes[ETYPE_URIS["dSTUT"]]["composition"]
    assert r1_composition == {
        "neuron": {
            "density": 3.0546548240048876,
            "count": 4.0,
        },
    }

    r2 = result["hasPart"][MOs1_URI]
    r2["label"] == "Secondary motor area, layer 1"
    r2["notation"] = "MOs1"
    r2["about"] == "BrainReigon"

    r2_mtypes = r2["hasPart"]
    assert set(r2_mtypes) == {MTYPE_URIS["L23_BP"]}

    r2_etypes = r2_mtypes[MTYPE_URIS["L23_BP"]]["hasPart"]
    assert set(r2_etypes) == {ETYPE_URIS["dSTUT"]}

    r2_composition = r2_etypes[ETYPE_URIS["dSTUT"]]["composition"]
    assert r2_composition == {
        "neuron": {
            "density": 2.151303891843197,
            "count": 5.0,
        },
    }


def test__cell_composition_summary_to_df(region_map, mtype_urls, etype_urls):
    # try roundtrip of composition summary
    df_region_mtype_etype = (
        pd.DataFrame(
            (
                ("AAA", "L23_BP", "cADpyr", 10.0),
                ("AAA", "L5_TPC:A", "cADpyr", 20.0),
                ("AAA", "L23_BP", "dSTUT", 30.0),
                ("AAA", "L5_TPC:A", "dSTUT", 40.0),
                ("FRP1", "L23_BP", "cADpyr", 110.0),
                ("FRP1", "L5_TPC:A", "cADpyr", 120.0),
                ("FRP1", "L23_BP", "dSTUT", 130.0),
                ("FRP1", "L5_TPC:A", "dSTUT", 140.0),
            ),
            columns=[
                "region",
                "mtype",
                "etype",
                "density",
            ],
        )
        .set_index(["region", "mtype", "etype"])
        .sort_index()
    )

    cell_composition_summary = test_module.density_summary_stats_region(
        region_map, df_region_mtype_etype, mtype_urls, etype_urls
    )

    mtype_urls_inverse = {v: k for k, v in mtype_urls.items()}
    etype_urls_inverse = {v: k for k, v in etype_urls.items()}

    df = test_module.cell_composition_summary_to_df(
        cell_composition_summary, region_map, mtype_urls_inverse, etype_urls_inverse
    )
    pd.testing.assert_frame_equal(
        df.set_index(["region", "mtype", "etype"]).sort_index(), df_region_mtype_etype
    )
