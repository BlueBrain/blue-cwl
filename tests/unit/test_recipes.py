import pytest
import pandas as pd
from pandas import testing as pdt
from cwl_registry import recipes as tested


def test_build_cell_composition_from_me_densities():
    dataset = {
        "mtypes": {
            "http://uri.interlex.org/base/ilx_0383198": {
                "label": "L23_BP",
                "etypes": {
                    "http://uri.interlex.org/base/ilx_0738202": {
                        "label": "dSTUT",
                        "path": "L23_BP-DSTUT_densities_v3.nrrd",
                    },
                    "http://uri.interlex.org/base/ilx_0738206": {
                        "label": "bIR",
                        "path": "L23_BP-BIR_densities_v3.nrrd",
                    },
                },
            },
            "http://uri.interlex.org/base/ilx_0383201": {
                "label": "L23_DBC",
                "etypes": {
                    "http://uri.interlex.org/base/ilx_0738206": {
                        "label": "bIR",
                        "path": "L23_DBC-BIR_densities_v3.nrrd",
                    },
                },
            },
        },
    }
    res = tested.build_cell_composition_from_me_densities(
        region="my-region",
        me_type_densities=dataset,
    )
    assert res == {
        "version": "v2",
        "neurons": [
            {
                "density": "L23_BP-DSTUT_densities_v3.nrrd",
                "region": "my-region",
                "traits": {"mtype": "L23_BP", "etype": "dSTUT"},
            },
            {
                "density": "L23_BP-BIR_densities_v3.nrrd",
                "region": "my-region",
                "traits": {"mtype": "L23_BP", "etype": "bIR"},
            },
            {
                "density": "L23_DBC-BIR_densities_v3.nrrd",
                "region": "my-region",
                "traits": {"mtype": "L23_DBC", "etype": "bIR"},
            },
        ],
    }


def test_build_mtype_taxonomy():
    res = tested.build_mtype_taxonomy(["L1_DAC", "GEN_mtype"])

    expected = pd.DataFrame(
        {
            "mtype": ["L1_DAC", "GEN_mtype"],
            "mClass": ["INT", "PYR"],
            "sClass": ["INH", "EXC"],
        }
    )
    pdt.assert_frame_equal(res, expected)
