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


def test_build_connectome_distance_dependent_recipe():
    configuration = pd.DataFrame(
        {
            "hi": ["left"],
            "hj": ["left"],
            "ri": ["SSp-bfd2"],
            "rj": ["SSp-bfd2"],
            "mi": ["L23_LBC"],
            "mj": ["L23_LBC"],
            "scale": 0.11,
            "exponent": 0.007,
            "mean_synapses_per_connection": 100,
            "sdev_synapses_per_connection": 1,
            "mean_conductance_velocity": 0.3,
            "sdev_conductance_velocity": 0.01,
        }
    )

    config_path = "my-config-path"
    output_dir = "my-dir"

    res = tested.build_connectome_distance_dependent_recipe(config_path, configuration, output_dir)

    assert res == {
        "circuit_config": "my-config-path",
        "output_path": "my-dir",
        "seed": 0,
        "manip": {
            "name": "ConnWiringPerPathway_DD",
            "fcts": [
                {
                    "source": "conn_wiring",
                    "kwargs": {
                        "sel_src": {
                            "region": "SSp-bfd2",
                            "mtype": "L23_LBC",
                        },
                        "sel_dest": {
                            "region": "SSp-bfd2",
                            "mtype": "L23_LBC",
                        },
                        "amount_pct": 100.0,
                        "prob_model_file": {
                            "model": "ConnProb2ndOrderExpModel",
                            "scale": 0.11,
                            "exponent": 0.007,
                        },
                        "nsynconn_model_file": {
                            "model": "ConnPropsModel",
                            "src_types": ["L23_LBC"],
                            "tgt_types": ["L23_LBC"],
                            "prop_stats": {
                                "n_syn_per_conn": {
                                    "L23_LBC": {
                                        "L23_LBC": {
                                            "type": "gamma",
                                            "mean": 100,
                                            "std": 1,
                                            "dtype": "int",
                                            "lower_bound": 1,
                                            "upper_bound": 1000,
                                        }
                                    }
                                }
                            },
                        },
                        "delay_model_file": {
                            "model": "LinDelayModel",
                            "delay_mean_coefs": [0.3, 0.003],
                            "delay_std": 0.01,
                            "delay_min": 0.2,
                        },
                    },
                },
            ],
        },
    }
