from unittest.mock import Mock

from numpy import testing as npt
import pytest
import voxcell
import libsonata
from pathlib import Path
import pandas as pd
from pandas import testing as pdt
from blue_cwl import recipes as tested
from blue_cwl.utils import load_json


DATA_DIR = Path(__file__).parent / "data"


def test_build_cell_composition_from_me_densities():
    dataset = pd.DataFrame(
        [
            ("L23_BP", "dSTUT", "L23_BP-DSTUT_densities_v3.nrrd"),
            ("L23_BP", "bIR", "L23_BP-BIR_densities_v3.nrrd"),
            ("L23_DBC", "bIR", "L23_DBC-BIR_densities_v3.nrrd"),
        ],
        columns=["mtype", "etype", "path"],
    )
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
    mtypes = [
        "L1_DAC",
        "L1_HAC",
        "L1_LAC",
        "L1_NGC-DA",
        "L1_NGC-SA",
        "L1_SAC",
        "L1_NGC",
        "L23_BP",
        "L23_BTC",
        "L23_ChC",
        "L23_DBC",
        "L23_LBC",
        "L23_MC",
        "L23_NBC",
        "L23_NGC",
        "L23_SBC",
        "L3_TPC:C",
        "L2_IPC",
        "L2_TPC:A",
        "L2_TPC:B",
        "L3_TPC:A",
        "L3_TPC:B",
        "L4_BP",
        "L4_BTC",
        "L4_ChC",
        "L4_DBC",
        "L4_LBC",
        "L4_MC",
        "L4_NBC",
        "L4_NGC",
        "L4_SBC",
        "L4_SSC",
        "L4_TPC",
        "L4_UPC",
        "L5_BP",
        "L5_BTC",
        "L5_ChC",
        "L5_DBC",
        "L5_LBC",
        "L5_MC",
        "L5_NBC",
        "L5_NGC",
        "L5_SBC",
        "L5_TPC:A",
        "L5_TPC:B",
        "L5_TPC:C",
        "L5_UPC",
        "L6_BP",
        "L6_BPC",
        "L6_BTC",
        "L6_ChC",
        "L6_DBC",
        "L6_HPC",
        "L6_IPC",
        "L6_LBC",
        "L6_MC",
        "L6_NBC",
        "L6_NGC",
        "L6_SBC",
        "L6_TPC:A",
        "L6_TPC:C",
        "L6_UPC",
        "GEN_mtype",
        "GIN_mtype",
    ]

    res = tested.build_mtype_taxonomy(mtypes)

    expected = pd.DataFrame.from_records(
        [
            ["L1_DAC", "INT", "INH"],
            ["L1_HAC", "INT", "INH"],
            ["L1_LAC", "INT", "INH"],
            ["L1_NGC-DA", "INT", "INH"],
            ["L1_NGC-SA", "INT", "INH"],
            ["L1_SAC", "INT", "INH"],
            ["L1_NGC", "INT", "INH"],
            ["L23_BP", "INT", "INH"],
            ["L23_BTC", "INT", "INH"],
            ["L23_ChC", "INT", "INH"],
            ["L23_DBC", "INT", "INH"],
            ["L23_LBC", "INT", "INH"],
            ["L23_MC", "INT", "INH"],
            ["L23_NBC", "INT", "INH"],
            ["L23_NGC", "INT", "INH"],
            ["L23_SBC", "INT", "INH"],
            ["L3_TPC:C", "PYR", "EXC"],
            ["L2_IPC", "PYR", "EXC"],
            ["L2_TPC:A", "PYR", "EXC"],
            ["L2_TPC:B", "PYR", "EXC"],
            ["L3_TPC:A", "PYR", "EXC"],
            ["L3_TPC:B", "PYR", "EXC"],
            ["L4_BP", "INT", "INH"],
            ["L4_BTC", "INT", "INH"],
            ["L4_ChC", "INT", "INH"],
            ["L4_DBC", "INT", "INH"],
            ["L4_LBC", "INT", "INH"],
            ["L4_MC", "INT", "INH"],
            ["L4_NBC", "INT", "INH"],
            ["L4_NGC", "INT", "INH"],
            ["L4_SBC", "INT", "INH"],
            ["L4_SSC", "INT", "EXC"],
            ["L4_TPC", "PYR", "EXC"],
            ["L4_UPC", "PYR", "EXC"],
            ["L5_BP", "INT", "INH"],
            ["L5_BTC", "INT", "INH"],
            ["L5_ChC", "INT", "INH"],
            ["L5_DBC", "INT", "INH"],
            ["L5_LBC", "INT", "INH"],
            ["L5_MC", "INT", "INH"],
            ["L5_NBC", "INT", "INH"],
            ["L5_NGC", "INT", "INH"],
            ["L5_SBC", "INT", "INH"],
            ["L5_TPC:A", "PYR", "EXC"],
            ["L5_TPC:B", "PYR", "EXC"],
            ["L5_TPC:C", "PYR", "EXC"],
            ["L5_UPC", "PYR", "EXC"],
            ["L6_BP", "INT", "INH"],
            ["L6_BPC", "PYR", "EXC"],
            ["L6_BTC", "INT", "INH"],
            ["L6_ChC", "INT", "INH"],
            ["L6_DBC", "INT", "INH"],
            ["L6_HPC", "PYR", "EXC"],
            ["L6_IPC", "PYR", "EXC"],
            ["L6_LBC", "INT", "INH"],
            ["L6_MC", "INT", "INH"],
            ["L6_NBC", "INT", "INH"],
            ["L6_NGC", "INT", "INH"],
            ["L6_SBC", "INT", "INH"],
            ["L6_TPC:A", "PYR", "EXC"],
            ["L6_TPC:C", "PYR", "EXC"],
            ["L6_UPC", "PYR", "EXC"],
            ["GEN_mtype", "PYR", "EXC"],
            ["GIN_mtype", "INT", "INH"],
        ],
        columns=["mtype", "mClass", "sClass"],
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
    morph_ext = "h5"

    res = tested.build_connectome_distance_dependent_recipe(
        config_path, configuration, output_dir, morph_ext
    )

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
                        "morph_ext": morph_ext,
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


@pytest.fixture(scope="module")
def synaptic_classification():
    return load_json(DATA_DIR / "synaptic_parameters.json")


@pytest.fixture(scope="module")
def synaptic_assignment():
    return load_json(DATA_DIR / "synaptic_type_assignment.json")


def test_get_leaf_regions(region_map):
    res = tested._get_leaf_regions("SSp-bfd", region_map)

    acronyms = [region_map.get(rid, "acronym") for rid in res]

    assert sorted(acronyms) == [
        "SSp-bfd1",
        "SSp-bfd2",
        "SSp-bfd3",
        "SSp-bfd4",
        "SSp-bfd5",
        "SSp-bfd6a",
        "SSp-bfd6b",
        "VISrll1",
        "VISrll2",
        "VISrll3",
        "VISrll4",
        "VISrll5",
        "VISrll6a",
        "VISrll6b",
    ]


def test_get_leaf_regions__cache(region_map):
    cache = {"SSp-bfd": {981, 1047}}

    res = tested._get_leaf_regions("SSp-bfd", region_map, cache=cache)

    acronyms = [region_map.get(rid, "acronym") for rid in res]

    assert sorted(acronyms) == [
        "SSp-bfd1",
        "SSp-bfd4",
    ]


def test_get_leaf_regions__annotation_ids(region_map):
    res = tested._get_leaf_regions("SSp-bfd", region_map, annotation_ids={1047})

    acronyms = [region_map.get(rid, "acronym") for rid in res]

    assert acronyms == ["SSp-bfd4"]


@pytest.fixture(scope="module")
def populations():
    def _mock_enumeration_values(name):
        if name == "hemisphere":
            return ["left", "right"]

        if name == "region":
            return ["SSp-bfd2", "SSp-bfd3", "CA1"]

        if name == "mtype":
            return ["L5_TPC:A", "L5_TPC:B"]

        if name == "etype":
            return []

        if name == "synapse_class":
            return ["EXC", "INH"]

        raise ValueError(name)

    pop = Mock()
    pop.enumeration_values = _mock_enumeration_values

    return (pop, pop)


def test_build_tailored_properties(synaptic_assignment, region_map, annotation, populations):
    res = list(
        tested._generate_tailored_properties(
            synaptic_assignment, region_map, annotation, populations
        )
    )
    assert res == [
        {
            "fromSClass": "EXC",
            "toSClass": "EXC",
            "synapticType": "E2",
            "synapticModel": "ProbAMPANMDA_EMS.mod",
        },
        {
            "fromSClass": "EXC",
            "toSClass": "INH",
            "synapticType": "E2_INH",
            "synapticModel": "ProbAMPANMDA_EMS.mod",
        },
        {
            "fromSClass": "INH",
            "toSClass": "EXC",
            "synapticType": "I2",
            "synapticModel": "ProbGABAAB.mod",
        },
        {
            "fromSClass": "INH",
            "toSClass": "INH",
            "synapticType": "I2",
            "synapticModel": "ProbGABAAB.mod",
        },
        {
            "synapticType": "E2_L5TTPC",
            "fromHemisphere": "left",
            "toHemisphere": "left",
            "fromRegion": "SSp-bfd2",
            "toRegion": "SSp-bfd2",
            "fromMType": "L5_TPC:A",
            "toMType": "L5_TPC:B",
            "synapticModel": "ProbAMPANMDA_EMS.mod",
        },
        {
            "synapticType": "E2_L5TTPC",
            "fromHemisphere": "left",
            "toHemisphere": "left",
            "fromRegion": "SSp-bfd2",
            "toRegion": "SSp-bfd3",
            "fromMType": "L5_TPC:A",
            "toMType": "L5_TPC:B",
            "synapticModel": "ProbAMPANMDA_EMS.mod",
        },
        {
            "synapticType": "E2_L5TTPC",
            "fromHemisphere": "left",
            "toHemisphere": "left",
            "fromRegion": "SSp-bfd3",
            "toRegion": "SSp-bfd2",
            "fromMType": "L5_TPC:A",
            "toMType": "L5_TPC:B",
            "synapticModel": "ProbAMPANMDA_EMS.mod",
        },
        {
            "synapticType": "E2_L5TTPC",
            "fromHemisphere": "left",
            "toHemisphere": "left",
            "fromRegion": "SSp-bfd3",
            "toRegion": "SSp-bfd3",
            "fromMType": "L5_TPC:A",
            "toMType": "L5_TPC:B",
            "synapticModel": "ProbAMPANMDA_EMS.mod",
        },
        {
            "synapticType": "E2_L5TTPC",
            "fromHemisphere": "right",
            "toHemisphere": "right",
            "fromRegion": "SSp-bfd2",
            "toRegion": "SSp-bfd2",
            "fromMType": "L5_TPC:A",
            "toMType": "L5_TPC:B",
            "synapticModel": "ProbAMPANMDA_EMS.mod",
        },
        {
            "synapticType": "E2_L5TTPC",
            "fromHemisphere": "right",
            "toHemisphere": "right",
            "fromRegion": "SSp-bfd2",
            "toRegion": "SSp-bfd3",
            "fromMType": "L5_TPC:A",
            "toMType": "L5_TPC:B",
            "synapticModel": "ProbAMPANMDA_EMS.mod",
        },
        {
            "synapticType": "E2_L5TTPC",
            "fromHemisphere": "right",
            "toHemisphere": "right",
            "fromRegion": "SSp-bfd3",
            "toRegion": "SSp-bfd2",
            "fromMType": "L5_TPC:A",
            "toMType": "L5_TPC:B",
            "synapticModel": "ProbAMPANMDA_EMS.mod",
        },
        {
            "synapticType": "E2_L5TTPC",
            "fromHemisphere": "right",
            "toHemisphere": "right",
            "fromRegion": "SSp-bfd3",
            "toRegion": "SSp-bfd3",
            "fromMType": "L5_TPC:A",
            "toMType": "L5_TPC:B",
            "synapticModel": "ProbAMPANMDA_EMS.mod",
        },
    ]


@pytest.fixture(scope="module")
def all_properties(synaptic_assignment, region_map, annotation):
    return list(tested._generate_tailored_properties(synaptic_assignment, region_map, annotation))


def test_synapse_properties__in_annotation(all_properties, region_map, annotation):
    # check that all the regions are in the annotation

    regions = set()
    for entry in all_properties:
        r1 = entry.get("fromRegion", None)
        r2 = entry.get("toRegion", None)

        if r1:
            regions.add(r1)
        if r2:
            regions.add(r2)

    ids = set(annotation.raw.flatten())
    ids.remove(0)
    annotation_regions = {region_map.get(rid, "acronym") for rid in ids}

    difference = regions - annotation_regions
    assert not difference, difference


@pytest.fixture
def small_synapse_properties():
    return [
        {
            "fromSClass": "EXC",
            "toSClass": "EXC",
            "synapticType": "E2",
            "synapticModel": "ProbAMPANMDA_EMS.mod",
        },
        {
            "fromSClass": "EXC",
            "toSClass": "INH",
            "synapticType": "E2_INH",
            "synapticModel": "ProbAMPANMDA_EMS.mod",
        },
        {
            "fromSClass": "INH",
            "toSClass": "EXC",
            "synapticType": "I2",
            "synapticModel": "ProbGABAAB.mod",
        },
        {
            "fromSClass": "INH",
            "toSClass": "INH",
            "synapticType": "I2",
            "synapticModel": "ProbGABAAB.mod",
        },
        {
            "synapticType": "E2_L23PC",
            "fromHemisphere": "left",
            "toHemisphere": "left",
            "fromRegion": "SSp-m6b",
            "toRegion": "SSp-m6b",
            "fromMType": "L2_TPC:A",
            "toMType": "L2_TPC:A",
            "synapticModel": "ProbAMPANMDA_EMS.mod",
        },
        {
            "synapticType": "E2_L23PC",
            "fromHemisphere": "left",
            "toHemisphere": "left",
            "fromRegion": "SSp-m6b",
            "toRegion": "SSp-ul6b",
            "fromMType": "L2_TPC:A",
            "toMType": "L2_TPC:A",
            "synapticModel": "ProbAMPANMDA_EMS.mod",
        },
        {
            "synapticType": "E2_L23PC",
            "fromHemisphere": "left",
            "toHemisphere": "left",
            "fromRegion": "SSp-m6b",
            "toRegion": "SSp-ll1",
            "fromMType": "L2_TPC:A",
            "toMType": "L2_TPC:A",
            "synapticModel": "ProbAMPANMDA_EMS.mod",
        },
        {
            "synapticType": "E2_L23PC",
            "fromHemisphere": "left",
            "toHemisphere": "left",
            "fromRegion": "SSp-m6b",
            "toRegion": "SSp-tr6a",
            "fromMType": "L2_TPC:A",
            "toMType": "L2_TPC:A",
            "synapticModel": "ProbAMPANMDA_EMS.mod",
        },
        {
            "synapticType": "E2_L23PC",
            "fromHemisphere": "left",
            "toHemisphere": "left",
            "fromRegion": "SSp-m6b",
            "toRegion": "SSs4",
            "fromMType": "L2_TPC:A",
            "toMType": "L2_TPC:A",
            "synapticModel": "ProbAMPANMDA_EMS.mod",
        },
        {
            "synapticType": "E2_L23PC",
            "fromHemisphere": "left",
            "toHemisphere": "left",
            "fromRegion": "SSp-m6b",
            "toRegion": "SSp-bfd6a",
            "fromMType": "L2_TPC:A",
            "toMType": "L2_TPC:A",
            "synapticModel": "ProbAMPANMDA_EMS.mod",
        },
    ]


def test_write_functionalizer_json_recipe(
    region_map, annotation, small_synapse_properties, synaptic_classification, populations, tmp_path
):

    tdir = tmp_path / "tmpdir"
    tdir.mkdir()

    config = {
        "synapse_properties": small_synapse_properties,
        "synapses_classification": synaptic_classification,
    }

    tested.write_functionalizer_json_recipe(
        synapse_config=config,
        region_map=region_map,
        annotation=annotation,
        output_dir=Path(tdir),
        output_recipe_filename="recipe.json",
        populations=populations,
    )

    res = load_json(tdir / "recipe.json")

    synapse_rules_file = str(tdir / "synapse_rules.parquet")
    df = pd.read_parquet(synapse_rules_file)

    assert res == {
        "version": 1,
        "bouton_interval": {"min_distance": 5.0, "max_distance": 7.0, "region_gap": 5.0},
        "bouton_distances": {
            "inhibitory_synapse_distance": 5.0,
            "excitatory_synapse_distance": 25.0,
        },
        "synapse_properties": {
            "rules": synapse_rules_file,
            "classes": [
                {
                    "class": "E2",
                    "conductance_mu": 0.68,
                    "conductance_sd": 0.44,
                    "n_rrp_vesicles_mu": 1.5,
                    "decay_time_mu": 1.74,
                    "decay_time_sd": 0.18,
                    "u_syn_mu": 0.5,
                    "u_syn_sd": 0.02,
                    "depression_time_mu": 671.0,
                    "depression_time_sd": 17.0,
                    "facilitation_time_mu": 17.0,
                    "facilitation_time_sd": 5.0,
                    "conductance_scale_factor": 0.7,
                    "u_hill_coefficient": 2.79,
                },
                {
                    "class": "E2_INH",
                    "conductance_mu": 0.42,
                    "conductance_sd": 0.14,
                    "n_rrp_vesicles_mu": 1.5,
                    "decay_time_mu": 1.74,
                    "decay_time_sd": 0.18,
                    "u_syn_mu": 0.5,
                    "u_syn_sd": 0.02,
                    "depression_time_mu": 671.0,
                    "depression_time_sd": 17.0,
                    "facilitation_time_mu": 17.0,
                    "facilitation_time_sd": 5.0,
                    "conductance_scale_factor": 0.8,
                    "u_hill_coefficient": 1.94,
                },
                {
                    "class": "I2",
                    "conductance_mu": 2.26,
                    "conductance_sd": 0.5,
                    "n_rrp_vesicles_mu": 1.0,
                    "decay_time_mu": 8.3,
                    "decay_time_sd": 2.2,
                    "u_syn_mu": 0.25,
                    "u_syn_sd": 0.13,
                    "depression_time_mu": 706.0,
                    "depression_time_sd": 405.0,
                    "facilitation_time_mu": 21.0,
                    "facilitation_time_sd": 9.0,
                    "conductance_scale_factor": 0.0,
                    "u_hill_coefficient": 1.94,
                },
            ],
        },
    }

    assert df.columns.tolist() == [
        "src_hemisphere_i",
        "src_region_i",
        "src_mtype_i",
        "src_etype_i",
        "dst_hemisphere_i",
        "dst_region_i",
        "dst_mtype_i",
        "dst_etype_i",
        "class",
        "neural_transmitter_release_delay",
        "axonal_conduction_velocity",
    ]

    assert df.src_hemisphere_i.tolist() == [-1] * 4
    assert df.dst_hemisphere_i.tolist() == [-1] * 4
    assert df.src_region_i.tolist() == [-1] * 4
    assert df.dst_region_i.tolist() == [-1] * 4
    assert df.src_mtype_i.tolist() == [-1] * 4
    assert df.dst_mtype_i.tolist() == [-1] * 4
    assert df.dst_etype_i.tolist() == [-1] * 4
    assert df["class"].tolist() == ["E2", "E2_INH", "I2", "I2"]

    npt.assert_allclose(df.neural_transmitter_release_delay, 0.1)
    npt.assert_allclose(df.axonal_conduction_velocity, 300.0)
