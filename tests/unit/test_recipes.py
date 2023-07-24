import pytest
import libsonata
from pathlib import Path
import pandas as pd
from pandas import testing as pdt
from cwl_registry import recipes as tested
from cwl_registry.utils import load_json


DATA_DIR = Path(__file__).parent / "data"


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
def pathways():
    data = [
        (
            "left",
            "left",
            "SSp-bfd2",
            "SSp-bfd3",
            "L5_TPC:A",
            "L5_TPC:B",
            "dSTUT",
            "cNAc",
            "EXC",
            "EXC",
        ),
        ("left", "right", "CA1", "CA1", "GIN_mtype", "GIN_mtype", "cAc", "cAc", "INH", "INH"),
    ]
    return pd.DataFrame(
        data,
        columns=[
            "source_hemisphere",
            "target_hemisphere",
            "source_region",
            "target_region",
            "source_mtype",
            "target_mtype",
            "source_etype",
            "target_etype",
            "source_synapse_class",
            "target_synapse_class",
        ],
    )


def test_build_tailored_properties(synaptic_assignment, region_map, annotation, pathways):
    res = list(
        tested._generate_tailored_properties(synaptic_assignment, region_map, annotation, pathways)
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


def test_write_xml_tree(small_synapse_properties, synaptic_classification, tmp_path):
    output_file = tmp_path / "recipe.xml"

    tested._write_xml_tree(small_synapse_properties, synaptic_classification, output_file)

    df = pd.DataFrame.from_dict(small_synapse_properties).rename(columns={"synapticType": "type"})

    res_properties = pd.read_xml(output_file, xpath="./SynapsesProperties/synapse")

    pdt.assert_frame_equal(res_properties, df[res_properties.columns])

    res_classes = pd.read_xml(output_file, xpath="./SynapsesClassification/class")

    assert set(res_classes.id) == {"E2", "I2", "E2_INH", "E2_L23PC"}
