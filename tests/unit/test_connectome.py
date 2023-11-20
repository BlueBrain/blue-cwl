from pathlib import Path
from unittest.mock import patch

import pandas as pd
import numpy as np
from pandas import testing as pdt

import libsonata
import pytest
from numpy import testing as npt


from cwl_registry import connectome as test_module
from cwl_registry.utils import load_arrow, write_arrow
from cwl_registry import brain_regions
from cwl_registry.connectome import Constants

DATA_DIR = Path(__file__).parent / "data"


ER = "placeholder__erdos_renyi"
DD = "placeholder__distance_dependent"
L = "left"
R = "right"


@pytest.fixture(scope="module")
def output_dir(tmpdir_factory):
    return tmpdir_factory.mktemp("test_connectome")


def _create_arrow_file(filepath, df):
    write_arrow(filepath=filepath, dataframe=df)
    return filepath


def _build_df(data, columns):
    """Create connectome dataframe."""
    return test_module._conform_types(pd.DataFrame(data, columns=columns))


@pytest.fixture(scope="module")
def macro():
    """Connection strength."""
    # fmt: off
    data = [
        ("LL", "AUDd4"   , "VISrl6a" , 1.0),
        ("LL", "SSp-bfd2", "SSp-bfd2", 1.0),
        ("RR", "MOs2"    , "MOs2"    , 1.0),
        ("LL", "FRP2"    , "FRP2"    , 1.0),
        ("LR", "CA1"     , "CA1"     , 1.0),
        ("RR", "TEa5"    , "TEa5"    , 1.0),
        ("RL", "SSp-bfd3", "SSp-bfd2", 1.0),
    ]
    # fmt: on
    return _build_df(data, Constants.COMPACT_MACRO_LEVELS + Constants.MACRO_VALUES)


@pytest.fixture(scope="module")
def macro_overrides():
    """Connection strength."""
    # fmt: off
    data = [
        ("LL", "AUDd4"   , "VISrl6a" , 2.0),
        ("LL", "SSp-bfd2", "SSp-bfd2", 0.0),
        ("RR", "x"       , "x"       , 0.1),
    ]
    # fmt: on
    return _build_df(data, Constants.COMPACT_MACRO_LEVELS + Constants.MACRO_VALUES)


@pytest.fixture(scope="module")
def macro_assembled():
    # fmt: off
    data = [
        (L, L, "AUDd4"   , "VISrl6a" , 2.0),
        (R, R, "MOs2"    , "MOs2"    , 1.0),
        (L, L, "FRP2"    , "FRP2"    , 1.0),
        (L, R, "CA1"     , "CA1"     , 1.0),
        (R, R, "TEa5"    , "TEa5"    , 1.0),
        (R, L, "SSp-bfd3", "SSp-bfd2", 1.0),
        (R, R, "x"       , "x"       , 0.1),
    ]
    # fmt: on
    return _build_df(data, Constants.MACRO_LEVELS + Constants.MACRO_VALUES)


@pytest.fixture(scope="module")
def macro_assembled_empty():
    # fmt: off
    data = [
        (L, L, "AUDd4"   , "VISrl6a" , 1.0),
        (L, L, "SSp-bfd2", "SSp-bfd2", 1.0),
        (R, R, "MOs2"    , "MOs2"    , 1.0),
        (L, L, "FRP2"    , "FRP2"    , 1.0),
        (L, R, "CA1"     , "CA1"     , 1.0),
        (R, R, "TEa5"    , "TEa5"    , 1.0),
        (R, L, "SSp-bfd3", "SSp-bfd2", 1.0),
    ]
    # fmt: on
    return _build_df(data, Constants.MACRO_LEVELS + Constants.MACRO_VALUES)


@pytest.fixture(scope="module")
def macro_file(output_dir, macro):
    return _create_arrow_file(output_dir / "macro.arrow", macro)


@pytest.fixture(scope="module")
def macro_overrides_file(output_dir, macro_overrides):
    return _create_arrow_file(output_dir / "macro_overrides.arrow", macro_overrides)


@pytest.fixture(scope="module")
def macro_overrides_empty_file():
    return DATA_DIR / "empty_macro_overrides.arrow"


@pytest.fixture(scope="module")
def macro_config(macro_file, macro_overrides_file):
    return {
        "initial": {"connection_strength": macro_file},
        "overrides": {"connection_strength": macro_overrides_file},
    }


@pytest.fixture(scope="module")
def macro_config_empty_overrides(macro_file, macro_overrides_empty_file):
    return {
        "initial": {"connection_strength": macro_file},
        "overrides": {"connection_strength": macro_overrides_empty_file},
    }


def test_assemble_macro_matrix(macro_config, macro_assembled):
    res = test_module.assemble_macro_matrix(macro_config)
    pdt.assert_frame_equal(res, macro_assembled)


def test_assemble_macro_matrix__empty_overrides(
    macro_config_empty_overrides, macro_assembled_empty
):
    res = test_module.assemble_macro_matrix(macro_config_empty_overrides)
    pdt.assert_frame_equal(res, macro_assembled_empty)


@pytest.fixture(scope="module")
def variants():
    # fmt: off
    data = [
        ("LL", "AUDd4"   ,"VISrl6a" ,"L4_UPC"   , "L5_LBC"   , ER ),
        ("LL", "SSp-bfd2","SSp-bfd2","L2_TPC:B" , "L2_TPC:B" , DD ),
        ("RR", "MOs2"    ,"MOs2"    ,"L2_IPC"   , "L23_SBC"  , DD ),
        ("LL", "FRP5"    ,"FRP5"    ,"L5_TPC:C" , "L4_DBC"   , DD ),
        ("LR", "CA1"     ,"CA1"     ,"GEN_mtype", "GIN_mtype", DD ),
        ("RR", "TEa5"    ,"TEa5"    ,"L5_TPC:A" , "L5_TPC:B" , ER ),
    ]

    # fmt: on
    return _build_df(data, Constants.COMPACT_MICRO_LEVELS + ["variant"])


@pytest.fixture(scope="module")
def variants_overrides():
    # fmt: off
    data = [
        ("RL", "SSp-bfd3", "SSp-bfd2", "L3_TPC:A", "L3_TPC:B", DD),
        ("LL", "FRP2"    , "FRP2"    , "L2_IPC"  , "L23_SBC" , "disabled"),
    ]
    # fmt: on
    return _build_df(data, Constants.COMPACT_MICRO_LEVELS + ["variant"])


# @pytest.fixture(scope="module")
# def variants_overrides_empty():
#    return load_arrow(DATA_DIR / "empty_variant_overrides.arrow")


@pytest.fixture(scope="module")
def variants_assembled():
    # fmt: off
    data = [
        (L, L, "AUDd4"   , "VISrl6a"  , "L4_UPC"   , "L5_LBC"   , ER),
        (L, L, "SSp-bfd2", "SSp-bfd2" , "L2_TPC:B" , "L2_TPC:B" , DD),
        (R, R, "MOs2"    , "MOs2"     , "L2_IPC"   , "L23_SBC"  , DD),
        (L, L, "FRP5"    , "FRP5"     , "L5_TPC:C" , "L4_DBC"   , DD),
        (L, R, "CA1"     , "CA1"      , "GEN_mtype", "GIN_mtype", DD),
        (R, R, "TEa5"    , "TEa5"     , "L5_TPC:A" , "L5_TPC:B" , ER),
        (R, L, "SSp-bfd3",  "SSp-bfd2", "L3_TPC:A" , "L3_TPC:B" , DD),
        (L, L, "FRP2"    , "FRP2"     , "L2_IPC"   , "L23_SBC" , "disabled"),
    ]
    # fmt: on
    return _build_df(data, Constants.MICRO_LEVELS + ["variant"])


@pytest.fixture(scope="module")
def variants_assembled_empty_overrides():
    # fmt: off
    data = [
        (L, L, "AUDd4"   , "VISrl6a"  , "L4_UPC"   , "L5_LBC"   , ER),
        (L, L, "SSp-bfd2", "SSp-bfd2" , "L2_TPC:B" , "L2_TPC:B" , DD),
        (R, R, "MOs2"    , "MOs2"     , "L2_IPC"   , "L23_SBC"  , DD),
        (L, L, "FRP5"    , "FRP5"     , "L5_TPC:C" , "L4_DBC"   , DD),
        (L, R, "CA1"     , "CA1"      , "GEN_mtype", "GIN_mtype", DD),
        (R, R, "TEa5"    , "TEa5"     , "L5_TPC:A" , "L5_TPC:B" , ER),
    ]
    # fmt: on
    return _build_df(data, Constants.MICRO_LEVELS + ["variant"])


@pytest.fixture(scope="module")
def variants_conformed():
    """Variants final after conformed to macro final entries.

    Entries that appear in variants but not in macro are removed.
    Entries that appeer in macro but not in variants are added with all combinations of the mtypes
    in that hemisphere/region pathway.
    """
    # fmt: off
    data = [
        (L, L, "AUDd4"   , "VISrl6a"  , "L4_UPC"   , "L5_LBC"   , ER),
        (R, R, "MOs2"    , "MOs2"     , "L2_IPC"   , "L23_SBC"  , DD),
        (L, R, "CA1"     , "CA1"      , "GEN_mtype", "GIN_mtype", DD),
        (R, R, "TEa5"    , "TEa5"     , "L5_TPC:A" , "L5_TPC:B" , ER),
        (R, L, "SSp-bfd3", "SSp-bfd2" , "L3_TPC:A" , "L3_TPC:B" , DD),
    ] + [
        (L, L, "FRP2"    , "FRP2"     , "L23_SBC"  , "L23_SBC"  , ER),
        (L, L, "FRP2"    , "FRP2"     , "L23_SBC"  , "L2_IPC"   , ER),
        (L, L, "FRP2"    , "FRP2"     , "L23_SBC"  , "L3_TPC:A" , ER),
        (L, L, "FRP2"    , "FRP2"     , "L23_SBC"  , "L3_TPC:B" , ER),
        (L, L, "FRP2"    , "FRP2"     , "L2_IPC"   , "L2_IPC"   , ER),
        (L, L, "FRP2"    , "FRP2"     , "L2_IPC"   , "L3_TPC:A" , ER),
        (L, L, "FRP2"    , "FRP2"     , "L2_IPC"   , "L3_TPC:B" , ER),
        (L, L, "FRP2"    , "FRP2"     , "L3_TPC:A" , "L23_SBC"  , ER),
        (L, L, "FRP2"    , "FRP2"     , "L3_TPC:A" , "L2_IPC"   , ER),
        (L, L, "FRP2"    , "FRP2"     , "L3_TPC:A" , "L3_TPC:A" , ER),
        (L, L, "FRP2"    , "FRP2"     , "L3_TPC:A" , "L3_TPC:B" , ER),
        (L, L, "FRP2"    , "FRP2"     , "L3_TPC:B" , "L23_SBC"  , ER),
        (L, L, "FRP2"    , "FRP2"     , "L3_TPC:B" , "L2_IPC"   , ER),
        (L, L, "FRP2"    , "FRP2"     , "L3_TPC:B" , "L3_TPC:A" , ER),
        (L, L, "FRP2"    , "FRP2"     , "L3_TPC:B" , "L3_TPC:B" , ER),
    ] + [
        (R, R, "x"       , "x"        , "GEN_mtype", "GEN_mtype", ER),
        (R, R, "x"       , "x"        , "GEN_mtype", "GIN_mtype", ER),
        (R, R, "x"       , "x"        , "GIN_mtype", "GEN_mtype", ER),
        (R, R, "x"       , "x"        , "GIN_mtype", "GIN_mtype", ER),
    ]

    # fmt: on
    return _build_df(data, Constants.MICRO_LEVELS + ["variant"])


@pytest.fixture(scope="module")
def variants_file(output_dir, variants):
    return _create_arrow_file(output_dir / "variants.arrow", variants)


@pytest.fixture(scope="module")
def variants_overrides_file(output_dir, variants_overrides):
    return _create_arrow_file(output_dir / "variants_overrides.arrow", variants_overrides)


@pytest.fixture(scope="module")
def variants_overrides_empty_file():
    return DATA_DIR / "empty_variant_overrides.arrow"


def test_assemble__variants(variants_file, variants_overrides_file, variants_assembled):
    res = test_module._assemble(
        variants_file,
        Constants.MICRO_LEVELS,
        Constants.MICRO_VARIANT_VALUES,
        variants_overrides_file,
    )
    pdt.assert_frame_equal(res, variants_assembled)


def test_assemble__variants_empty(
    variants_file, variants_overrides_empty_file, variants_assembled_empty_overrides
):
    return
    res = test_module._assemble(
        variants_file,
        Constants.MICRO_LEVELS,
        Constants.MICRO_VARIANT_VALUES,
        variants_overrides_empty_file,
    )
    pdt.assert_frame_equal(res, variants_assembled_empty_overrides)


@pytest.fixture
def hrm_cell_counts():
    return pd.read_parquet(DATA_DIR / "hrm_cell_counts.parquet")["count"]


def test_conform__variants(
    macro_assembled, variants_assembled, variants_conformed, hrm_cell_counts
):
    res = test_module._conform_variants(
        variants=variants_assembled,
        macro=macro_assembled,
        available_pathways=hrm_cell_counts.index.to_frame(index=False),
    )

    pdt.assert_frame_equal(res, variants_conformed)


@pytest.fixture(scope="module")
def micro_er():
    # fmt: off
    data = [
        ("RR", "ENTm6", "VISal5" , "GEN_mtype", "L5_SBC", 0.2, 1.0, 0.1, 100.0, 5.0 ),
        ("LL", "AUDd4", "VISrl6a", "L4_UPC"   , "L5_LBC", 0.3, 2.0, 0.2, 150.0, 10.0),
    ]
    # fmt: on
    return _build_df(data, Constants.COMPACT_MICRO_LEVELS + Constants.MICRO_ER_VALUES)


@pytest.fixture(scope="module")
def micro_er_overrides():
    # fmt: off
    data = [
        ("LL", "AUDd4", "VISrl6a", "L4_UPC", "L5_LBC", 1.2, 2.2, 0.1, 150.0, 11.0),
    ]
    # fmt: on
    return _build_df(data, Constants.COMPACT_MICRO_LEVELS + Constants.MICRO_ER_VALUES)


@pytest.fixture(scope="module")
def micro_er_assembled():
    # fmt: off
    data = [
        (R, R, "ENTm6", "VISal5" , "GEN_mtype", "L5_SBC", 0.2, 1.0, 0.1, 100.0, 5.0 ),
        (L, L, "AUDd4", "VISrl6a", "L4_UPC"   , "L5_LBC", 1.2, 2.2, 0.1, 150.0, 11.0),
    ]
    # fmt: on
    return _build_df(data, Constants.MICRO_LEVELS + Constants.MICRO_ER_VALUES)


@pytest.fixture(scope="module")
def micro_er_assembled_empty_overrides():
    # fmt: off
    data = [
        (R, R, "ENTm6", "VISal5" , "GEN_mtype", "L5_SBC", 0.2, 1.0, 0.1, 100.0, 5.0 ),
        (L, L, "AUDd4", "VISrl6a", "L4_UPC"   , "L5_LBC", 0.3, 2.0, 0.2, 150.0, 10.0),
    ]
    # fmt: on
    return _build_df(data, Constants.MICRO_LEVELS + Constants.MICRO_ER_VALUES)


@pytest.fixture(scope="module")
def micro_er_conformed():
    # fmt: off
    data = [
        (L, L, "AUDd4", "VISrl6a", "L4_UPC"   , "L5_LBC"   , 1.2, 2.2 , 0.1, 150.0, 11.0),
        (R, R, "TEa5" , "TEa5"   , "L5_TPC:A" , "L5_TPC:B" , 2.0, 10.0, 1.0, 50.0 , 1.0 ),
    ] + [
        (L, L, "FRP2" , "FRP2"   , "L23_SBC"  , "L23_SBC"  , 2.0, 10.0, 1.0, 50.0 , 1.0),
        (L, L, "FRP2" , "FRP2"   , "L23_SBC"  , "L2_IPC"   , 2.0, 10.0, 1.0, 50.0 , 1.0),
        (L, L, "FRP2" , "FRP2"   , "L23_SBC"  , "L3_TPC:A" , 2.0, 10.0, 1.0, 50.0 , 1.0),
        (L, L, "FRP2" , "FRP2"   , "L23_SBC"  , "L3_TPC:B" , 2.0, 10.0, 1.0, 50.0 , 1.0),
        (L, L, "FRP2" , "FRP2"   , "L2_IPC"   , "L2_IPC"   , 2.0, 10.0, 1.0, 50.0 , 1.0),
        (L, L, "FRP2" , "FRP2"   , "L2_IPC"   , "L3_TPC:A" , 2.0, 10.0, 1.0, 50.0 , 1.0),
        (L, L, "FRP2" , "FRP2"   , "L2_IPC"   , "L3_TPC:B" , 2.0, 10.0, 1.0, 50.0 , 1.0),
        (L, L, "FRP2" , "FRP2"   , "L3_TPC:A" , "L23_SBC"  , 2.0, 10.0, 1.0, 50.0 , 1.0),
        (L, L, "FRP2" , "FRP2"   , "L3_TPC:A" , "L2_IPC"   , 2.0, 10.0, 1.0, 50.0 , 1.0),
        (L, L, "FRP2" , "FRP2"   , "L3_TPC:A" , "L3_TPC:A" , 2.0, 10.0, 1.0, 50.0 , 1.0),
        (L, L, "FRP2" , "FRP2"   , "L3_TPC:A" , "L3_TPC:B" , 2.0, 10.0, 1.0, 50.0 , 1.0),
        (L, L, "FRP2" , "FRP2"   , "L3_TPC:B" , "L23_SBC"  , 2.0, 10.0, 1.0, 50.0 , 1.0),
        (L, L, "FRP2" , "FRP2"   , "L3_TPC:B" , "L2_IPC"   , 2.0, 10.0, 1.0, 50.0 , 1.0),
        (L, L, "FRP2" , "FRP2"   , "L3_TPC:B" , "L3_TPC:A" , 2.0, 10.0, 1.0, 50.0 , 1.0),
        (L, L, "FRP2" , "FRP2"   , "L3_TPC:B" , "L3_TPC:B" , 2.0, 10.0, 1.0, 50.0 , 1.0),
    ] + [
        (R, R, "x"    , "x"      , "GEN_mtype", "GEN_mtype", 2.0, 10.0, 1.0, 50.0 , 1.0),
        (R, R, "x"    , "x"      , "GEN_mtype", "GIN_mtype", 2.0, 10.0, 1.0, 50.0 , 1.0),
        (R, R, "x"    , "x"      , "GIN_mtype", "GEN_mtype", 2.0, 10.0, 1.0, 50.0 , 1.0),
        (R, R, "x"    , "x"      , "GIN_mtype", "GIN_mtype", 2.0, 10.0, 1.0, 50.0 , 1.0),
    ]
    # fmt: on
    return _build_df(data, Constants.MICRO_LEVELS + Constants.MICRO_ER_VALUES)


@pytest.fixture(scope="module")
def micro_er_defaults():
    return {
        "weight": 2.0,
        "nsynconn_mean": 10.0,
        "nsynconn_std": 1.0,
        "delay_velocity": 50.0,
        "delay_offset": 1.0,
    }


@pytest.fixture(scope="module")
def micro_er_file(output_dir, micro_er):
    return _create_arrow_file(output_dir / "micro_er.arrow", micro_er)


@pytest.fixture(scope="module")
def micro_er_overrides_file(output_dir, micro_er_overrides):
    return _create_arrow_file(output_dir / "micro_er_overrides.arrow", micro_er_overrides)


@pytest.fixture(scope="module")
def micro_er_overrides_empty_file():
    return DATA_DIR / "empty_er_overrides.arrow"


def test_assemble__er(micro_er_file, micro_er_overrides_file, micro_er_assembled):
    res = test_module._assemble(
        initial_path=micro_er_file,
        index_columns=Constants.MICRO_LEVELS,
        value_columns=Constants.MICRO_ER_VALUES,
        overrides_path=micro_er_overrides_file,
    )
    pdt.assert_frame_equal(res, micro_er_assembled)


def test_assemble__er__empty_overrides(
    micro_er_file, micro_er_overrides_empty_file, micro_er_assembled_empty_overrides
):
    res = test_module._assemble(
        initial_path=micro_er_file,
        index_columns=Constants.MICRO_LEVELS,
        value_columns=Constants.MICRO_ER_VALUES,
        overrides_path=micro_er_overrides_empty_file,
    )
    pdt.assert_frame_equal(res, micro_er_assembled_empty_overrides)


def test_conform__er(variants_conformed, micro_er_defaults, micro_er_assembled, micro_er_conformed):
    mask = variants_conformed["variant"] == ER
    target_pathways = variants_conformed[mask].drop(columns="variant")

    res = test_module._conform(
        parameters=micro_er_assembled,
        to=target_pathways,
        with_defaults=micro_er_defaults,
    )
    pdt.assert_frame_equal(res, micro_er_conformed)


@pytest.fixture(scope="module")
def micro_dd():
    # fmt: off
    data = [
        ("LL", "SSp-bfd2", "SSp-bfd2", "L2_TPC:B", "L2_TPC:B", 1.0, 0.008, 3.0, 1.5, 250.0, 0.8),
        ("RR", "MOs2"    , "MOs2"    , "L2_IPC"  , "L23_SBC" , 1.0, 0.008, 3.0, 1.5, 250.0, 0.8),
        ("LL", "FRP5"    , "FRP5"    , "L5_TPC:C", "L4_DBC"  , 1.0, 0.008, 3.0, 1.5, 250.0, 0.8),
    ]
    # fmt: on
    return _build_df(data, Constants.COMPACT_MICRO_LEVELS + Constants.MICRO_DD_VALUES)


@pytest.fixture(scope="module")
def micro_dd_overrides():
    data = [
        ("LL", "SSp-bfd2", "SSp-bfd2", "L2_TPC:B", "L2_TPC:B", 1.5, 0.002, 3.0, 1.5, 250.0, 0.9),
        ("RL", "SSp-bfd3", "SSp-bfd2", "L3_TPC:A", "L3_TPC:B", 0.5, 0.005, 2.0, 0.1, 100.0, 0.5),
    ]
    return _build_df(data, Constants.COMPACT_MICRO_LEVELS + Constants.MICRO_DD_VALUES)


@pytest.fixture(scope="module")
def micro_dd_assembled():
    # fmt: off
    data = [
        (L, L, "SSp-bfd2", "SSp-bfd2", "L2_TPC:B", "L2_TPC:B", 1.5, 0.002, 3.0, 1.5, 250.0, 0.9),
        (R, R, "MOs2"    , "MOs2"    , "L2_IPC"  , "L23_SBC" , 1.0, 0.008, 3.0, 1.5, 250.0, 0.8),
        (L, L, "FRP5"    , "FRP5"    , "L5_TPC:C", "L4_DBC"  , 1.0, 0.008, 3.0, 1.5, 250.0, 0.8),
        (R, L, "SSp-bfd3", "SSp-bfd2", "L3_TPC:A", "L3_TPC:B", 0.5, 0.005, 2.0, 0.1, 100.0, 0.5)
    ]
    # fmt: on
    return _build_df(data, Constants.MICRO_LEVELS + Constants.MICRO_DD_VALUES)


@pytest.fixture(scope="module")
def micro_dd_assembled_empty_overrides():
    # fmt: off
    data = [
        (L, L, "SSp-bfd2", "SSp-bfd2", "L2_TPC:B", "L2_TPC:B", 1.0, 0.008, 3.0, 1.5, 250.0, 0.8),
        (R, R, "MOs2"    , "MOs2"    , "L2_IPC"  , "L23_SBC" , 1.0, 0.008, 3.0, 1.5, 250.0, 0.8),
        (L, L, "FRP5"    , "FRP5"    , "L5_TPC:C", "L4_DBC"  , 1.0, 0.008, 3.0, 1.5, 250.0, 0.8),
    ]
    # fmt: on
    return _build_df(data, Constants.MICRO_LEVELS + Constants.MICRO_DD_VALUES)


@pytest.fixture(scope="module")
def micro_dd_conformed():
    """Assembled micro dd parameters conformed to variant matrix."""
    # fmt: off
    data = [
        (R, R, "MOs2"    , "MOs2"    , "L2_IPC"   , "L23_SBC"  , 1.0, 0.008, 3.0 , 1.5, 250.0, 0.8),
        (L, R, "CA1"     , "CA1"     , "GEN_mtype", "GIN_mtype", 2.0, 0.001, 10.0, 1.0, 50.0 , 1.0),
        (R, L, "SSp-bfd3", "SSp-bfd2", "L3_TPC:A" , "L3_TPC:B" , 0.5, 0.005, 2.0 , 0.1, 100.0, 0.5),
    ]
    # fmt: on
    return _build_df(data, Constants.MICRO_LEVELS + Constants.MICRO_DD_VALUES)


@pytest.fixture(scope="module")
def micro_dd_defaults():
    return {
        "weight": 2.0,
        "exponent": 0.001,
        "nsynconn_mean": 10.0,
        "nsynconn_std": 1.0,
        "delay_velocity": 50.0,
        "delay_offset": 1.0,
    }


@pytest.fixture(scope="module")
def micro_dd_file(output_dir, micro_dd):
    return _create_arrow_file(output_dir / "micro_dd.arrow", micro_dd)


@pytest.fixture(scope="module")
def micro_dd_overrides_file(micro_dd_overrides, output_dir):
    return _create_arrow_file(output_dir / "micro_dd_overrides.arrow", micro_dd_overrides)


@pytest.fixture(scope="module")
def micro_dd_overrides_empty_file():
    return DATA_DIR / "empty_dd_overrides.arrow"


def test_assemble__dd(micro_dd_file, micro_dd_overrides_file, micro_dd_assembled):
    res = test_module._assemble(
        initial_path=micro_dd_file,
        index_columns=Constants.MICRO_LEVELS,
        value_columns=Constants.MICRO_DD_VALUES,
        overrides_path=micro_dd_overrides_file,
    )
    pdt.assert_frame_equal(res, micro_dd_assembled)


def test_assemble__dd__empty_overrides(
    micro_dd_file, micro_dd_overrides_empty_file, micro_dd_assembled_empty_overrides
):
    res = test_module._assemble(
        initial_path=micro_dd_file,
        index_columns=Constants.MICRO_LEVELS,
        value_columns=Constants.MICRO_DD_VALUES,
        overrides_path=micro_dd_overrides_empty_file,
    )
    pdt.assert_frame_equal(res, micro_dd_assembled_empty_overrides)


def test_conform__dd(variants_conformed, micro_dd_defaults, micro_dd_assembled, micro_dd_conformed):
    mask = variants_conformed["variant"] == DD
    target_dd_pathways = variants_conformed[mask].drop(columns="variant")

    res = test_module._conform(
        parameters=micro_dd_assembled,
        to=target_dd_pathways,
        with_defaults=micro_dd_defaults,
    )
    pdt.assert_frame_equal(res, micro_dd_conformed)


@pytest.fixture(scope="module")
def micro_config(
    variants_file,
    variants_overrides_file,
    micro_dd_file,
    micro_dd_overrides_file,
    micro_dd_defaults,
    micro_er_file,
    micro_er_overrides_file,
    micro_er_defaults,
):
    config_er_defaults = {name: {"default": value} for name, value in micro_er_defaults.items()}
    config_dd_defaults = {name: {"default": value} for name, value in micro_dd_defaults.items()}

    return {
        "variants": {
            ER: {
                "params": config_er_defaults,
            },
            DD: {"params": config_dd_defaults},
        },
        "initial": {
            "variants": variants_file,
            ER: micro_er_file,
            DD: micro_dd_file,
        },
        "overrides": {
            "variants": variants_overrides_file,
            ER: micro_er_overrides_file,
            DD: micro_dd_overrides_file,
        },
    }


def test_assemble__no_overrides(micro_dd, micro_dd_file):
    res = test_module._assemble(
        micro_dd_file,
        index_columns=Constants.MICRO_LEVELS,
        value_columns=Constants.MICRO_DD_VALUES,
        overrides_path=None,
    )
    pdt.assert_frame_equal(res, test_module._split_side_into_hemispheres(micro_dd))


def test_resolve_micro_matrix__er(
    micro_config, macro_assembled, variants_conformed, hrm_cell_counts, region_volumes
):
    res = test_module._resolve_micro_matrix(
        micro_config=micro_config,
        variant_name="placeholder__erdos_renyi",
        macro_matrix=macro_assembled,
        variants_matrix=variants_conformed,
        population=None,
        cell_counts=hrm_cell_counts,
        region_volumes=region_volumes,
    )

    assert sorted(res.columns) == [
        "delay_offset",
        "delay_velocity",
        "nsynconn_mean",
        "nsynconn_std",
        "pconn",
        "source_hemisphere",
        "source_mtype",
        "source_region",
        "target_hemisphere",
        "target_mtype",
        "target_region",
    ]

    assert isinstance(res.index, pd.RangeIndex)


@pytest.fixture
def hrm_cell_positions():
    return pd.read_parquet(DATA_DIR / "hrm_cell_positions.parquet")


def test_resolve_micro_matrix__dd(
    micro_config,
    macro_assembled,
    variants_conformed,
    hrm_cell_counts,
    region_volumes,
    hrm_cell_positions,
):
    with patch("cwl_registry.population_utils.get_HRM_positions", return_value=hrm_cell_positions):
        res = test_module._resolve_micro_matrix(
            micro_config=micro_config,
            variant_name="placeholder__distance_dependent",
            macro_matrix=macro_assembled,
            variants_matrix=variants_conformed,
            population=None,
            cell_counts=hrm_cell_counts,
            region_volumes=region_volumes,
        )

    assert sorted(res.columns) == [
        "delay_offset",
        "delay_velocity",
        "exponent",
        "nsynconn_mean",
        "nsynconn_std",
        "scale",
        "source_hemisphere",
        "source_mtype",
        "source_region",
        "target_hemisphere",
        "target_mtype",
        "target_region",
    ]


def test_resolve_micro_matrices(
    micro_config,
    macro_assembled,
    micro_er_conformed,
    micro_dd_conformed,
    hrm_cell_counts,
    hrm_cell_positions,
    region_volumes,
):
    with patch("cwl_registry.population_utils.get_HRM_positions", return_value=hrm_cell_positions):
        with patch("cwl_registry.population_utils.get_HRM_counts", return_value=hrm_cell_counts):
            res = test_module.resolve_micro_matrices(
                micro_config, macro_assembled, None, region_volumes
            )

    assert res.keys() == {ER, DD}

    pdt.assert_frame_equal(res[ER].drop(columns="pconn"), micro_er_conformed.drop(columns="weight"))
    pdt.assert_frame_equal(res[DD].drop(columns="scale"), micro_dd_conformed.drop(columns="weight"))


def test_macro_synapse_counts(macro_assembled, region_volumes):
    res = test_module._macro_synapse_counts(macro_assembled, region_volumes)

    assert res.dtype == int

    assert res.index.equals(res.index)
    expected = [
        1.513750e08,
        7.164219e08,
        4.667188e07,
        5.104617e09,
        4.756094e08,
        2.673828e08,
        2.848438e06,
    ]
    npt.assert_allclose(res.values, expected, rtol=1e-6)

    macro = macro_assembled.set_index(Constants.MACRO_LEVELS)

    res = test_module._macro_synapse_counts(macro, region_volumes)

    assert res.index.equals(res.index)
    expected = [
        1.513750e08,
        7.164219e08,
        4.667188e07,
        5.104617e09,
        4.756094e08,
        2.673828e08,
        2.848438e06,
    ]
    npt.assert_allclose(res.values, expected, rtol=1e-6)


from cwl_registry.utils import write_parquet


def test_pre_post_cell_counts(micro_er_conformed, hrm_cell_counts):
    micro = micro_er_conformed

    res = test_module._pre_post_cell_counts(micro, hrm_cell_counts)

    assert res[0].dtype == int
    assert res[1].dtype == int

    npt.assert_array_equal(
        res[0],
        [
            1125,
            11059,
            143,
            143,
            143,
            143,
            62,
            62,
            62,
            17,
            17,
            17,
            17,
            2,
            2,
            2,
            2,
            621,
            621,
            293,
            293,
        ],
    )
    npt.assert_array_equal(
        res[1],
        [
            17,
            8916,
            143,
            62,
            17,
            2,
            62,
            17,
            2,
            143,
            62,
            17,
            2,
            143,
            62,
            17,
            2,
            621,
            293,
            621,
            293,
        ],
    )


def test_micro_synapse_counts(micro_er_conformed, macro_assembled, hrm_cell_counts):
    macro = macro_assembled
    micro = micro_er_conformed

    macro = test_module._align(macro, micro, Constants.MACRO_LEVELS).set_index(
        Constants.MACRO_LEVELS
    )

    res = test_module._micro_synapse_counts(micro, macro, hrm_cell_counts)

    assert res.dtype == int

    assert res.index.equals(macro.index)

    assert sum(res) == 1989625490


def test_probability_of_connection(
    micro_er_conformed, macro_assembled, hrm_cell_counts, region_volumes
):
    res = test_module._probability_of_connection(
        micro_er_conformed, macro_assembled, hrm_cell_counts, region_volumes
    )

    assert len(res) == 21


@pytest.fixture
def distance_bins():
    return np.linspace(0, 1000, 101, dtype=float)


@pytest.fixture
def distance_bin_centers(distance_bins):
    return distance_bins[:-1] + 0.5 * np.diff(distance_bins[[0, 1]])


def test_estimate_dd_scale__empty_histogram(distance_bins):
    dist_hist = np.zeros(len(distance_bins) - 1)

    nconns = np.array([100.0])
    exponents = np.array([0.008])

    res = test_module._estimate_dd_scale(nconns, dist_hist, distance_bins, exponents)

    assert len(res) == 1
    assert np.isinf(res).all()


def test_estimate_dd_scale__zero_nconn(distance_bins):
    dist_hist = np.full(len(distance_bins) - 1, 100.0)

    nconns = np.array([0.0])
    exponents = np.array([0.008])

    res = test_module._estimate_dd_scale(nconns, dist_hist, distance_bins, exponents)

    assert len(res) == 1
    assert np.isclose(res, 0.0), res


def test_estimate_dd_scale__linear(distance_bins):
    dist_hist = np.linspace(100, 1, len(distance_bins) - 1)

    nconns = np.array([10.0])
    exponents = np.array([0.008])

    res = test_module._estimate_dd_scale(nconns, dist_hist, distance_bins, exponents)

    assert len(res) == 1
    assert np.isclose(res, 0.00909360614)


def test_estimate_dd_scale__compare_non_vectorized():
    dist_hists = np.array(
        [
            [6, 9, 3, 9, 4, 4, 9],
            [2, 9, 5, 1, 2, 5, 6],
        ]
    )
    dist_bins = np.array([0.0, 100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0])
    bin_centers = np.array([50.0, 150.0, 250.0, 350.0, 450.0, 550.0, 650.0])

    nconns = np.array([10.0, 12.0])
    exponents = np.array([0.002, 0.003])

    res = test_module._estimate_dd_scale(nconns, dist_hists, dist_bins, exponents)

    expected = [
        _estimate_DD_scale(nconns[i], dist_hists[i], dist_bins, exponents[i]) for i in range(2)
    ]

    assert list(res) == expected


def _estimate_DD_scale(N_conn, dist_hist, dist_bins, DD_exp):
    """Estimate DD scale based on number of connections to be geneated, DD exponent, and distance histogram."""
    DD_fct = lambda d, scale, exponent: scale * np.exp(
        -exponent * d
    )  # Exponential DD probability function
    bin_centers = np.array([np.mean(dist_bins[i : i + 2]) for i in range(len(dist_bins) - 1)])
    ref_scale = 1.0
    p_vals = DD_fct(bin_centers, ref_scale, DD_exp)
    N_conn_ref = np.sum(p_vals * dist_hist)
    DD_scale = ref_scale * N_conn / N_conn_ref

    return DD_scale


'''

def test_micro_synapse_counts(micro_er_conformed, macro_assembled, hrm_counts):

    res = test_module._micro_synapse_counts(micro_er_conformed, macro, hrm_counts)

    breakpoint()
    print()

"""
@pytest.fixture
def synapse_counts(micro_er_conformed, macro_assembled, brain_region_volumes):
    macro_cols = ["side", "source_region", "target_region"]
    macro = test_module._align(macro_assembled, micro_er_conformed, macro_cols)
    return test_module.synapse_counts_per_pathway(macro, brain_region_volumes)
"""


def test_probability_of_connection(
    micro_er_conformed, macro_assembled, hrm_counts, brain_region_volumes
):
    test_module.probability_of_connection(
        micro_er_conformed, macro_assembled, hrm_counts, brain_region_volumes
    )
'''
