from pathlib import Path

import pandas as pd
import numpy as np
from pandas import testing as pdt

import pytest
from numpy import testing as npt


from cwl_registry import connectome as test_module
from cwl_registry.utils import load_arrow, write_arrow


DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture(scope="module")
def output_dir(tmpdir_factory):
    return tmpdir_factory.mktemp("test_connectome")


def _create_arrow_file(filepath, df):
    write_arrow(filepath=filepath, dataframe=df)
    return filepath


@pytest.fixture(scope="module")
def macro():
    """Connection strength."""
    df = pd.DataFrame(
        {
            "side": ["LL", "LL", "RR", "LL", "LR", "RR", "RL"],
            "source_region": ["AUDd4", "SSp-bfd2", "MOs2", "FRP2", "new", "new", "SSp-bfd3"],
            "target_region": ["VISrl6a", "SSp-bfd2", "MOs2", "FRP2", "new", "new", "SSp-bfd2"],
            "value": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        }
    )
    return test_module._conform_types(df)


@pytest.fixture(scope="module")
def macro_overrides():
    """Connection strength."""
    df = pd.DataFrame(
        {
            "side": ["LL", "LL", "RR"],
            "source_region": ["AUDd4", "SSp-bfd2", "x"],
            "target_region": ["VISrl6a", "SSp-bfd2", "x"],
            "value": [2.0, 0.0, 0.1],
        }
    )
    return test_module._conform_types(df)


@pytest.fixture(scope="module")
def macro_assembled():
    df = pd.DataFrame(
        {
            "side": ["LL", "RR", "LL", "LR", "RR", "RL", "RR"],
            "source_region": ["AUDd4", "MOs2", "FRP2", "new", "new", "SSp-bfd3", "x"],
            "target_region": ["VISrl6a", "MOs2", "FRP2", "new", "new", "SSp-bfd2", "x"],
            "value": [2.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.1],
        }
    )
    return test_module._conform_types(df)


@pytest.fixture(scope="module")
def macro_file(output_dir, macro):
    return _create_arrow_file(output_dir / "macro.arrow", macro)


@pytest.fixture(scope="module")
def macro_overrides_file(output_dir, macro_overrides):
    return _create_arrow_file(output_dir / "macro_overrides.arrow", macro_overrides)


@pytest.fixture(scope="module")
def macro_config(macro_file, macro_overrides_file):
    return {
        "initial": {
            "connection_strength": macro_file,
        },
        "overrides": {
            "connection_strength": macro_overrides_file,
        },
    }


def test_assemble_macro_matrix(macro_config, macro_assembled):
    res = test_module.assemble_macro_matrix(macro_config)
    pdt.assert_frame_equal(res, macro_assembled)


@pytest.fixture(scope="module")
def variants():
    variants = [
        "placeholder__erdos_renyi",
        "placeholder__distance_dependent",
        "placeholder__distance_dependent",
        "placeholder__distance_dependent",
        "placeholder__distance_dependent",
        "placeholder__erdos_renyi",
    ]
    df = pd.DataFrame(
        {
            "side": ["LL", "LL", "RR", "LL", "LR", "RR"],
            "source_region": ["AUDd4", "SSp-bfd2", "MOs2", "FRP5", "new", "new"],
            "target_region": ["VISrl6a", "SSp-bfd2", "MOs2", "FRP5", "new", "new"],
            "source_mtype": ["L4_UPC", "L2_TPC:B", "L2_IPC", "L5_TPC:C", "new", "new"],
            "target_mtype": ["L5_LBC", "L2_TPC:B", "L23_SBC", "L4_DBC", "new", "new"],
            "variant": variants,
        }
    )
    return test_module._conform_types(df)


@pytest.fixture(scope="module")
def variants_overrides():
    variants = [
        "placeholder__distance_dependent",
    ]
    df = pd.DataFrame(
        {
            "side": ["RL"],
            "source_region": ["SSp-bfd3"],
            "target_region": ["SSp-bfd2"],
            "source_mtype": ["L3_TPC:A"],
            "target_mtype": ["L3_TPC:B"],
            "variant": variants,
        }
    )
    return test_module._conform_types(df)


@pytest.fixture(scope="module")
def variants_assembled():
    variants = [
        "placeholder__erdos_renyi",
        "placeholder__distance_dependent",
        "placeholder__distance_dependent",
        "placeholder__distance_dependent",
        "placeholder__distance_dependent",
        "placeholder__erdos_renyi",
        "placeholder__distance_dependent",
    ]
    df = pd.DataFrame(
        {
            "side": ["LL", "LL", "RR", "LL", "LR", "RR", "RL"],
            "source_region": ["AUDd4", "SSp-bfd2", "MOs2", "FRP5", "new", "new", "SSp-bfd3"],
            "target_region": ["VISrl6a", "SSp-bfd2", "MOs2", "FRP5", "new", "new", "SSp-bfd2"],
            "source_mtype": ["L4_UPC", "L2_TPC:B", "L2_IPC", "L5_TPC:C", "new", "new", "L3_TPC:A"],
            "target_mtype": ["L5_LBC", "L2_TPC:B", "L23_SBC", "L4_DBC", "new", "new", "L3_TPC:B"],
            "variant": variants,
        }
    )
    return test_module._conform_types(df)


@pytest.fixture(scope="module")
def variants_conformed():
    """Variants final after conformed to macro final entries."""
    variants = [
        "placeholder__erdos_renyi",
        "placeholder__distance_dependent",
        "placeholder__erdos_renyi",
        "placeholder__distance_dependent",
        "placeholder__erdos_renyi",
        "placeholder__distance_dependent",
        "placeholder__erdos_renyi",
    ]
    df = pd.DataFrame(
        {
            "side": ["LL", "RR", "LL", "LR", "RR", "RL", "RR"],
            "source_region": ["AUDd4", "MOs2", "FRP2", "new", "new", "SSp-bfd3", "x"],
            "target_region": ["VISrl6a", "MOs2", "FRP2", "new", "new", "SSp-bfd2", "x"],
            "source_mtype": [
                "L4_UPC",
                "L2_IPC",
                "GEN_mtype",
                "new",
                "new",
                "L3_TPC:A",
                "GEN_mtype",
            ],
            "target_mtype": [
                "L5_LBC",
                "L23_SBC",
                "GEN_mtype",
                "new",
                "new",
                "L3_TPC:B",
                "GEN_mtype",
            ],
            "variant": variants,
        }
    )
    return test_module._conform_types(df)


@pytest.fixture(scope="module")
def variants_file(output_dir, variants):
    return _create_arrow_file(output_dir / "variants.arrow", variants)


@pytest.fixture(scope="module")
def variants_overrides_file(output_dir, variants_overrides):
    return _create_arrow_file(output_dir / "variants_overrides.arrow", variants_overrides)


def test_assemble__variants(variants_file, variants_overrides_file, variants_assembled):
    res = test_module._assemble(variants_file, test_module.MICRO_INDEX, variants_overrides_file)
    pdt.assert_frame_equal(res, variants_assembled)


def test_conform__variants(macro_assembled, variants_assembled, variants_conformed):
    res = test_module._conform(
        parameters=variants_assembled,
        to=macro_assembled[test_module.MACRO_INDEX],
        with_defaults={
            "variant": "placeholder__erdos_renyi",
            "source_mtype": "GEN_mtype",
            "target_mtype": "GEN_mtype",
        },
    )
    pdt.assert_frame_equal(res, variants_conformed)


@pytest.fixture(scope="module")
def micro_er():
    df = pd.DataFrame(
        {
            "side": ["RR", "LL"],
            "source_region": ["ENTm6", "AUDd4"],
            "target_region": ["VISal5", "VISrl6a"],
            "source_mtype": ["GEN_mtype", "L4_UPC"],
            "target_mtype": ["L5_SBC", "L5_LBC"],
            "weight": [0.2, 0.3],
            "nsynconn_mean": [1.0, 2.0],
            "nsynconn_std": [0.1, 0.2],
            "delay_velocity": [100.0, 150.0],
            "delay_offset": [5.0, 10.0],
        }
    )
    return test_module._conform_types(df)


@pytest.fixture(scope="module")
def micro_er_overrides():
    df = pd.DataFrame(
        {
            "side": ["LL"],
            "source_region": ["AUDd4"],
            "target_region": ["VISrl6a"],
            "source_mtype": ["L4_UPC"],
            "target_mtype": ["L5_LBC"],
            "weight": [1.2],
            "nsynconn_mean": [2.2],
            "nsynconn_std": [0.1],
            "delay_velocity": [150.0],
            "delay_offset": [11.0],
        }
    )
    return test_module._conform_types(df)


@pytest.fixture(scope="module")
def micro_er_assembled():
    df = pd.DataFrame(
        {
            "side": ["RR", "LL"],
            "source_region": ["ENTm6", "AUDd4"],
            "target_region": ["VISal5", "VISrl6a"],
            "source_mtype": ["GEN_mtype", "L4_UPC"],
            "target_mtype": ["L5_SBC", "L5_LBC"],
            "weight": [0.2, 1.2],
            "nsynconn_mean": [1.0, 2.2],
            "nsynconn_std": [0.1, 0.1],
            "delay_velocity": [100.0, 150.0],
            "delay_offset": [5.0, 11.0],
        }
    )
    return test_module._conform_types(df)


@pytest.fixture(scope="module")
def micro_er_conformed():
    df = pd.DataFrame(
        {
            "side": ["LL", "LL", "RR", "RR"],
            "source_region": ["AUDd4", "FRP2", "new", "x"],
            "target_region": ["VISrl6a", "FRP2", "new", "x"],
            "source_mtype": ["L4_UPC", "GEN_mtype", "new", "GEN_mtype"],
            "target_mtype": ["L5_LBC", "GEN_mtype", "new", "GEN_mtype"],
            "weight": [1.2, 2.0, 2.0, 2.0],
            "nsynconn_mean": [2.2, 10.0, 10.0, 10.0],
            "nsynconn_std": [0.1, 1.0, 1.0, 1.0],
            "delay_velocity": [150.0, 50.0, 50.0, 50.0],
            "delay_offset": [11.0, 1.0, 1.0, 1.0],
        }
    )
    return test_module._conform_types(df)


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


def test_assemble__er(micro_er_file, micro_er_overrides_file, micro_er_assembled):
    res = test_module._assemble(
        initial_path=micro_er_file,
        index_columns=test_module.MICRO_INDEX,
        overrides_path=micro_er_overrides_file,
    )
    pdt.assert_frame_equal(res, micro_er_assembled)


def test_conform__er(variants_conformed, micro_er_defaults, micro_er_assembled, micro_er_conformed):
    mask = variants_conformed["variant"] == "placeholder__erdos_renyi"
    target_pathways = variants_conformed[mask].drop(columns="variant")

    res = test_module._conform(
        parameters=micro_er_assembled,
        to=target_pathways,
        with_defaults=micro_er_defaults,
    )
    pdt.assert_frame_equal(res, micro_er_conformed)


@pytest.fixture(scope="module")
def micro_dd():
    df = pd.DataFrame(
        {
            "side": ["LL", "RR", "LL"],
            "source_region": ["SSp-bfd2", "MOs2", "FRP5"],
            "target_region": ["SSp-bfd2", "MOs2", "FRP5"],
            "source_mtype": ["L2_TPC:B", "L2_IPC", "L5_TPC:C"],
            "target_mtype": ["L2_TPC:B", "L23_SBC", "L4_DBC"],
            "weight": [1.0, 1.0, 1.0],
            "exponent": [0.008, 0.008, 0.008],
            "nsynconn_mean": [3.0, 3.0, 3.0],
            "nsynconn_std": [1.5, 1.5, 1.5],
            "delay_velocity": [250.0, 250.0, 250.0],
            "delay_offset": [0.8, 0.8, 0.8],
        }
    )
    return test_module._conform_types(df)


@pytest.fixture(scope="module")
def micro_dd_overrides():
    df = pd.DataFrame(
        {
            "side": ["LL", "RL"],
            "source_region": ["SSp-bfd2", "SSp-bfd3"],
            "target_region": ["SSp-bfd2", "SSp-bfd2"],
            "source_mtype": ["L2_TPC:B", "L3_TPC:A"],
            "target_mtype": ["L2_TPC:B", "L3_TPC:B"],
            "weight": [1.5, 0.5],
            "exponent": [0.002, 0.005],
            "nsynconn_mean": [3.0, 2.0],
            "nsynconn_std": [1.5, 0.1],
            "delay_velocity": [250.0, 100.0],
            "delay_offset": [0.9, 0.5],
        }
    )
    return test_module._conform_types(df)


@pytest.fixture(scope="module")
def micro_dd_assembled():
    df = pd.DataFrame(
        {
            "side": ["LL", "RR", "LL", "RL"],
            "source_region": ["SSp-bfd2", "MOs2", "FRP5", "SSp-bfd3"],
            "target_region": ["SSp-bfd2", "MOs2", "FRP5", "SSp-bfd2"],
            "source_mtype": ["L2_TPC:B", "L2_IPC", "L5_TPC:C", "L3_TPC:A"],
            "target_mtype": ["L2_TPC:B", "L23_SBC", "L4_DBC", "L3_TPC:B"],
            "weight": [1.5, 1.0, 1.0, 0.5],
            "exponent": [0.002, 0.008, 0.008, 0.005],
            "nsynconn_mean": [3.0, 3.0, 3.0, 2.0],
            "nsynconn_std": [1.5, 1.5, 1.5, 0.1],
            "delay_velocity": [250.0, 250.0, 250.0, 100.0],
            "delay_offset": [0.9, 0.8, 0.8, 0.5],
        }
    )
    return test_module._conform_types(df)


@pytest.fixture(scope="module")
def micro_dd_conformed():
    """Assembled micro dd parameters conformed to variant matrix."""
    df = pd.DataFrame(
        {
            "side": ["RR", "LR", "RL"],
            "source_region": ["MOs2", "new", "SSp-bfd3"],
            "target_region": ["MOs2", "new", "SSp-bfd2"],
            "source_mtype": ["L2_IPC", "new", "L3_TPC:A"],
            "target_mtype": ["L23_SBC", "new", "L3_TPC:B"],
            "weight": [1.0, 2.0, 0.5],
            "exponent": [0.008, 0.001, 0.005],
            "nsynconn_mean": [3.0, 10.0, 2.0],
            "nsynconn_std": [1.5, 1.0, 0.1],
            "delay_velocity": [250.0, 50.0, 100.0],
            "delay_offset": [0.8, 1.0, 0.5],
        }
    )
    return test_module._conform_types(df)


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


def test_assemble__dd(micro_dd_file, micro_dd_overrides_file, micro_dd_assembled):
    res = test_module._assemble(
        initial_path=micro_dd_file,
        index_columns=test_module.MICRO_INDEX,
        overrides_path=micro_dd_overrides_file,
    )
    pdt.assert_frame_equal(res, micro_dd_assembled)


def test_conform__dd(variants_conformed, micro_dd_defaults, micro_dd_assembled, micro_dd_conformed):
    mask = variants_conformed["variant"] == "placeholder__distance_dependent"
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
            "placeholder__erdos_renyi": {
                "params": config_er_defaults,
            },
            "placeholder__distance_dependent": {"params": config_dd_defaults},
        },
        "initial": {
            "variants": variants_file,
            "placeholder__erdos_renyi": micro_er_file,
            "placeholder__distance_dependent": micro_dd_file,
        },
        "overrides": {
            "variants": variants_overrides_file,
            "placeholder__erdos_renyi": micro_er_overrides_file,
            "placeholder__distance_dependent": micro_dd_overrides_file,
        },
    }


def test_assemble__no_overrides(micro_dd, micro_dd_file):
    res = test_module._assemble(
        micro_dd_file, index_columns=test_module.MICRO_INDEX, overrides_path=None
    )
    pdt.assert_frame_equal(res, micro_dd)


def test_assemble_micro_matrices(
    micro_config, macro_assembled, micro_er_conformed, micro_dd_conformed
):
    res = test_module.assemble_micro_matrices(micro_config, macro_assembled)

    assert res.keys() == {"placeholder__erdos_renyi", "placeholder__distance_dependent"}

    pdt.assert_frame_equal(res["placeholder__erdos_renyi"], micro_er_conformed)
    pdt.assert_frame_equal(res["placeholder__distance_dependent"], micro_dd_conformed)
