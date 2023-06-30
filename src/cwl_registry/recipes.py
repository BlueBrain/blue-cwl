"""Construction of recipes for circuit building."""
import importlib.resources
import shutil
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from cwl_registry import utils


def build_cell_composition_from_me_densities(region: str, me_type_densities: Dict[str, Any]):
    """Create cell composition file from KG me densities."""
    composition = {"version": "v2", "neurons": []}

    for _, mtype in me_type_densities["mtypes"].items():
        mtype_name = mtype["label"]

        for _, etype in mtype["etypes"].items():
            density = etype["path"]
            etype_name = etype["label"]

            composition["neurons"].append(
                {
                    "density": density,
                    "region": region,
                    "traits": {
                        "mtype": mtype_name,
                        "etype": etype_name,
                    },
                }
            )

    return composition


def build_mtype_taxonomy(mtypes: List[str]):
    """A temporary solution in creating a taxonomy for circuit-build."""
    taxonomy = [
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
    ]
    df = pd.DataFrame(taxonomy, columns=["mtype", "mClass", "sClass"])

    available_mtypes_mask = df.loc[:, "mtype"].isin(list(mtypes))

    per_mtype_df = df.loc[available_mtypes_mask]

    return per_mtype_df.reset_index(drop=True)


def build_connectome_manipulator_recipe(
    circuit_config_path: str, micro_matrices, output_dir: Path
) -> dict:
    """Build connectome manipulator recipe."""
    key_mapping = {
        "source_hemisphere": "src_hemisphere",
        "target_hemisphere": "dst_hemisphere",
        "source_region": "src_region",
        "target_region": "dst_region",
        "source_mtype": "src_type",
        "target_mtype": "dst_type",
        "pconn": "connprob_coeff_a",
        "scale": "connprob_coeff_a",
        "exponent": "connprob_coeff_b",
        "delay_velocity": "lindelay_delay_mean_coeff_a",
        "delay_offset": "lindelay_delay_mean_coeff_b",
    }

    def reset_multi_index(df):
        if isinstance(df.index, pd.MultiIndex):
            return df.reset_index()
        return df

    def build_pathways(algo, df):
        df = reset_multi_index(df)

        # remove zero probabilities of connection
        if "pconn" in df.columns:
            df = df[~np.isclose(df["pconn"], 0.0)]

        # remove zero or infinite scales
        if "scale" in df.columns:
            df = df[~np.isclose(df["scale"], 0.0)]

        df = df.reset_index(drop=True).rename(columns=key_mapping)

        if algo == "placeholder__erdos_renyi":
            df["connprob_order"] = 1
        elif algo == "placeholder__distance_dependent":
            df["connprob_order"] = 2
        else:
            raise ValueError(algo)

        return df

    frames = [build_pathways(name, df) for name, df in micro_matrices.items()]
    merged_frame = pd.concat(frames, ignore_index=True)

    merged_frame = merged_frame.set_index(
        ["src_hemisphere", "dst_hemisphere", "src_region", "dst_region"]
    )
    merged_frame = merged_frame.sort_index()
    output_file = output_dir / "pathways.parquet"

    utils.write_parquet(filepath=output_file, dataframe=merged_frame, index=True)

    config = {
        "circuit_config": str(circuit_config_path),
        "seed": 0,
        "N_split_nodes": 1000,
        "manip": {
            "name": "WholeBrainMacroMicroWiring",
            "fcts": [
                {
                    "source": "conn_wiring",
                    "model_pathways": str(output_file),
                    "model_config": {
                        "prob_model_spec": {"model": "ConnProbModel"},
                        "nsynconn_model_spec": {"model": "NSynConnModel"},
                        "delay_model_spec": {"model": "LinDelayModel"},
                    },
                }
            ],
        },
    }

    return config


def build_connectome_distance_dependent_recipe(config_path, configuration, output_dir):
    """Build recipe for connectome manipulator."""
    res = {
        "circuit_config": str(config_path),
        "output_path": str(output_dir),
        "seed": 0,
        "manip": {"name": "ConnWiringPerPathway_DD", "fcts": []},
    }
    # TODO: Add hemisphere when hemispheres are available
    for row in configuration.itertuples():
        res["manip"]["fcts"].append(
            {
                "source": "conn_wiring",
                "kwargs": {
                    "sel_src": {
                        "region": row.ri,
                        "mtype": row.mi,
                    },
                    "sel_dest": {
                        "region": row.rj,
                        "mtype": row.mj,
                    },
                    "amount_pct": 100.0,
                    "prob_model_file": {
                        "model": "ConnProb2ndOrderExpModel",
                        "scale": row.scale,
                        "exponent": row.exponent,
                    },
                    "nsynconn_model_file": {
                        "model": "ConnPropsModel",
                        "src_types": [
                            row.mi,
                        ],
                        "tgt_types": [
                            row.mj,
                        ],
                        "prop_stats": {
                            "n_syn_per_conn": {
                                row.mi: {
                                    row.mj: {
                                        "type": "gamma",
                                        "mean": row.mean_synapses_per_connection,
                                        "std": row.sdev_synapses_per_connection,
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
                        "delay_mean_coefs": [
                            row.mean_conductance_velocity,
                            0.003,
                        ],
                        "delay_std": row.sdev_conductance_velocity,
                        "delay_min": 0.2,
                    },
                },
            }
        )
    return res


def write_functionalizer_recipe(output_file):
    """Copy an existing xml recipe."""
    path = importlib.resources.files("cwl_registry") / "data" / "builderRecipeAllPathways.xml"

    shutil.copyfile(path, output_file)

    return output_file
