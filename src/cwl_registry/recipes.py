"""Construction of recipes for circuit building."""
from typing import Any, Dict, List

import pandas as pd


def build_cell_composition_from_me_densities(region: str, me_type_densities: Dict[str, Any]):
    """Create cell composition file from KG me densities."""
    composition = {"version": "v2", "neurons": []}

    for _, mtype in me_type_densities.items():

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
