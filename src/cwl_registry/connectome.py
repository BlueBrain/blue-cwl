"""Connectome assembly of macro and micro connectome matrices.

All matrices are comprised of columns, a subset of which is the index columns and the rest the
values.

The index columns are used to build dataframe indices and perform pathway comparisons and update.

Macro Matrix
------------

The macro matrix consists of the following columns:
    - side (category)
    - source_region (category)
    - target_region (category)
    - value (float32)

where:
    index columns: [side, source_region, target_region]
    values       : [value]

Micro Matrices
--------------

The micro connectome configuration consists of the following matrices:

Variant matrix:
    - index columns: [side, source_region, target_region, source_mtype, target_mtype]
    - value columns: [variant]

Variant parameter matrices:
    - placeholder__erdos_renyi:
        index columns: [side, source_region, target_region, source_mtype, target_mtype]
        value columns: [weight, nsynconn_mean, nsynconn_std, delay_velocity, delay_offset]

    - placeholder__distance_dependent:
        index columns: [side, source_region, target_region, source_mtype, target_mtype]
        value columns: [weight, exponent, nsynconn_mean, nsynconn_std, delay_velocity, delay_offset]

Steps, assembly & conformity
----------------------------

step 1: Macro assembly

The macro matrix is assembled by taking the initial et of rows and connecting them w/ the overrides.
See `_assemble` for more details.

Step 2: Micro variants assembly & conformity to step 1

The variant matrix is assembled and then it is conformed to the macro matrix so that additional
pathways in the macro are added with default values and pathways not in it are removed.
See `_conform` for more details.

step 3: Micro variant parameters assembly & conformity to step 2

Each variant parameters matrix is assembled and conformed to the subset of the pathways that will be
built with the respective variant. Pathways in variant matrix that are not in the micro variant one
are added with default values, whereas pathways not in it are removed.
"""
import logging
from typing import Optional

import numpy as np
import pandas as pd

from cwl_registry import utils
from cwl_registry.exceptions import CWLWorkflowError

L = logging.getLogger(__name__)


MACRO_INDEX = ["side", "source_region", "target_region"]
MICRO_INDEX = ["side", "source_region", "target_region", "source_mtype", "target_mtype"]
CATEGORY_COLUMNS = MICRO_INDEX + ["variant"]


def assemble_macro_matrix(config: dict) -> pd.DataFrame:
    """Assemble macro connectome dataframe from the materialized macro config.

    Args:
        config: Materialized macro config. Example:
            {
                "initial": {"connection_strength": path/to/initial/arrow/file},
                "overrides": {"connection_strength": path/to/overrides/arrow/file}
            }

    Returns:
        DataFrame with the following categorical columns:
            - side
            - source_region
            - target_region
            - variant

    Note: overrides can be empty.
    """
    initial_path = config["initial"]["connection_strength"]
    overrides_path = config["overrides"].get("connection_strength", None)

    if overrides_path is None:
        L.warning("No overrides found for macro connection strength matrix.")

    df = _assemble(
        initial_path=initial_path,
        index_columns=MACRO_INDEX,
        overrides_path=overrides_path,
    )

    # zero connection strength means that the pathway will not be built
    df = df[df["value"] != 0.0].reset_index(drop=True)

    return _conform_types(df)


def assemble_micro_matrices(
    config: dict, macro_connectome: Optional[pd.DataFrame] = None
) -> dict[str, pd.DataFrame]:
    """Assemble variant matrices from config with initial data and overrides.

    Args:
        config: Materialized micro connectome config.
        macro_connectome: Optional macro connectome dataframe.
    """
    initial_section = config["initial"]
    overrides_section = config["overrides"]

    # dataframe with variant name over all pathways
    variants = _assemble(
        initial_path=initial_section["variants"],
        index_columns=MICRO_INDEX,
        overrides_path=overrides_section.get("variants", None),
    )

    if macro_connectome is not None:
        variants = _conform(
            variants,
            to=macro_connectome[MACRO_INDEX],
            with_defaults={
                "variant": "placeholder__erdos_renyi",
                "source_mtype": "GEN_mtype",
                "target_mtype": "GEN_mtype",
            },
        )

    L.debug("Number of pathways in variant matrix: %d", len(variants))

    per_variant_pathways = {}
    for variant_name, variant_data in config["variants"].items():
        # variant-specific parameters
        variant_parameters = _assemble(
            initial_path=initial_section[variant_name],
            index_columns=MICRO_INDEX,
            overrides_path=overrides_section.get(variant_name, None),
        )

        defaults = {
            param_name: param_data["default"]
            for param_name, param_data in variant_data["params"].items()
        }

        _check_defaults_consistency(variant_parameters, MICRO_INDEX, defaults)

        per_variant_pathways[variant_name] = _conform(
            parameters=variant_parameters,
            to=variants[variants["variant"] == variant_name][MICRO_INDEX],
            with_defaults=defaults,
        )
    return per_variant_pathways


def _check_defaults_consistency(df, index_columns: list[str], defaults):
    """Check that the non-index columns in df are present as keys in defaults."""
    value_columns = set(df.columns) - set(index_columns)

    if value_columns != set(defaults):
        raise CWLWorkflowError(
            "Defaults keys are not matching dataframe's value columns.\n"
            f"Datataframe value columns: {sorted(value_columns)}\n"
            f"Defaults keys: {sorted(defaults)}"
        )


def _conform_types(df):
    for col in df.columns:
        if col in CATEGORY_COLUMNS:
            df[col] = df[col].astype("category").cat.remove_unused_categories()
        else:
            df[col] = df[col].astype(np.float32)
    return df


def _assemble(initial_path: str, index_columns: list[str], overrides_path: Optional[str] = None):
    """Assemble a dataframe from a dataframe for initial values and optional overrides.

    The dataframe assembly is taking place by loading the initial arrow file and updating it with
    the optional overrides in two steps:
        * Update the value rows of initial with overrides if the share the same 'index_columns' row.
        * Append the value rows of overrides the index of which does not exist in initial.

    Args:
        initial_path: Path to initial dataframe stored in arrow format.
        index_columns: List of column names to use as an index to compare the dataframes.

    Returns:
        Assembled dataframe.
    """
    initial = utils.load_arrow(initial_path)
    L.debug("Initial dataframe size: %d", len(initial))

    if overrides_path is None:
        return initial

    initial.set_index(index_columns, inplace=True)

    overrides = utils.load_arrow(overrides_path).set_index(index_columns)
    L.debug("Overrides dataframe size: %d", len(overrides))

    is_in_initial = overrides.index.isin(initial.index)

    if is_in_initial.any():
        initial.update(overrides[is_in_initial])
        initial = pd.concat([initial, overrides[~is_in_initial]])
        L.debug("(n_updates, n_appends): (%d, %d)", sum(is_in_initial), sum(~is_in_initial))
    else:
        initial = pd.concat([initial, overrides])
        L.debug("(n_updates, n_appends): (0, %d)", len(overrides))

    L.debug("Final matrix size: %d", len(initial))

    return _conform_types(initial.reset_index())


def _conform(parameters: pd.DataFrame, to: pd.DataFrame, with_defaults) -> pd.DataFrame:
    """Conform parameters to 'to' dataframe using 'on' columns.

    Args:
        parameters: Dataframe with pathways and parameters.
        to: Dataframe with pathways to restrict/expand parameters.
        with_defaults: A dictionary with the default values of the parameters to use.

    Rules:
        * pathways in variants and not in parameters will get default values.
        * pathways in parameters but not in variants will be removed.

    Returns:
        A dataframe with the parameters columns expanded/reduced based on 'to' dataframe columns.
    """
    parameters = parameters.set_index(to.columns.tolist())
    to = to.set_index(to.columns.tolist())

    parameters_in_variants_mask = parameters.index.isin(to.index)

    if parameters_in_variants_mask is not None:
        parameters = parameters[parameters_in_variants_mask]

    final = pd.DataFrame(index=to.index, columns=parameters.columns, data=with_defaults)
    final.update(parameters)

    return _conform_types(final.reset_index())
