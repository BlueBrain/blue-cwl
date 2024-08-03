# SPDX-License-Identifier: Apache-2.0

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

# ruff: noqa: D214, D417

import logging
from collections.abc import Callable, Sequence
from functools import partial
from itertools import product

import libsonata
import numpy as np
import pandas as pd
from scipy.spatial.distance import cdist

from blue_cwl import population_utils, utils
from blue_cwl.constants import HR, HRM
from blue_cwl.exceptions import CWLWorkflowError


class Constants:
    """Connectome related Constants."""

    HEMISPHERES = ["left", "right"]

    SOURCE_MACRO_LEVELS = ["source_hemisphere", "source_region"]
    TARGET_MACRO_LEVELS = ["target_hemisphere", "target_region"]

    MACRO_LEVELS = [
        x for pair in zip(SOURCE_MACRO_LEVELS, TARGET_MACRO_LEVELS, strict=True) for x in pair
    ]
    COMPACT_MACRO_LEVELS = ["side"] + MACRO_LEVELS[2:]
    MACRO_VALUES = ["value"]
    MACRO_ALL = MACRO_LEVELS + MACRO_VALUES

    MICRO_MACRO_DIFF = ["source_mtype", "target_mtype"]
    SOURCE_MICRO_LEVELS = SOURCE_MACRO_LEVELS + [MICRO_MACRO_DIFF[0]]
    TARGET_MICRO_LEVELS = TARGET_MACRO_LEVELS + [MICRO_MACRO_DIFF[1]]

    MICRO_LEVELS = [
        x for pair in zip(SOURCE_MICRO_LEVELS, TARGET_MICRO_LEVELS, strict=True) for x in pair
    ]
    COMPACT_MICRO_LEVELS = ["side"] + MICRO_LEVELS[2:]

    MICRO_VARIANT_VALUES = ["variant"]

    MICRO_ER_VALUES = ["weight", "nsynconn_mean", "nsynconn_std", "delay_velocity", "delay_offset"]
    MICRO_DD_VALUES = [
        "weight",
        "exponent",
        "nsynconn_mean",
        "nsynconn_std",
        "delay_velocity",
        "delay_offset",
    ]

    MICRO_VALUES_DICT = {
        "variants": MICRO_VARIANT_VALUES,
        "placeholder__erdos_renyi": MICRO_ER_VALUES,
        "placeholder__distance_dependent": MICRO_DD_VALUES,
    }

    VARIANTS_COLUMNS = MICRO_LEVELS + ["variant"]
    COMPACT_VARIANTS_COLUMNS = ["side"] + VARIANTS_COLUMNS[:2]

    CATEGORY_COLUMNS = MICRO_LEVELS + ["side", "variant"]


L = logging.getLogger(__name__)


@utils.log
def assemble_macro_matrix(macro_config: dict) -> pd.DataFrame:
    """Assemble macro connectome dataframe from the materialized macro config.

    The macro matrix is assembled by combining the initial pathways and the overrides if any.

    macro_config arrow files are expected to have the following columns:
        - side (category)
        - source_region (category)
        - target_region (category)
        - value (float32)

    where  the 'side'  is the compact form of the source and target hemispheres, e.g. 'LR'.

    Args:
        macro_config: Materialized macro config with the arrow file paths.

            Example:

            .. code-block:: python

                {
                    "initial": {"connection_strength": "path/to/arrow/file"},
                    "overrides": {"connection_strength": "path/to/arrow/file"},
                }

    Returns:
        DataFrame with the following categorical columns:
            - source_hemisphere (category)
            - target_hemisphere (category)
            - source_region (category)
            - target_region (category)
            - value (float32)

    Note:
        - 'overrides' can be empty and rows with zero 'value' are removed.
        - Duplicate pathways will be dropped keeping the last entry.
    """
    initial_path = macro_config["initial"]["connection_strength"]
    overrides_path = macro_config["overrides"].get("connection_strength", None)

    if overrides_path is None:
        L.warning("No overrides found for macro connection strength matrix.")

    df = _assemble(
        initial_path=initial_path,
        overrides_path=overrides_path,
        index_columns=Constants.MACRO_LEVELS,
        value_columns=Constants.MACRO_VALUES,
    )

    mask = df["value"] == 0.0
    if mask.any():
        L.warning(
            "Zero connection strengths found in macro matrix and will be removed: %s",
            df[mask],
        )

    # zero connection strength means that the pathway will not be built
    df = df[~mask].reset_index(drop=True)

    df = _conform_types(df)

    _validate_no_multi_index(df)
    _validate_frame_columns(df, Constants.MACRO_ALL)

    return df


@utils.log
def assemble_micro_matrix(micro_config: dict, variant_name: str) -> pd.DataFrame:
    """Assemble micro connectome dataframe from the materialized micro config.

    Args:
        micro_config: Materialized macro config with the arrow file paths.

            Example:

            .. code-block:: python

                {
                    "initial": {
                        "placeholder__erdos_renyi": "path/to/initial/arrow/er_file",
                        "placeholder__distance_dependent": "path/to/initial/arrow/dd_file",
                    },
                    "overrides": {
                        "placeholder__erdos_renyi": "path/to/overrides/arrow/er_file",
                        "placeholder__distance_dependent": "path/to/overrides/arrow/dd_file",
                    },
                }

        variant_name: Name of the variant algorithm. One of
            - placeholder__erdos_renyi
            - placeholder__distance_dependent

    Returns:
        DataFrame with the following columns:
            - source_hemisphere (category)
            - target_hemisphere (category)
            - source_region (category)
            - target_region (category)
            - weight (float32)
            - nsynconn_mean (float32)
            - nsynconn_std (float32)
            - delay_velocity (float32)
            - delay_offset (float32)

            # placeholder__distance_dependent matrix only
            - exponent (float32)

    Note:
        - 'overrides' can be empty.
        - Duplicate pathways will be dropped keeping the last entry.
    """
    return _assemble(
        initial_path=micro_config["initial"][variant_name],
        index_columns=Constants.MICRO_LEVELS,
        value_columns=Constants.MICRO_VALUES_DICT[variant_name],
        overrides_path=micro_config["overrides"].get(variant_name, None),
    )


@utils.log
def resolve_micro_matrices(
    micro_config: dict, macro_matrix: pd.DataFrame, population, region_volumes
) -> dict[str, pd.DataFrame]:
    """Load and resolve micro matrices from materialized micro config.

    Args:
        micro_config: Materialized macro config with the arrow file paths.

            Example:

            .. code-block:: python

                {
                    "initial": {
                        "placeholder__erdos_renyi": path / to / initial / arrow / er_file,
                        "placeholder__distance_dependent": path / to / initial / arrow / dd_file,
                    },
                    "overrides": {
                        "placeholder__erdos_renyi": path / to / overrides / arrow / er_file,
                        "placeholder__distance_dependent": path / to / overrides / arrow / dd_file,
                    },
                }

        macro_matrix: DataFrame with RangeIndex and the following columns:
            - source_hemisphere (category)
            - target_hemisphere (category)
            - source_region (category)
            - target_region (category)
            - value (float32)
        population: Cells node population
        region_volumes: A series of volumes indexed by brain region acronym.

    Returns: A dictionary the keys of which are variant names and the values micro matrices.
    """
    cell_counts = population_utils.get_HRM_counts(population)

    variants_matrix = assemble_micro_matrix(micro_config, "variants")
    variants_matrix = _conform_variants(
        variants_matrix,
        macro_matrix,
        cell_counts.index.to_frame(index=False),
    )

    return {
        variant_name: _resolve_micro_matrix(
            micro_config=micro_config,
            variant_name=variant_name,
            macro_matrix=macro_matrix,
            variants_matrix=variants_matrix,
            population=population,
            cell_counts=cell_counts,
            region_volumes=region_volumes,
        )
        for variant_name in micro_config["variants"]
    }


def _resolve_micro_matrix(
    micro_config: dict,
    variant_name: str,
    macro_matrix: pd.DataFrame,
    variants_matrix: pd.DataFrame,
    population,
    cell_counts: pd.DataFrame,
    region_volumes: pd.DataFrame,
) -> pd.DataFrame:
    micro_matrix = assemble_micro_matrix(micro_config, variant_name)

    defaults = {
        param_name: param_data["default"]
        for param_name, param_data in micro_config["variants"][variant_name]["params"].items()
    }

    _check_defaults_consistency(micro_matrix, Constants.MICRO_LEVELS, defaults)

    micro_matrix = _conform(
        parameters=micro_matrix,
        to=variants_matrix[variants_matrix["variant"] == variant_name][Constants.MICRO_LEVELS],
        with_defaults=defaults,
    )

    micro_matrix = _scale_micro_matrix(
        variant_name=variant_name,
        macro=macro_matrix,
        micro=micro_matrix,
        cell_counts=cell_counts,
        population=population,
        region_volumes=region_volumes,
    )

    # required only for scaling
    micro_matrix = micro_matrix.drop(columns="weight").reset_index(drop=True)

    return micro_matrix


@utils.log
def _conform_variants(
    variants: pd.DataFrame, macro: pd.DataFrame, available_pathways: pd.DataFrame
):
    """Conform variant matrix to the macro and available pathways."""
    # remove micro/macro pathways that do not exist in the circuit
    macro = _filter_macro_by_circuit_pathways(macro, available_pathways)
    variants = _filter_micro_by_circuit_pathways(variants, available_pathways)

    variants = variants.set_index(Constants.MACRO_LEVELS)
    macro = macro.set_index(Constants.MACRO_LEVELS)

    # remove variants that do not exist in macro
    variants = variants[variants.index.isin(macro.index)]

    # The rows where the algorithm is marked as 'disabled' will be removed at the end of conforming.
    # We want first for the step below to expand all mtype pathway combinations from macro and then
    # remove the specific mtype combinations that are marked as 'disabled'.
    to_remove_mask = variants["variant"] == "disabled"
    to_remove = variants[to_remove_mask]
    variants = variants[~to_remove_mask]

    # for (H, R) pathways that exist in macro but not in variants
    # add all combinations of the mtype pathways inside these regions
    only_in_macro = ~macro.index.isin(variants.index)

    if only_in_macro.any():
        variants = variants.set_index(Constants.MICRO_MACRO_DIFF, append=True)

        to_add_pathways = _macro_to_variant_pathways(
            macro_pathways=macro[only_in_macro].reset_index(),
            available_pathways=available_pathways,
            default_variant="placeholder__erdos_renyi",
        )

        variants = pd.concat([variants, to_add_pathways])

        L.info(
            (
                "Found %d (hemisphere, region) pathways in macro that are not in variants. "
                "%d (hemisphere, region, mtype) )pathways added to variants from combining the pre "
                "and post mtypes in each pathway."
            ),
            sum(only_in_macro),
            len(to_add_pathways),
        )

    if not to_remove.empty:
        L.info("Found %d disabled pathways in variants and will be removed.", len(to_remove))
        to_remove = to_remove.set_index(Constants.MICRO_MACRO_DIFF, append=True)
        # Set errors to ignore so that only existing are removed without raising otherwise
        variants = variants.drop(index=to_remove.index, errors="ignore")

    return _conform_types(variants.reset_index())


def _scale_micro_matrix(
    variant_name: str,
    macro: pd.DataFrame,
    micro: pd.DataFrame,
    population,
    cell_counts: pd.Series,
    region_volumes: pd.DataFrame,
) -> pd.DataFrame:
    """Scale relative micro matrix by adding a 'pconn' or 'scale' column.

    The probability of connection is calculated for both variants. However, in the case of
    'placeholder__distance_dependent' variant the 'pconn' is replaced by the 'scale' column.
    """
    df = micro.copy()

    L.info("Calculating probability of connection...")

    df["pconn"] = _probability_of_connection(
        micro=micro,
        macro=macro,
        cell_counts=cell_counts,
        region_volumes=region_volumes,
    )

    # 'scale' is using 'pconn' and is exclusive to DD variant
    if variant_name == "placeholder__distance_dependent":
        L.info("Calculating distance dependent scale...")

        df["scale"] = _scale_distance_dependent(
            micro=df,
            population=population,
            cell_counts=cell_counts,
            population_fraction=0.1,
        )

        # needed only to calculate scale for dd
        df = df.drop(columns="pconn")
    return df


@utils.log
def _scale_distance_dependent(
    micro: pd.DataFrame,
    population: libsonata.NodePopulation,
    cell_counts: pd.DataFrame,
    population_fraction: float,
) -> pd.DataFrame:
    positions = _subsample_HRM_positions(
        df=population_utils.get_HRM_positions(population),
        fraction=population_fraction,
        min_size=500,
        max_size=5000,
    ).reset_index()

    micro = micro.copy()

    # add pathway index to keep track when expanding the positions per pathway below
    micro["pathway"] = micro.index

    source_groups = _grouped_by_pathway_positions(
        micro=micro[Constants.SOURCE_MICRO_LEVELS + ["pathway"]],
        positions=positions,
        left_on=Constants.SOURCE_MICRO_LEVELS,
    )

    target_groups = _grouped_by_pathway_positions(
        micro=micro[Constants.TARGET_MICRO_LEVELS + ["pathway"]],
        positions=positions,
        left_on=Constants.TARGET_MICRO_LEVELS,
    )

    n_bins = 50
    max_dist = 5000.0
    dist_bins = np.linspace(0.0, max_dist, n_bins + 1)

    hists = np.empty((len(micro), n_bins), dtype=float)
    for (i, pre_pos), (j, post_pos) in zip(source_groups, target_groups, strict=True):
        if i != j:
            raise CWLWorkflowError("i != j")
        distances = cdist(pre_pos, post_pos).flatten()
        hists[i] = np.histogram(distances[distances > 0.0], bins=dist_bins)[0]

    # scale histograms to account for population subsampling
    hists *= (1.0 / population_fraction) ** 2

    pre_counts, post_counts = _pre_post_cell_counts(micro, cell_counts)

    n_conn = micro["pconn"].values * pre_counts * post_counts

    scales = _estimate_dd_scale(n_conn, hists, dist_bins, micro["exponent"].values)

    return _sanitize_and_clip(scales, 0.0, 1.0)


def _grouped_by_pathway_positions(micro, positions, left_on):
    return pd.merge(micro, positions, left_on=left_on, right_on=HRM, how="inner").groupby(
        "pathway", sort=True, group_keys=False
    )[["x", "y", "z"]]


def _estimate_dd_scale(
    n_conn: float, dist_hist: np.ndarray, dist_bins: np.ndarray, exponents: np.ndarray
):
    """Estimate the distance dependent scale based on the number of connection to be created."""
    bin_centers = dist_bins[:-1] + 0.5 * np.diff(dist_bins[[0, 1]])

    # (Npathways X Nbins)
    exponential_probabilities = np.exp(-exponents[:, np.newaxis] * bin_centers)

    # Npathways
    n_conn_ref = np.sum(exponential_probabilities * dist_hist, axis=1)

    scales = n_conn / n_conn_ref

    return scales


def _subsample_HRM_positions(df, fraction: float, min_size: int, max_size: int):
    def subsample(df):
        if len(df) <= min_size:
            return df
        selection_size = min(max_size, round(fraction * len(df)))
        return df.sample(n=selection_size, replace=False, random_state=0)

    return df.groupby(["hemisphere", "region", "mtype"], group_keys=False, observed=True).apply(
        subsample
    )


def _check_defaults_consistency(df, index_columns: list[str], defaults):
    """Check that the non-index columns in df are present as keys in defaults."""
    value_columns = set(df.columns) - set(index_columns)

    if value_columns != set(defaults):
        raise CWLWorkflowError(
            "Defaults keys are not matching dataframe's value columns.\n"
            f"Datataframe value columns: {sorted(value_columns)}\n"
            f"Defaults keys: {sorted(defaults)}"
        )


def _conform_types(df: pd.DataFrame):
    for col in df.columns:
        if col in Constants.CATEGORY_COLUMNS:
            df[col] = df[col].astype("category").cat.remove_unused_categories()
        else:
            df[col] = df[col].astype(np.float32)
    return df


def _load_connectome_matrix(
    path: str,
    validators: Sequence[Callable[[pd.DataFrame], None]] | None = None,
    sanitizers: Sequence[Callable[[pd.DataFrame], None]] | None = None,
) -> pd.DataFrame:
    frame = utils.load_arrow(path)

    frame = _split_side_into_hemispheres(frame)

    if validators:
        for validator in validators:
            validator(frame)

    if sanitizers:
        for sanitizer in sanitizers:
            frame = sanitizer(frame)

    return frame


def _validate_frame_columns(df: pd.DataFrame, columns: list[str]) -> None:
    columns = sorted(df.columns)
    expected_columns = sorted(columns)

    if columns != expected_columns:
        raise CWLWorkflowError(f"Found columns {columns} but expected {expected_columns}")


def _validate_no_multi_index(df: pd.DataFrame) -> None:
    if isinstance(df, pd.MultiIndex):
        raise CWLWorkflowError(
            f"A MultiIndex is not expected for a connectome frame. Found: {df.index}"
        )


def _sanitize_drop_duplicates(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    return frame.drop_duplicates(subset=columns, keep="last")


@utils.log
def _assemble(
    initial_path: str,
    index_columns: list[str],
    value_columns: list[str],
    overrides_path: str | None = None,
):
    """Assemble a dataframe from a dataframe for initial values and optional overrides.

    The dataframe assembly is taking place by loading the initial arrow file and updating it with
    the optional overrides in two steps:
        * Update the value rows of initial with overrides if the share the same 'index_columns' row.
        * Append the value rows of overrides the index of which does not exist in initial.

    Args:
        initial_path: Path to initial dataframe stored in arrow format.
        index_columns: List of column names to use as an index to compare the dataframes.
        value_columns: List of column names to use as values.
        overrides_path: Optional path to connectome overrides.

    Returns:
        Assembled dataframe.
    """
    validators = (
        _validate_no_multi_index,
        partial(_validate_frame_columns, columns=index_columns + value_columns),
    )

    sanitizers = (partial(_sanitize_drop_duplicates, columns=index_columns),)

    initial = _load_connectome_matrix(initial_path, validators, sanitizers)

    L.debug("Initial dafaframe:\n%s", initial)

    if overrides_path is None:
        return initial

    overrides = _load_connectome_matrix(overrides_path, validators, sanitizers)

    L.debug("Overrides dafaframe:\n%s", initial)

    initial.set_index(index_columns, inplace=True)
    overrides.set_index(index_columns, inplace=True)

    is_in_initial = overrides.index.isin(initial.index)

    if is_in_initial.any():
        initial.update(overrides[is_in_initial])
        initial = pd.concat([initial, overrides[~is_in_initial]])
        L.debug("(n_updates, n_appends): (%d, %d)", sum(is_in_initial), sum(~is_in_initial))
    else:
        initial = pd.concat([initial, overrides])
        L.debug("(n_updates, n_appends): (0, %d)", len(overrides))

    L.debug("Final dataframe: %s", initial)

    return _conform_types(initial.reset_index())


def _columns_as_index(frame, columns):
    return pd.MultiIndex.from_frame(frame[columns])


def _filter_macro_by_circuit_pathways(macro, pathways):
    index = _columns_as_index(pathways, HR).drop_duplicates()

    source = _columns_as_index(macro, Constants.SOURCE_MACRO_LEVELS)
    target = _columns_as_index(macro, Constants.TARGET_MACRO_LEVELS)

    return macro[source.isin(index) & target.isin(index)].reset_index(drop=True)


def _filter_micro_by_circuit_pathways(micro, pathways):
    index = _columns_as_index(pathways, HRM).drop_duplicates()

    source = _columns_as_index(micro, Constants.SOURCE_MICRO_LEVELS)
    target = _columns_as_index(micro, Constants.TARGET_MICRO_LEVELS)

    return micro[source.isin(index) & target.isin(index)].reset_index(drop=True)


def _create_combos(pathways, source, target):
    source.names = target.names = HR

    source_groups = pd.DataFrame(data={"indicator": range(len(source))}, index=source)
    target_groups = pd.DataFrame(data={"indicator": range(len(target))}, index=target)

    source_groups = pathways.join(source_groups).groupby("indicator")["mtype"]
    target_groups = pathways.join(target_groups).groupby("indicator")["mtype"]

    values = []

    for (_, pre_mtypes), (_, post_mtypes) in zip(source_groups, target_groups, strict=True):
        source_hemisphere, source_region = pre_mtypes.index[0]
        target_hemisphere, target_region = post_mtypes.index[0]

        values.extend(
            (source_hemisphere, target_hemisphere, source_region, target_region, pre, post)
            for pre, post in product(pre_mtypes, post_mtypes)
        )

    return pd.DataFrame(values, columns=Constants.MICRO_LEVELS).set_index(Constants.MACRO_LEVELS)


def _macro_to_variant_pathways(macro_pathways, available_pathways, default_variant):
    L.debug("Found %d pathways in macro and not in variants.", len(macro_pathways))

    source = _columns_as_index(macro_pathways, Constants.SOURCE_MACRO_LEVELS)
    target = _columns_as_index(macro_pathways, Constants.TARGET_MACRO_LEVELS)

    available_pathways = available_pathways.set_index(HR)

    res = _create_combos(available_pathways, source, target)

    res = res.set_index(Constants.MICRO_MACRO_DIFF, append=True)
    res["variant"] = default_variant

    return res


@utils.log
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


def _get_from_index_or_column(frame: pd.DataFrame, name: str):
    if name in frame.columns:
        return frame[name]
    return frame.index.get_level_values(name)


@utils.log
def _probability_of_connection(
    micro: pd.DataFrame, macro, cell_counts: pd.Series, region_volumes
) -> pd.Series:
    """Probability of connection."""
    # truncate macro down to micro index
    macro = _align(macro, micro, Constants.MACRO_LEVELS).set_index(Constants.MACRO_LEVELS)

    scale = _synapse_scaling_factor(macro, micro, cell_counts, region_volumes)

    if not all(scale.index == macro.index):
        raise CWLWorkflowError(
            "Scale and macro indices are not consistent:\n"
            f"scale index: {scale.index}\n"
            f"macro index: {macro.index}"
        )

    micro_scales = scale.get(_columns_as_index(micro, Constants.MACRO_LEVELS), 0.0).values

    probabilities = micro["weight"].values * micro_scales

    return _sanitize_and_clip(probabilities, 0.0, 1.0)


def _sanitize_and_clip(array, minimum, maximum):
    array = array.copy()

    mask = np.isinf(array)

    if mask.any():
        L.debug("Encountered %d inf values. They will be set to zero.", mask.sum())
        array[mask] = 0.0

    mask = np.isnan(array)

    if mask.any():
        L.debug("Encountered %d nan values. They will be set to zero.", mask.sum())
        array[mask] = 0.0

    mask = array < minimum

    if mask.any():
        L.debug(
            "Encountered %d values smaller than %f. They will be clipped to the latter.",
            mask.sum(),
            minimum,
        )

    mask = array > maximum

    if mask.any():
        L.debug(
            "Encountered %d values greater than %f. They will be clipped to the latter.",
            mask.sum(),
            maximum,
        )

    return np.clip(array, minimum, maximum)


@utils.log
def _align(df1: pd.DataFrame, df2: pd.DataFrame, index_columns: list[str]):
    df_indexed_1 = df1.set_index(index_columns)
    df_indexed_2 = df2.set_index(index_columns)

    return df1[df_indexed_1.index.isin(df_indexed_2.index)].reset_index(drop=True)


@utils.log
def _synapse_scaling_factor(macro: pd.DataFrame, micro, cell_counts, region_volumes):
    """Calculate the scaling between macro and micro synapse counts."""
    macro_synapse_counts = _macro_synapse_counts(macro, region_volumes)
    micro_synapse_counts = _micro_synapse_counts(micro, macro, cell_counts)

    res = macro_synapse_counts / micro_synapse_counts

    res.replace(np.inf, 0, inplace=True)

    return res


@utils.log
def _macro_synapse_counts(macro_matrix: pd.DataFrame, brain_region_volumes: pd.Series) -> pd.Series:
    """Macro synapse counts.

    Args:
        macro_matrix: A dataframe with the macro columns.
            - source_hemisphere
            - target_hemisphere
            - source_region
            - target_region
            - value
        brain_region_volumes:
            A Series with index of region acronyms and values of region volumes in um^3

    Returns:
        A Series the index of which is the same as macro_matrix's Index and the values the total
        number of synapses per pathway.
    """
    target_regions = _get_from_index_or_column(macro_matrix, "target_region")

    # use half the volume to account for hemispheres
    postsynaptic_volumes = 0.5 * brain_region_volumes.loc[target_regions]

    # value is number of synapses per cubic micron
    total_synapse_counts = pd.Series(
        data=macro_matrix["value"].values * postsynaptic_volumes.values,
        index=macro_matrix.index,
    )

    return total_synapse_counts.astype(int)


@utils.log
def _micro_synapse_counts(micro, macro, hrm_cell_counts):
    """Return the number of synapses per (H, R) x (H, R) pathway."""
    pre_cell_counts, post_cell_counts = _pre_post_cell_counts(micro, hrm_cell_counts)

    # synapse counts (H, R, M) x (H, R, M)
    synapse_counts = pd.Series(
        data=(
            micro["weight"].values
            * micro["nsynconn_mean"].values
            * pre_cell_counts
            * post_cell_counts
        ),
        index=_columns_as_index(micro, Constants.MICRO_LEVELS),
    ).astype(int)

    # synapse counts (H, R) x (H, R)
    # convert micro to macro by summing over mtype levels
    # Note that observed=True will only list the categories that have values in the frame
    result = synapse_counts.groupby(level=Constants.MACRO_LEVELS, observed=True, sort=False).sum()

    # ensure that there is no mismatch in index categories because of the groupby
    result = result.reindex_like(macro)

    return result


def _split_side_into_hemispheres(df):
    """Replace 'side' column with 'source_hemisphere' and 'target_hemispheres' columns."""
    source_hemisphere = df["side"].str[0].replace("L", "left").replace("R", "right")
    target_hemisphere = df["side"].str[1].replace("L", "left").replace("R", "right")

    df["source_hemisphere"] = pd.Categorical(source_hemisphere, categories=Constants.HEMISPHERES)
    df["target_hemisphere"] = pd.Categorical(target_hemisphere, categories=Constants.HEMISPHERES)

    return df.drop(columns="side")


@utils.log
def _pre_post_cell_counts(
    micro_matrix: pd.DataFrame, hrm_cell_counts: pd.DataFrame
) -> tuple[np.ndarray, np.ndarray]:
    """Calculate the number presynaptic and postsynaptic cells in the micro pathways."""
    presynaptic_counts = hrm_cell_counts.get(
        _columns_as_index(micro_matrix, Constants.SOURCE_MICRO_LEVELS),
        0,
    ).values
    postsynaptic_counts = hrm_cell_counts.get(
        _columns_as_index(micro_matrix, Constants.TARGET_MICRO_LEVELS),
        0,
    ).values
    return presynaptic_counts, postsynaptic_counts
