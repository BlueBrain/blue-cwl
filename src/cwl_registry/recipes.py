"""Construction of recipes for circuit building."""

import itertools
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
import voxcell

from cwl_registry import utils
from cwl_registry.exceptions import CWLWorkflowError
from cwl_registry.typing import StrOrPath

if TYPE_CHECKING:
    from libsonata import NodePopulation


L = logging.getLogger(__name__)


def build_cell_composition_from_me_densities(
    region: str, me_type_densities: pd.DataFrame
) -> dict[str, Any]:
    """Create cell composition file from KG me densities."""
    return {
        "version": "v2",
        "neurons": [
            {
                "density": row.path,
                "region": region,
                "traits": {
                    "mtype": row.mtype,
                    "etype": row.etype,
                },
            }
            for row in me_type_densities.itertuples(index=False)
        ],
    }


def build_mtype_taxonomy(mtypes: list[str]) -> pd.DataFrame:
    """A temporary solution in creating a taxonomy for circuit-build."""
    tokens = {
        "DAC": ("INT", "INH"),
        "HAC": ("INT", "INH"),
        "LAC": ("INT", "INH"),
        "NGC-DA": ("INT", "INH"),
        "NGC-SA": ("INT", "INH"),
        "SAC": ("INT", "INH"),
        "BP": ("INT", "INH"),
        "BTC": ("INT", "INH"),
        "ChC": ("INT", "INH"),
        "DBC": ("INT", "INH"),
        "LBC": ("INT", "INH"),
        "MC": ("INT", "INH"),
        "NBC": ("INT", "INH"),
        "NGC": ("INT", "INH"),
        "SBC": ("INT", "INH"),
        "GIN_mtype": ("INT", "INH"),
        "TPC": ("PYR", "EXC"),
        "TPC:A": ("PYR", "EXC"),
        "TPC:B": ("PYR", "EXC"),
        "TPC:C": ("PYR", "EXC"),
        "UPC": ("PYR", "EXC"),
        "BPC": ("PYR", "EXC"),
        "IPC": ("PYR", "EXC"),
        "SSC": ("INT", "EXC"),
        "HPC": ("PYR", "EXC"),
        "GEN_mtype": ("PYR", "EXC"),
    }
    pattern = r"^(L\d+_)?([\w-]+:?\w)$"
    reg = re.compile(pattern)

    m_classes = []
    s_classes = []
    not_found = []
    for mtype in mtypes:
        match = reg.match(mtype)
        if match:
            mclass, sclass = tokens[match.groups()[-1]]
            m_classes.append(mclass)
            s_classes.append(sclass)
        else:
            not_found.append(mtype)

    if not_found:
        raise CWLWorkflowError(f"mtypes not in taxonomy definition: {not_found}")

    df = pd.DataFrame(
        {
            "mtype": mtypes,
            "mClass": m_classes,
            "sClass": s_classes,
        }
    )
    return df


def build_connectome_manipulator_recipe(
    circuit_config_path: str,
    micro_matrices: dict[str, pd.DataFrame],
    output_dir: StrOrPath,
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
    output_file = Path(output_dir, "pathways.parquet")

    utils.write_parquet(filepath=output_file, dataframe=merged_frame, index=True, compression=None)

    config = {
        "circuit_config": str(circuit_config_path),
        "seed": 0,
        "N_split_nodes": 1000,
        "manip": {
            "name": "WholeBrainMacroMicroWiring",
            "fcts": [
                {
                    "source": "conn_wiring",
                    "morph_ext": "h5",
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


def build_connectome_distance_dependent_recipe(config_path, configuration, output_dir, morph_ext):
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
                    "morph_ext": morph_ext,
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


def write_functionalizer_json_recipe(
    synapse_config: dict,
    region_map: voxcell.RegionMap,
    annotation: voxcell.VoxelData,
    output_dir: Path,
    output_recipe_filename: str,
    populations: tuple["NodePopulation", "NodePopulation"] | None = None,
):
    """Build functionalizer json recipe.

    Args:
        synapse_config: config for the recipe.
        region_map: voxcell.regionmap
        annotation: Brain regions annotation
        output_dir: output directory to output recipe dataframes
        output_recipe_filename: filename for the json recipe
        populations: a tuple of the source and target node populations.
    """
    synapse_properties_generator = _generate_tailored_properties(
        synapse_properties=synapse_config["synapse_properties"],
        region_map=region_map,
        annotation=annotation,
        populations=populations,
    )
    output_file = _write_json_recipe(
        synapse_properties_generator,
        synapse_config["synapses_classification"],
        populations=populations,
        output_dir=output_dir,
        output_recipe_filename=output_recipe_filename,
    )
    # validate the recipe
    return output_file


def _write_json_recipe(
    synapse_properties, synapse_classification, populations, output_dir, output_recipe_filename
):
    synapse_rules_file = output_dir / "synapse_rules.parquet"

    L.info("Writing synapse rules parquet...")
    synapse_properties = _write_synapse_properties_parquet(
        synapse_properties,
        populations=populations,
        output_file=synapse_rules_file,
    )
    synapse_classes = _build_synapse_classes(synapse_properties, synapse_classification)

    recipe = {
        "version": 1,
        "bouton_interval": {
            "min_distance": 5.0,
            "max_distance": 7.0,
            "region_gap": 5.0,
        },
        "bouton_distances": {
            "inhibitory_synapse_distance": 5.0,
            "excitatory_synapse_distance": 25.0,
        },
        "synapse_properties": {
            "rules": str(synapse_rules_file),
            "classes": synapse_classes,
        },
    }
    output_file = output_dir / output_recipe_filename
    utils.write_json(data=recipe, filepath=output_file)
    return output_file


def _build_synapse_classes(synapse_properties, synapse_classification):
    rename_mapping = {
        "gsyn": "conductance_mu",
        "gsynsd": "conductance_sd",
        "d": "depression_time_mu",
        "dsd": "depression_time_sd",
        "f": "facilitation_time_mu",
        "fsd": "facilitation_time_sd",
        "u": "u_syn_mu",
        "usd": "u_syn_sd",
        "dtc": "decay_time_mu",
        "dtcsd": "decay_time_sd",
        "gsynsrsf": "conductance_scale_factor",
        "uhillcoefficient": "u_hill_coefficient",
        "nrrp": "n_rrp_vesicles_mu",
    }
    synaptic_types = sorted(pd.unique(synapse_properties["class"]))

    synapse_classes = []
    for synaptic_type in synaptic_types:
        sclass = {"class": synaptic_type}
        for key, value in synapse_classification[synaptic_type].items():
            sclass[rename_mapping.get(key.lower(), key)] = value
        synapse_classes.append(sclass)

    return synapse_classes


def _write_synapse_properties_parquet(synapse_properties, populations, output_file):
    columns_dtypes = {
        "src_hemisphere_i": np.int8,
        "src_region_i": np.int16,
        "src_mtype_i": np.int16,
        "src_etype_i": np.int16,
        "dst_hemisphere_i": np.int8,
        "dst_region_i": np.int16,
        "dst_mtype_i": np.int16,
        "dst_etype_i": np.int16,
        "class": "category",
        "model": "category",
    }
    L.info("Constructing synapse rules dataframe...")
    result = pd.DataFrame(
        [
            (
                prop.get("fromHemisphere", "*"),
                prop.get("fromRegion", "*"),
                prop.get("fromMType", "*"),
                prop.get("fromEType", "*"),
                prop.get("toHemisphere", "*"),
                prop.get("toRegion", "*"),
                prop.get("toMType", "*"),
                prop.get("toEType", "*"),
                prop["synapticType"],
                prop["synapticModel"],
            )
            for prop in synapse_properties
        ],
        columns=list(columns_dtypes),
    )

    L.info("Mapping string values to circuit @library indices...")
    _map_str_rows_to_int_library_positions(result, populations)

    for column_name, column_dtype in columns_dtypes.items():
        result[column_name] = result[column_name].astype(column_dtype)

    result["neural_transmitter_release_delay"] = np.full(
        fill_value=0.1, dtype=np.float32, shape=len(result)
    )
    result["axonal_conduction_velocity"] = np.full(
        fill_value=300.0, dtype=np.float32, shape=len(result)
    )

    result.to_parquet(path=output_file)
    L.info("Synapse rules written at %s", output_file)

    return result


def _map_str_rows_to_int_library_positions(result, populations):
    names = ["hemisphere", "region", "mtype", "etype"]

    # create mapping between circuit @library enumeration keys and int positions
    # e.g {'hemisphere': {'left': 0, 'right': 1, '*': -1}
    src_mapping = _create_enumeration_inverse_mapping(populations[0], names)
    dst_mapping = _create_enumeration_inverse_mapping(populations[1], names)

    missing = {}
    # map dataframe string names to integer enumerations using the [src|dst]_mapping
    for name in names:
        source_key = f"src_{name}_i"
        target_key = f"dst_{name}_i"

        result[source_key], src_missing = _map_string_to_int_values(
            result[source_key], src_mapping[name]
        )
        result[target_key], dst_missing = _map_string_to_int_values(
            result[target_key], dst_mapping[name]
        )

        missing_entries = src_missing | dst_missing

        if missing_entries:
            missing[name] = missing_entries

    if missing:
        raise CWLWorkflowError(f"Missing entries from circuit @library: {missing}")

    return result


def _map_string_to_int_values(string_series, integer_mapping):
    int_series = string_series.map(integer_mapping)

    na_mask = pd.isna(int_series)

    if na_mask.any():
        missing_entries = set(string_series[na_mask])
    else:
        missing_entries = {}

    return int_series, missing_entries


def _create_enumeration_inverse_mapping(population, enumeration_names):
    def _inv_map(values):
        res = {value: i for i, value in enumerate(values)}
        res["*"] = -1
        return res

    return {name: _inv_map(population.enumeration_values(name)) for name in enumeration_names}


def _generate_tailored_properties(
    synapse_properties: dict,
    region_map: voxcell.RegionMap,
    annotation: voxcell.VoxelData,
    populations: tuple["NodePopulation", "NodePopulation"] | None = None,
) -> pd.DataFrame:
    """Generate properties tailored to a circuit if its pathways are passed."""
    annotation_ids = set(pd.unique(annotation.raw.flatten()))

    available = {}
    available_source_regions = None
    available_target_regions = None
    if populations:
        mapping = {
            "hemisphere": ("fromHemisphere", "toHemisphere"),
            "synapse_class": ("fromSClass", "toSClass"),
            "mtype": ("fromMType", "toMType"),
            "etype": ("fromEType", "toEType"),
        }

        for name, (from_name, to_name) in mapping.items():
            available[from_name] = set(populations[0].enumeration_values(name))
            available[to_name] = set(populations[1].enumeration_values(name))

        available_source_regions = {
            rid
            for acronym in populations[0].enumeration_values("region")
            for rid in region_map.find(acronym, attr="acronym", with_descendants=True)
        } | {None}
        available_target_regions = {
            rid
            for acronym in populations[1].enumeration_values("region")
            for rid in region_map.find(acronym, attr="acronym", with_descendants=True)
        } | {None}

    # slice based on other properties that are already expanded such as mtype, etype, sclass, etc.
    df_properties = pd.DataFrame(synapse_properties, dtype=object).replace({np.nan: None})
    for name, available_values in available.items():
        if name in df_properties.columns:
            col = df_properties.get(name)
            df_properties = df_properties.loc[pd.isna(col) | col.isin(available_values)]

    cache: dict[str, set[int]] = {}
    for prop in df_properties.to_dict(orient="records"):
        yield from _expand_properties(
            prop,
            region_map,
            annotation_ids,
            include_null=False,
            cache=cache,
            allowed_from_region_leaves=available_source_regions,
            allowed_to_region_leaves=available_target_regions,
        )


def _expand_properties(
    prop: dict,
    region_map: voxcell.RegionMap,
    annotation_ids: set[int],
    include_null=True,
    cache: dict[str, set[int]] | None = None,
    allowed_from_region_leaves: set | None = None,
    allowed_to_region_leaves: set | None = None,
) -> list:
    """Return a list of properties matching the leaf regions.

    Args:
        prop : Synapse property rule that needs to be expanded.
        region_map : brain region map.
        annotation_ids: A set of annotation ids.
        include_null : if True, retain the attributes even if they are null. Else remove them
        cache: Optional cache to reuse elements.
        allowed_from_region_leaves: Optional set of allowed from region leaves.
        allowed_to_region_leaves: Optional test of allowed to region leaves.

    Returns:
        A list of synapse properties at the leaf region level.
    """
    exp_props = []

    source_region = prop.get("fromRegion", None)
    target_region = prop.get("toRegion", None)

    from_leaf_regions: set[int] | set[None] = {None}
    if source_region:
        from_leaf_regions = _get_leaf_regions(source_region, region_map, annotation_ids, cache)

        if allowed_from_region_leaves:
            from_leaf_regions &= allowed_from_region_leaves

    to_leaf_regions: set[int] | set[None] = {None}
    if target_region:
        to_leaf_regions = _get_leaf_regions(target_region, region_map, annotation_ids, cache)

        if allowed_to_region_leaves:
            to_leaf_regions &= allowed_to_region_leaves

    for from_reg, to_reg in itertools.product(from_leaf_regions, to_leaf_regions):
        # Construct the individual dicts for each leaf region pairs

        from_reg = region_map.get(from_reg, "acronym") if from_reg is not None else None

        to_reg = region_map.get(to_reg, "acronym") if to_reg is not None else None

        prop_up = prop | {"fromRegion": from_reg, "toRegion": to_reg}

        if not include_null:
            exp_props.append({k: v for k, v in prop_up.items() if bool(v)})
        else:
            exp_props.append(prop_up)

    return exp_props


def _get_leaf_regions(
    acronym: str,
    region_map: voxcell.RegionMap,
    annotation_ids: set[int] | None = None,
    cache: dict[str, set[int]] | None = None,
) -> set[int]:
    """Get leaf regions as a list.

    Args:
        acronym: The region acronym
        region_map: The voxcell RegionMap
        annotation_ids: Optional ids to intersect with the leaf ids
        cache: Optional cache to speed up already visited entries.
    """
    if cache and acronym in cache:
        return cache[acronym]

    ids = region_map.find(acronym, attr="acronym", with_descendants=True)

    result = {rid for rid in ids if region_map.is_leaf_id(rid)}

    if annotation_ids:
        result &= annotation_ids

    return result
