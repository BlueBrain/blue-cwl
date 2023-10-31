"""Construction of recipes for circuit building."""
import importlib.resources
import itertools
import re
import shutil
from pathlib import Path
from typing import Any, Dict, List

import lxml.etree as ET
import numpy as np
import pandas as pd
import voxcell

from cwl_registry import utils
from cwl_registry.exceptions import CWLWorkflowError


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


def write_functionalizer_xml_recipe(
    synapse_config: dict,
    region_map: voxcell.RegionMap,
    annotation: voxcell.VoxelData,
    output_file: Path,
    circuit_pathways: pd.DataFrame | None = None,
):
    """Build functionalizer xml recipe.

    Args:
        synapse_config: Config for the recipe.
        region_map: voxcell.RegionMap
        output_file: Output file to write the xml recipe.
        circuit_pathways: Dataframe with the following columns
            - source_hemisphere
            - target_hemisphere
            - source_region
            - target_region
            - source_mtype
            - target_mtype
            - source_etype
            - target_etype
            - source_synaptic_class
            - target_synaptic_class
    """
    synapse_properties_generator = _generate_tailored_properties(
        synapse_properties=synapse_config["synapse_properties"],
        region_map=region_map,
        annotation=annotation,
        pathways=circuit_pathways,
    )

    _write_xml_tree(
        synapse_properties_generator, synapse_config["synapses_classification"], output_file
    )

    return output_file


def _write_xml_tree(synapse_properties, synapse_classification, output_file):
    tree = _build_xml_tree(synapse_properties, synapse_classification)
    with open(output_file, "wb") as fp:
        tree.write(fp, pretty_print=True, xml_declaration=True, encoding="utf-8")


def write_default_functionalizer_xml_recipe(output_file):
    """Copy an existing xml recipe."""
    path = importlib.resources.files("cwl_registry") / "data" / "builderRecipeAllPathways.xml"

    shutil.copyfile(path, output_file)

    return output_file


def _generate_tailored_properties(
    synapse_properties: dict,
    region_map: voxcell.RegionMap,
    annotation: voxcell.VoxelData,
    pathways: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Generate properties tailored to a circuit if its pathways are passed."""
    if pathways is not None:
        available = {
            "fromSClass": set(pathways["source_synapse_class"]),
            "toSClass": set(pathways["target_synapse_class"]),
            "fromHemisphere": set(pathways["source_hemisphere"]),
            "toHemisphere": set(pathways["target_hemisphere"]),
            "fromRegion": set(pathways["source_region"]),
            "toRegion": set(pathways["target_region"]),
            "fromMType": set(pathways["source_mtype"]),
            "toMType": set(pathways["target_mtype"]),
            "fromEType": set(pathways["source_etype"]),
            "toEType": set(pathways["target_etype"]),
        }

        def in_pathways(entry: dict) -> bool:
            """Check that a synapse entry is in the circuit pathways.

            Note: Missing or None entries will not be filtered out.
            """
            for prop_name, available_values in available.items():
                value = entry.get(prop_name, None)
                if value is not None and value not in available_values:
                    return False
            return True

    else:

        def in_pathways(_: dict) -> bool:
            return True

    annotation_ids = set(annotation.raw.flatten())

    cache = {}
    entries = (
        entry
        for prop in synapse_properties
        for entry in _expand_properties(
            prop, region_map, annotation_ids, include_null=False, cache=cache
        )
        if in_pathways(entry)
    )
    yield from entries


def _expand_properties(prop, region_map, annotation_ids: set[int], include_null=True, cache=None):
    """Return a list of properties matching the leaf regions.

    Args:
        prop : Synapse property rule that needs to be expanded
        region_map : brain region map
        incldue_null : if True, retain the attributes even if they are null. Else remove them

    Returns:
        A list of synapse properties at the leaf region level.
    """
    exp_props = []

    source_region = prop.get("fromRegion", None)
    target_region = prop.get("toRegion", None)

    if source_region:
        from_leaf_regions = _get_leaf_regions(source_region, region_map, annotation_ids, cache)
    else:
        from_leaf_regions = [None]

    if target_region:
        to_leaf_regions = _get_leaf_regions(target_region, region_map, annotation_ids, cache)
    else:
        to_leaf_regions = [None]

    for from_reg, to_reg in itertools.product(from_leaf_regions, to_leaf_regions):
        # Construct the individual dicts for each leaf region pairs

        from_reg = region_map.get(from_reg, "acronym") if from_reg is not None else None

        to_reg = region_map.get(to_reg, "acronym") if to_reg is not None else None

        prop_up = dict(prop, fromRegion=from_reg, toRegion=to_reg)

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

    ids = region_map.find(acronym, "acronym", with_descendants=True)

    result = {rid for rid in ids if region_map.is_leaf_id(rid)}

    if annotation_ids:
        result &= annotation_ids

    return result


def _build_xml_tree(synapse_properties, synapses_classification):
    """Concatenate the recipes and return the resulting xml tree.

    Args:
        synapse_properties(list): ordered list of synapse properties
        synapses_classification(dict): synapse classifications
        region_map (voxcell.RegionMap) : brain region hierarchy map

    Returns:
        ET.ElementTree: the resulting xml tree.
    """
    root = ET.Element("blueColumn")
    root.addprevious(ET.Comment("Generated by SBO Workflow."))

    root.append(
        ET.Element(
            "InterBoutonInterval",
            attrib={"minDistance": "5.0", "maxDistance": "7.0", "regionGap": "5.0"},
        )
    )
    root.append(
        ET.Element(
            "InitialBoutonDistance",
            attrib={"defaultInhSynapsesDistance": "5.0", "defaultExcSynapsesDistance": "25.0"},
        )
    )

    properties = ET.Element(
        "SynapsesProperties",
        attrib={
            "neuralTransmitterReleaseDelay": "0.1",
            "axonalConductionVelocity": "300.0",
        },
    )

    synaptic_types = set()
    for prop in synapse_properties:
        xml_prop = {k: (str(v) if bool(v) else "") for k, v in prop.items()}

        # Remove the synaptic model info as it is not used by functionalizer
        del xml_prop["synapticModel"]

        synaptic_type = xml_prop.pop("synapticType")
        xml_prop["type"] = synaptic_type
        synaptic_types.add(synaptic_type)

        properties.append(ET.Element("synapse", attrib=xml_prop))

    root.append(properties)

    classes = ET.Element("SynapsesClassification")
    for synaptic_type in synaptic_types:
        class_data = {k: str(v) for k, v in synapses_classification[synaptic_type].items()}
        classes.append(ET.Element("class", attrib={"id": synaptic_type, **class_data}))

    root.append(classes)

    empty_names = [
        "SynapsesReposition",
        "NeuronTypes",
        "ConnectionRules",
        "SpineLengths",
        "TouchReduction",
        "TouchRules",
    ]
    for name in empty_names:
        root.append(ET.Element(name))

    return ET.ElementTree(root)
