# SPDX-License-Identifier: Apache-2.0

"""Synapse filtering module."""

import copy
import logging
from pathlib import Path

import click
import fz_td_recipe
import libsonata
import voxcell
from entity_management.simulation import DetailedCircuit
from entity_management.util import get_entity

from blue_cwl import recipes, registering, staging, utils
from blue_cwl.exceptions import CWLWorkflowError
from blue_cwl.typing import StrOrPath
from blue_cwl.wrappers import common

L = logging.getLogger(__name__)


# pylint: disable=unused-argument


INPUT_NODE_POPULATION_COLUMNS = [
    "etype",
    "hemisphere",
    "morph_class",
    "mtype",
    "region",
    "subregion",
    "synapse_class",
    "x",
    "y",
    "z",
    "morphology",
    "orientation_w",
    "orientation_x",
    "orientation_y",
    "orientation_z",
]


@click.group
def app():
    """Synapse filtering."""


@app.command(name="dirs")
@click.option("--output-stage-dir", required=True)
@click.option("--output-build-dir", required=True)
def dirs_cli(**options):
    """Generate output directories."""
    dirs(**options)


def dirs(*, output_stage_dir, output_build_dir):
    """Generate output directories."""
    utils.create_dir(output_stage_dir)
    utils.create_dir(output_build_dir)


@app.command(name="stage")
@click.option(
    "--configuration-id",
    required=True,
    help="Nexus ID of the configuration resource.",
)
@click.option("--circuit-id", required=True, help="DetailedCircuit resource Nexus ID.")
@click.option(
    "--variant-id",
    required=True,
    help="Variant resource Nexus ID.",
)
@click.option("--staging-dir", required=True, help="Directory to write staging data.")
@click.option(
    "--output-configuration-file",
    required=True,
    help="File path to output the staged configuration.",
)
@click.option(
    "--output-circuit-file",
    required=True,
    help="File path to output the staged circuit config.",
)
@click.option(
    "--output-variant-file",
    required=True,
    help="File path to output the staged variant cwl file.",
)
@click.option(
    "--output-atlas-file",
    required=True,
    help="File path to output the staged atlas info file.",
)
@click.option(
    "--output-edges-file",
    required=True,
    help="File path to output the circuit's staged edges.",
)
def stage_cli(**options):
    """Stage the online resources to local data to be used downstream."""
    stage(**options)


def stage(
    *,
    configuration_id: str,
    circuit_id: str,
    variant_id: str,
    staging_dir: StrOrPath,
    output_configuration_file: StrOrPath,
    output_circuit_file: StrOrPath,
    output_atlas_file: StrOrPath,
    output_variant_file: StrOrPath,
    output_edges_file: StrOrPath,
) -> None:
    """Stage the online resources to local data to be used downstream.

    Args:
        configuration_id: Nexus ID of the configuration resource.
        circuit_id: DetailedCircuit resource Nexus ID.
        variant_id: Variant resource Nexus ID.
        staging_dir: Directory to write staging data.
        output_configuration_file: File path to output the staged configuration.
        output_circuit_file: File path to output the staged circuit config.
        output_variant_file: File path to output the staged variant cwl file.
        output_atlas_file: File path to output the staged atlas info file.
        output_edges_file: File path to output the circuit's staged edges.
    """
    staging_dir = utils.create_dir(staging_dir)

    staging.materialize_synapse_config(
        configuration_id,
        output_dir=staging_dir,
        output_file=output_configuration_file,
    )
    L.info("Synapse configuration distribution staged at %s", output_configuration_file)

    staging.stage_variant(variant_id, output_file=Path(output_variant_file))
    L.info("Variant definition staged at %s", output_variant_file)

    _stage_detailed_circuit(
        circuit_id,
        staged_circuit_file=Path(output_circuit_file),
        staged_edges_file=Path(output_edges_file),
    )
    L.info("Detailed circuit staged at %s", output_circuit_file)

    _stage_atlas(circuit_id, staging_dir, output_file=output_atlas_file)
    L.info("Atlas staged at %s", output_atlas_file)


def _stage_detailed_circuit(circuit_id, staged_circuit_file, staged_edges_file):
    staging.stage_detailed_circuit(
        circuit_id,
        output_file=staged_circuit_file,
    )

    circuit_config = utils.load_json(staged_circuit_file)

    edges_file, _ = utils.get_first_edge_population_from_config(circuit_config)

    staging.stage_file(source=edges_file, target=staged_edges_file, symbolic=True)


def _stage_atlas(circuit_id: str, staging_dir: StrOrPath, output_file: StrOrPath):
    atlas_dir = utils.create_dir(Path(staging_dir, "atlas"))

    partial_circuit = get_entity(resource_id=circuit_id, cls=DetailedCircuit)

    staging.stage_atlas(
        partial_circuit.atlasRelease,
        output_dir=atlas_dir,
        output_file=Path(output_file),
    )


@app.command(name="recipe")
@click.option("--atlas-file", required=True)
@click.option("--circuit-file", required=True)
@click.option("--source-node-population-name", required=True)
@click.option("--target-node-population-name", required=True)
@click.option("--configuration-file", required=True)
@click.option("--output-recipe-file", required=True)
def recipe_cli(**options):
    """Generate functionalizer's connectome recipe."""
    recipe(**options)


def recipe(
    *,
    atlas_file: StrOrPath,
    circuit_file: StrOrPath,
    source_node_population_name: str,
    target_node_population_name: str,
    configuration_file: StrOrPath,
    output_recipe_file: StrOrPath,
):
    """Generate functionalizer's connectome recipe."""
    configuration = utils.load_json(configuration_file)["configuration"]
    configuration = {name: utils.load_json(path) for name, path in configuration.items()}

    circuit_config = libsonata.CircuitConfig.from_file(circuit_file)

    source_population = circuit_config.node_population(source_node_population_name)
    target_population = circuit_config.node_population(target_node_population_name)

    atlas_info = staging.AtlasInfo.from_file(atlas_file)

    L.info("Building functionalizer recipe...")
    recipe_file = recipes.write_functionalizer_json_recipe(
        synapse_config=configuration,
        region_map=voxcell.RegionMap.load_json(atlas_info.ontology_path),
        annotation=voxcell.VoxelData.load_nrrd(atlas_info.annotation_path),
        populations=(source_population, target_population),
        output_dir=Path(output_recipe_file).parent,
        output_recipe_filename=Path(output_recipe_file).name,
    )

    # validate recipe
    fz_td_recipe.Recipe(
        recipe_file, circuit_file, (source_node_population_name, target_node_population_name)
    )


@app.command(name="register")
@click.option("--circuit-id", required=True)
@click.option("--edges-file", required=True)
@click.option("--output-dir", required=True)
@click.option("--output-resource-file", required=True)
def register_cli(**options):
    """Register generated circuit with functional connectivity."""
    register(**options)


def register(
    *,
    circuit_id: str,
    edges_file: StrOrPath,
    output_dir: StrOrPath,
    output_resource_file: StrOrPath,
) -> None:
    """Register generated circuit with functional connectivity.

    Args:
        circuit_id: The id of the circuit to append the edges file.
        edges_file:  The edges file to add to the existing circuit.
        output_dir: The output directory to write the generated data.
        output_resource_file: The file path to write the registered in Nexus resource jsonld.
    """
    L.info("Registering partial circuit with functional connectome...")

    partial_circuit = get_entity(resource_id=circuit_id, cls=DetailedCircuit)
    circuit_config_path = partial_circuit.circuitConfigPath.get_url_as_path()
    config = utils.load_json(circuit_config_path)

    output_config_file = Path(output_dir, "circuit_config.json")
    _write_partial_config(config, edges_file, output_config_file)
    L.info("Circuit config written at %s", output_config_file)

    L.info("Registering DetailedCircuit entity...")
    circuit = registering.register_partial_circuit(
        name="Partial circuit with functional connectivity",
        brain_region_id=utils.get_partial_circuit_region_id(partial_circuit),
        atlas_release=partial_circuit.atlasRelease,
        description="Circuit with nodes and functionalized synapses.",
        sonata_config_path=output_config_file,
    )

    common.write_entity_id_to_file(entity=circuit, output_file=output_resource_file)


def _write_partial_config(config: dict, edges_file: StrOrPath, output_file: StrOrPath) -> None:
    config = copy.deepcopy(config)

    edges = config["networks"]["edges"]

    if len(edges) == 0:
        raise CWLWorkflowError(f"Only one edge population is supported. Found: {len(edges)}")

    edges[0]["edges_file"] = str(edges_file)

    utils.write_json(filepath=output_file, data=config)
