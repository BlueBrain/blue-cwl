"""Connectome manipulation wrapper."""

import copy
import logging
import subprocess

import click
import libsonata
import numpy as np
import voxcell
from entity_management.nexus import load_by_id
from entity_management.simulation import DetailedCircuit
from entity_management.util import get_entity

from blue_cwl import brain_regions, connectome, recipes, registering, staging, utils, validation
from blue_cwl.exceptions import CWLWorkflowError
from blue_cwl.variant import Variant

L = logging.getLogger(__name__)

# pylint: disable=R0801

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


@click.group()
def app():
    """Placeholder micro connectome generation."""


@app.command(name="mono-execution")
@click.option("--configuration-id", required=True)
@click.option("--circuit-id", required=True)
@click.option("--macro-connectome-config-id", required=True)
@click.option("--variant-id", required=True)
@click.option("--output-dir", required=True)
def mono_execution(
    configuration_id, circuit_id, macro_connectome_config_id, variant_id, output_dir
):
    """Build micro connectome."""
    output_dir = utils.create_dir(output_dir)
    _app(configuration_id, circuit_id, macro_connectome_config_id, variant_id, output_dir)


def _app(configuration, partial_circuit, macro_connectome_config, variant_config, output_dir):
    staging_dir = utils.create_dir(output_dir / "stage")
    build_dir = utils.create_dir(output_dir / "build", clean_if_exists=True)

    input_circuit_entity = get_entity(
        resource_id=partial_circuit,
        cls=DetailedCircuit,
    )
    input_circuit_config = utils.load_json(
        filepath=input_circuit_entity.circuitConfigPath.get_url_as_path(),
    )
    input_nodes_file, node_population_name = utils.get_biophysical_partial_population_from_config(
        input_circuit_config
    )
    validation.check_properties_in_population(
        node_population_name, input_nodes_file, INPUT_NODE_POPULATION_COLUMNS
    )

    variant = get_entity(resource_id=variant_config, cls=Variant)

    recipe_file = _create_recipe(
        macro_config_id=macro_connectome_config,
        micro_config_id=configuration,
        circuit_id=partial_circuit,
        atlas_release_id=input_circuit_entity.atlasRelease.get_id(),
        staging_dir=staging_dir,
        build_dir=build_dir,
    )
    L.info("Running connectome manipulator...")
    edge_population_name = f"{node_population_name}__{node_population_name}__chemical"
    edges_file = build_dir / "edges.h5"
    _run_connectome_manipulator(
        recipe_file=recipe_file,
        output_dir=build_dir,
        variant=variant,
        output_edges_file=edges_file,
        output_edge_population_name=edge_population_name,
    )
    L.info("Writing partial circuit config...")
    sonata_config_file = build_dir / "circuit_config.json"
    _write_partial_config(
        config=input_circuit_config,
        edges_file=edges_file,
        population_name=edge_population_name,
        output_file=sonata_config_file,
    )

    partial_circuit = get_entity(resource_id=partial_circuit, cls=DetailedCircuit)

    # output circuit
    L.info("Registering partial circuit...")
    partial_circuit = registering.register_partial_circuit(
        name="Partial circuit with connectivity",
        brain_region_id=utils.get_partial_circuit_region_id(partial_circuit),
        atlas_release_id=partial_circuit.atlasRelease.get_id(),
        description="Partial circuit with cell properties, emodels, morphologies and connectivity.",
        sonata_config_path=sonata_config_file,
    )

    utils.write_resource_to_definition_output(
        json_resource=load_by_id(partial_circuit.get_id()),
        variant=variant,
        output_dir=output_dir,
    )


def _create_recipe(
    macro_config_id, micro_config_id, circuit_id, atlas_release_id, staging_dir, build_dir
):
    L.debug("Materializing macro connectome dataset configuration...")
    macro_config = staging.materialize_macro_connectome_config(
        macro_config_id, output_file=staging_dir / "materialized_macro_config.json"
    )

    L.debug("Materializing micro connectome dataset configuration...")
    micro_config = staging.materialize_micro_connectome_config(
        micro_config_id, output_file=staging_dir / "materialized_micro_config.json"
    )

    L.debug("Assembling macro matrix...")
    macro_matrix = connectome.assemble_macro_matrix(macro_config)

    config_path = get_entity(
        resource_id=circuit_id, cls=DetailedCircuit
    ).circuitConfigPath.get_url_as_path()

    nodes_file, node_population_name = utils.get_biophysical_partial_population_from_config(
        utils.load_json(config_path)
    )

    population = libsonata.NodeStorage(nodes_file).open_population(node_population_name)

    atlas_info = staging.stage_atlas(atlas_release_id, output_dir=staging_dir / "atlas")

    regions = np.unique(population.get_attribute("region", population.select_all())).tolist()
    region_volumes = brain_regions.volumes(
        voxcell.RegionMap.load_json(atlas_info.ontology_path), regions
    )

    L.debug("Assembling micro datasets...")
    micro_matrices = connectome.resolve_micro_matrices(
        micro_config=micro_config,
        macro_matrix=macro_matrix,
        population=population,
        region_volumes=region_volumes,
    )

    L.debug("Generating connectome recipe...")
    recipe_file = build_dir / "manipulation-config.json"
    recipe = recipes.build_connectome_manipulator_recipe(config_path, micro_matrices, build_dir)
    utils.write_json(data=recipe, filepath=recipe_file)

    return recipe_file


def _run_connectome_manipulator(
    recipe_file, output_dir, variant, output_edges_file, output_edge_population_name
):
    """Run connectome manipulator."""
    _run_manipulator(recipe_file, output_dir, variant)

    parquet_dir = output_dir / "parquet"

    # Launch a second allocation to merge parquet edges into a SONATA edge population
    _run_parquet_conversion(parquet_dir, output_edges_file, output_edge_population_name, variant)

    L.debug("Edge population %s generated at %s", output_edge_population_name, output_edges_file)


def _run_manipulator(recipe_file, output_dir, variant):
    base_command = [
        "parallel-manipulator",
        "-v",
        "manipulate-connectome",
        "--output-dir",
        str(output_dir),
        str(recipe_file),
        "--parallel",
        "--keep-parquet",
        "--resume",
    ]
    str_base_command = " ".join(base_command)
    str_command = utils.build_variant_allocation_command(
        str_base_command, variant, sub_task_index=0
    )

    L.info("Tool full command: %s", str_command)
    subprocess.run(str_command, check=True, shell=True)


def _run_parquet_conversion(parquet_dir, output_edges_file, output_edge_population_name, variant):
    # Launch a second allocation to merge parquet edges into a SONATA edge population
    base_command = [
        "parquet2hdf5",
        str(parquet_dir),
        str(output_edges_file),
        output_edge_population_name,
        "--no-index",
    ]
    str_base_command = " ".join(base_command)

    str_command = utils.build_variant_allocation_command(
        str_base_command, variant, sub_task_index=1, srun="srun dplace"
    )

    L.info("Tool full command: %s", str_command)
    subprocess.run(str_command, check=True, shell=True)

    if not output_edges_file.exists():
        raise CWLWorkflowError(f"Edges file has failed to be generated at {output_edges_file}")


def _write_partial_config(config, edges_file, population_name, output_file):
    """Update partial config with new nodes path and the morphology directory."""
    config = copy.deepcopy(config)
    config["networks"]["edges"] = [
        {
            "edges_file": str(edges_file),
            "populations": {population_name: {"type": "chemical"}},
        }
    ]
    utils.write_json(filepath=output_file, data=config)
