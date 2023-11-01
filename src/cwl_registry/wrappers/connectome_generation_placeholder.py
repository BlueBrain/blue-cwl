"""Connectome manipulation wrapper."""
import copy
import logging
import subprocess

import click
import libsonata
import numpy as np
import voxcell
from entity_management.nexus import load_by_id

from cwl_registry import brain_regions, connectome, nexus, recipes, registering, staging, utils
from cwl_registry.exceptions import CWLWorkflowError
from cwl_registry.variant import Variant

L = logging.getLogger(__name__)

# pylint: disable=R0801


@click.command()
@click.option("--configuration", required=True)
@click.option("--partial-circuit", required=True)
@click.option("--macro-connectome-config", required=True)
@click.option("--variant-config", required=True)
@click.option("--output-dir", required=True)
def app(configuration, partial_circuit, macro_connectome_config, variant_config, output_dir):
    """Build micro connectome"""
    output_dir = utils.create_dir(output_dir)
    _app(configuration, partial_circuit, macro_connectome_config, variant_config, output_dir)


def _app(configuration, partial_circuit, macro_connectome_config, variant_config, output_dir):
    forge = nexus.get_forge()

    staging_dir = utils.create_dir(output_dir / "stage")
    build_dir = utils.create_dir(output_dir / "build", clean_if_exists=True)

    circuit_resource = nexus.get_resource(forge, partial_circuit)
    config_path = circuit_resource.circuitConfigPath.url.removeprefix("file://")
    config = utils.load_json(config_path)
    _, node_population_name = utils.get_biophysical_partial_population_from_config(config)

    variant = Variant.from_resource_id(forge, variant_config)

    recipe_file = _create_recipe(
        forge,
        macro_connectome_config,
        configuration,
        partial_circuit,
        circuit_resource.atlasRelease.id,
        staging_dir,
        build_dir,
    )
    L.info("Running connectome manipulator...")
    edge_population_name = f"{node_population_name}__{node_population_name}__chemical"
    edges_file = output_dir / "edges.h5"
    _run_connectome_manipulator(
        recipe_file=recipe_file,
        output_dir=build_dir,
        variant=variant,
        output_edges_file=edges_file,
        output_edge_population_name=edge_population_name,
    )
    L.info("Writing partial circuit config...")
    sonata_config_file = output_dir / "circuit_config.json"
    _write_partial_config(
        config=config,
        edges_file=edges_file,
        population_name=edge_population_name,
        output_file=sonata_config_file,
    )

    forge = nexus.get_forge(force_refresh=True)

    # output circuit
    L.info("Registering partial circuit...")
    circuit_resource = registering.register_partial_circuit(
        name="Partial circuit with connectivity",
        brain_region_id=circuit_resource.brainLocation.brainRegion.id,
        atlas_release_id=circuit_resource.atlasRelease.id,
        description="Partial circuit with cell properties, emodels, morphologies and connectivity.",
        sonata_config_path=sonata_config_file,
    )

    utils.write_resource_to_definition_output(
        json_resource=load_by_id(circuit_resource.get_id()),
        variant=variant,
        output_dir=output_dir,
    )


def _create_recipe(
    forge, macro_config_id, micro_config_id, circuit_id, atlas_release_id, staging_dir, build_dir
):
    L.debug("Materializing macro connectome dataset configuration...")
    macro_config = staging.materialize_macro_connectome_config(
        forge, macro_config_id, output_file=staging_dir / "materialized_macro_config.json"
    )

    L.debug("Materializing micro connectome dataset configuration...")
    micro_config = staging.materialize_micro_connectome_config(
        forge, micro_config_id, output_file=staging_dir / "materialized_micro_config.json"
    )

    L.debug("Assembling macro matrix...")
    macro_matrix = connectome.assemble_macro_matrix(macro_config)

    circuit_resource = nexus.get_resource(forge, circuit_id)
    config_path = circuit_resource.circuitConfigPath.url.removeprefix("file://")
    nodes_file, node_population_name = utils.get_biophysical_partial_population_from_config(
        utils.load_json(config_path)
    )

    population = libsonata.NodeStorage(nodes_file).open_population(node_population_name)

    atlas_info = staging.stage_atlas(forge, atlas_release_id, output_dir=staging_dir / "atlas")

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
    _run_parquet_conversion(parquet_dir, output_edges_file, output_edge_population_name)

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
    str_command = utils.build_variant_allocation_command(str_base_command, variant)

    L.info("Tool full command: %s", str_command)
    subprocess.run(str_command, check=True, shell=True)


def _run_parquet_conversion(parquet_dir, output_edges_file, output_edge_population_name):
    # Launch a second allocation to merge parquet edges into a SONATA edge population
    base_command = [
        "parquet2hdf5",
        str(parquet_dir),
        str(output_edges_file),
        output_edge_population_name,
        "--no-index",
    ]
    str_base_command = " ".join(base_command)

    # TODO: To remove when sub-workflows are supported
    str_command = (
        "salloc "
        "--account=proj134 "
        "--partition=prod "
        "--nodes=100 "
        "--tasks-per-node=10 "
        "--cpus-per-task=4 "
        "--exclusive "
        "--time=8:00:00 "
        f"srun dplace {str_base_command}"
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
