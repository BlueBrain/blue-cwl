"""Synapse filtering module."""

import copy
import logging
import os
import subprocess
from pathlib import Path

import click
import fz_td_recipe
import libsonata
import voxcell
from entity_management.nexus import load_by_id
from entity_management.simulation import DetailedCircuit
from entity_management.util import get_entity

from blue_cwl import Variant, recipes, registering, staging, utils, validation
from blue_cwl.exceptions import CWLWorkflowError
from blue_cwl.typing import StrOrPath

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


@app.command(name="mono-execution")
@click.option(
    "--configuration-id",
    required=True,
    help="Nexus ID of the configuration resource.",
)
@click.option(
    "--variant-id",
    required=False,
    help="Nexus ID of the variant definition resource.",
)
@click.option("--circuit-id", required=True, help="DetailedCircuit resource Nexus ID.")
@click.option("--output-dir", required=True)
def mono_execution_cli(**options):
    """Synapse filtering."""
    mono_execution(**options)


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
    L.info("Registering partial circuit with functioanal connectome...")

    partial_circuit = get_entity(resource_id=circuit_id, cls=DetailedCircuit)
    circuit_config_path = partial_circuit.circuitConfigPath.get_url_as_path()
    config = utils.load_json(circuit_config_path)

    output_config_file = Path(output_dir, "circuit_config.json")
    _write_partial_config(config, edges_file, output_config_file)
    L.info("Circuit config written at %s", output_config_file)

    # output circuit
    L.info("Registering partial circuit...")
    partial_circuit = registering.register_partial_circuit(
        name="Partial circuit with functional connectivity",
        brain_region_id=utils.get_partial_circuit_region_id(partial_circuit),
        atlas_release_id=partial_circuit.atlasRelease.get_id(),
        description="Circuit with nodes and functionalized synapses.",
        sonata_config_path=output_config_file,
    )
    jsonld_resource = load_by_id(partial_circuit.get_id())
    utils.write_json(filepath=output_resource_file, data=jsonld_resource)
    L.info("Circuit jsonld resource written at %s", jsonld_resource)


def mono_execution(
    configuration_id: str, variant_id: str, circuit_id: str, output_dir: StrOrPath
) -> None:
    """Synapse connectome filtering using spykfunc.

    Args:
        configuration_id: The Nexus id of the configuration resource.
        variant_id: The Nexus id of the variant resource.
        circuit_id: The Nexus id of the partial input circuit.
        output_dir: The output directory to write the generated data.
    """
    output_dir = utils.create_dir(Path(output_dir).resolve())

    build_dir = Path(output_dir, "build")
    staging_dir = Path(output_dir, "stage")

    # create directories
    dirs(output_stage_dir=staging_dir, output_build_dir=build_dir)

    variant = get_entity(resource_id=variant_id, cls=Variant)
    partial_circuit = get_entity(resource_id=circuit_id, cls=DetailedCircuit)
    circuit_config_path = partial_circuit.circuitConfigPath.get_url_as_path()
    config = utils.load_json(circuit_config_path)

    nodes_file, node_population_name = utils.get_biophysical_partial_population_from_config(config)
    _, edge_population_name = utils.get_first_edge_population_from_config(config)

    # staged output files
    staged_configuration_file = Path(staging_dir, "staged_configuration_file.json")
    staged_circuit_file = Path(staging_dir, "circuit_config.json")
    staged_atlas_file = Path(staging_dir, "atlas.json")
    staged_variant_file = Path(staging_dir, "variant.cwl")
    staged_edges_file = Path(staging_dir, "edges.h5")

    # parquet2hdf5 output edges file
    output_edges_file = Path(build_dir, "edges.h5")

    # functionalizer's recipe output json file
    recipe_file = Path(build_dir, "recipe.json")

    # functionalizer output parquet dir
    parquet_dir = Path(build_dir, "circuit.parquet")

    # nexus jsonld resource output file
    resource_file = Path(output_dir, "resource.json")

    stage(
        configuration_id=configuration_id,
        variant_id=variant_id,
        circuit_id=circuit_id,
        staging_dir=staging_dir,
        output_configuration_file=staged_configuration_file,
        output_circuit_file=staged_circuit_file,
        output_atlas_file=staged_atlas_file,
        output_variant_file=staged_variant_file,
        output_edges_file=staged_edges_file,
    )
    recipe(
        atlas_file=staged_atlas_file,
        circuit_file=staged_circuit_file,
        source_node_population_name=node_population_name,
        target_node_population_name=node_population_name,
        configuration_file=staged_configuration_file,
        output_recipe_file=recipe_file,
    )
    validation.check_properties_in_population(
        population_name=node_population_name,
        nodes_file=nodes_file,
        property_names=INPUT_NODE_POPULATION_COLUMNS,
    )
    _run_functionalizer(
        circuit_config_path=circuit_config_path,
        node_population_name=node_population_name,
        edges_file=staged_edges_file,
        recipe_file=recipe_file,
        output_dir=build_dir,
        parquet_dir=parquet_dir,
        variant=variant,
    )
    _run_parquet_conversion(
        parquet_dir=parquet_dir,
        output_edges_file=output_edges_file,
        output_edge_population_name=edge_population_name,
        variant=variant,
    )
    register(
        circuit_id=circuit_id,
        edges_file=output_edges_file,
        output_dir=build_dir,
        output_resource_file=resource_file,
    )


def _run_functionalizer(
    *,
    circuit_config_path: StrOrPath,
    node_population_name: str,
    edges_file: StrOrPath,
    recipe_file: StrOrPath,
    output_dir: StrOrPath,
    parquet_dir: StrOrPath,
    variant: Variant,
) -> None:
    L.info("Running functionalizer...")

    work_dir = utils.create_dir(Path(output_dir, "workdir"), clean_if_exists=True)

    base_command = [
        "env",
        f"SPARK_USER={os.environ['USER']}",
        "dplace",
        "functionalizer",
        str(edges_file),
        "--circuit-config",
        str(circuit_config_path),
        "--work-dir",
        str(work_dir),
        "--output-dir",
        str(output_dir),
        "--from",
        node_population_name,
        "--to",
        node_population_name,
        "--filters",
        "SynapseProperties",
        "--recipe",
        str(recipe_file),
    ]
    str_base_command = " ".join(base_command)
    str_command = utils.build_variant_allocation_command(
        str_base_command, variant, sub_task_index=0
    )

    L.info("Tool full command: %s", str_command)
    subprocess.run(str_command, check=True, shell=True)

    if not Path(parquet_dir).exists():
        raise CWLWorkflowError(f"Parquet dir at {parquet_dir} does not exist.")

    L.info("Parquet files generated in %s", parquet_dir)


def _run_parquet_conversion(
    parquet_dir: StrOrPath, output_edges_file: StrOrPath, output_edge_population_name, variant
) -> None:
    L.info("Running parquet conversion to sonata...")

    # Launch a second allocation to merge parquet edges into a SONATA edge population
    base_command = [
        "parquet2hdf5",
        str(parquet_dir),
        str(output_edges_file),
        output_edge_population_name,
    ]
    str_base_command = " ".join(base_command)

    str_command = utils.build_variant_allocation_command(
        str_base_command, variant, sub_task_index=1, srun="srun dplace"
    )

    L.info("Tool full command: %s", str_command)
    subprocess.run(str_command, check=True, shell=True)

    if not Path(output_edges_file).exists():
        raise CWLWorkflowError(f"Edges file has failed to be generated at {output_edges_file}")

    L.info("Functionalized edges generated at %s", output_edges_file)


def _write_partial_config(config: dict, edges_file: StrOrPath, output_file: StrOrPath) -> None:
    config = copy.deepcopy(config)

    edges = config["networks"]["edges"]

    if len(edges) == 0:
        raise CWLWorkflowError(f"Only one edge population is supported. Found: {len(edges)}")

    edges[0]["edges_file"] = str(edges_file)

    utils.write_json(filepath=output_file, data=config)
