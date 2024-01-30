"""Synapse filtering module."""
import copy
import logging
import os
import subprocess
from pathlib import Path

import click
import voxcell
from entity_management.nexus import load_by_id
from entity_management.simulation import DetailedCircuit

from cwl_registry import Variant, nexus, recipes, registering, staging, utils, validation
from cwl_registry.exceptions import CWLWorkflowError

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


@click.command()
@click.option("--configuration", required=True)
@click.option("--variant-config", required=False)
@click.option("--partial-circuit", required=True)
@click.option("--output-dir", required=True)
def app(configuration, variant_config, partial_circuit, output_dir):
    """Synapse filtering."""
    connectome_filtering_synapses(configuration, variant_config, partial_circuit, output_dir)


def connectome_filtering_synapses(
    configuration: str, variant_config: str, partial_circuit: str, output_dir: os.PathLike
):
    """Synapse filtering."""
    output_dir = utils.create_dir(Path(output_dir).resolve())
    staging_dir = utils.create_dir(output_dir / "stage")
    build_dir = utils.create_dir(output_dir / "build")

    variant = nexus.get_entity(variant_config, cls=Variant)

    partial_circuit = nexus.get_entity(partial_circuit, cls=DetailedCircuit)
    config = utils.load_json(partial_circuit.circuitConfigPath.get_url_as_path())

    nodes_file, node_population_name = utils.get_biophysical_partial_population_from_config(config)
    validation.check_properties_in_population(
        node_population_name, nodes_file, INPUT_NODE_POPULATION_COLUMNS
    )

    validation.check_properties_in_population(
        population_name=node_population_name,
        nodes_file=nodes_file,
        property_names=["hemisphere", "region", "mtype", "etype", "synapse_class"],
    )

    edges_file, edge_population_name = utils.get_first_edge_population_from_config(config)

    morphologies_dir = utils.get_morphologies_dir(config, node_population_name, "h5")

    atlas_dir = utils.create_dir(staging_dir / "atlas")
    L.info("Staging atlas to  %s", atlas_dir)
    atlas_info = staging.stage_atlas(
        partial_circuit.atlasRelease,
        output_dir=atlas_dir,
    )

    L.info("Staging configuration...")
    staging.stage_distribution_file(
        configuration,
        output_dir=staging_dir,
        filename="synapse_config.json",
    )
    configuration = staging.materialize_synapse_config(
        configuration, staging_dir, output_file=staging_dir / "materialized_synapse_config.json"
    )["configuration"]

    if configuration:
        configuration = {name: utils.load_json(path) for name, path in configuration.items()}

        pop = voxcell.CellCollection.load_sonata(nodes_file)

        L.info("Building functionalizer xml recipe...")
        recipe_file = recipes.write_functionalizer_xml_recipe(
            synapse_config=configuration,
            region_map=voxcell.RegionMap.load_json(atlas_info.ontology_path),
            annotation=voxcell.VoxelData.load_nrrd(atlas_info.annotation_path),
            populations=(pop, pop),
            output_file=build_dir / "recipe.xml",
        )
    else:
        L.warning(
            "Empty placeholder SynapseConfig was encountered. "
            "A default xml recipe will be created for backwards compatibility."
        )
        recipe_file = recipes.write_default_functionalizer_xml_recipe(
            output_file=build_dir / "recipe.xml"
        )

    L.info("Running functionalizer...")
    _run_functionalizer(
        nodes_file,
        node_population_name,
        edges_file,
        edge_population_name,
        recipe_file,
        morphologies_dir,
        build_dir,
        variant,
    )
    parquet_dir = build_dir / "circuit.parquet"
    assert parquet_dir.exists()

    L.info("Parquet files generated in %s", parquet_dir)

    output_edges_file = build_dir / "edges.h5"

    L.info("Running parquet conversion to sonata...")

    _run_parquet_conversion(parquet_dir, output_edges_file, edge_population_name, variant)

    L.info("Functionalized edges generated at %s", output_edges_file)

    output_config_file = build_dir / "circuit_config.json"
    _write_partial_config(config, output_edges_file, output_config_file)

    # output circuit
    L.info("Registering partial circuit...")
    partial_circuit = registering.register_partial_circuit(
        name="Partial circuit with functional connectivity",
        brain_region_id=utils.get_partial_circuit_region_id(partial_circuit),
        atlas_release_id=partial_circuit.atlasRelease.get_id(),
        description="Circuit with nodes and functionalized synapses.",
        sonata_config_path=output_config_file,
    )
    utils.write_resource_to_definition_output(
        json_resource=load_by_id(partial_circuit.get_id()),
        variant=variant,
        output_dir=output_dir,
    )


def _run_functionalizer(
    nodes_file,
    node_population_name,
    edges_file,
    edge_population_name,
    recipe_file,
    morphologies_dir,
    output_dir,
    variant,
):
    work_dir = utils.create_dir(output_dir / "workdir", clean_if_exists=True)

    base_command = [
        "env",
        f"SPARK_USER={os.environ['USER']}",
        "dplace",
        "functionalizer",
        str(edges_file),
        edge_population_name,
        "--work-dir",
        str(work_dir),
        "--output-dir",
        str(output_dir),
        "--from",
        str(nodes_file),
        node_population_name,
        "--to",
        str(nodes_file),
        node_population_name,
        "--filters",
        "SynapseProperties",
        "--recipe",
        str(recipe_file),
        "--morphologies",
        str(morphologies_dir),
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
    ]
    str_base_command = " ".join(base_command)

    str_command = utils.build_variant_allocation_command(
        str_base_command, variant, sub_task_index=1, srun="srun dplace"
    )

    L.info("Tool full command: %s", str_command)
    subprocess.run(str_command, check=True, shell=True)

    if not output_edges_file.exists():
        raise CWLWorkflowError(f"Edges file has failed to be generated at {output_edges_file}")


def _write_partial_config(config, edges_file, output_file):
    config = copy.deepcopy(config)

    edges = config["networks"]["edges"]

    if len(edges) == 0:
        raise CWLWorkflowError(f"Only one edge population is supported. Found: {len(edges)}")

    edges[0]["edges_file"] = str(edges_file)

    utils.write_json(filepath=output_file, data=config)
