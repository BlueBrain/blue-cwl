"""Synapse filtering module."""
import copy
import logging
import os
import subprocess
from pathlib import Path

import click
import libsonata
import voxcell
from entity_management.nexus import load_by_id

from cwl_registry import (
    Variant,
    nexus,
    population_utils,
    recipes,
    registering,
    staging,
    utils,
    validation,
)
from cwl_registry.exceptions import CWLWorkflowError

L = logging.getLogger(__name__)


# pylint: disable=unused-argument


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

    forge = nexus.get_forge()

    config = utils.load_json(nexus.get_config_path_from_circuit_resource(forge, partial_circuit))
    partial_circuit = nexus.get_resource(forge, partial_circuit)

    nodes_file, node_population_name = utils.get_biophysical_partial_population_from_config(config)

    validation.check_properties_in_population(
        population_name=node_population_name,
        nodes_file=nodes_file,
        property_names=["hemisphere", "region", "mtype", "etype", "synapse_class"],
    )

    edges_file, edge_population_name = utils.get_first_edge_population_from_config(config)

    morphologies_dir = utils.get_morphologies_dir(
        config["networks"]["nodes"], node_population_name, "h5"
    )

    atlas_dir = utils.create_dir(staging_dir / "atlas")
    L.info("Staging atlas to  %s", atlas_dir)
    atlas_info = staging.stage_atlas(
        forge=forge,
        resource_id=partial_circuit.atlasRelease.id,
        output_dir=atlas_dir,
    )

    L.info("Staging configuration...")
    configuration = staging.materialize_synapse_config(forge, configuration, staging_dir)[
        "configuration"
    ]
    if configuration:
        configuration = {name: utils.load_json(path) for name, path in configuration.items()}

        L.info("Building functionalizer xml recipe...")
        recipe_file = recipes.write_functionalizer_xml_recipe(
            synapse_config=configuration,
            circuit_pathways=_get_connectome_pathways(
                edges_file, edge_population_name, nodes_file, node_population_name
            ),
            region_map=voxcell.RegionMap.load_json(atlas_info.ontology_path),
            annotation=voxcell.VoxelData.load_nrrd(atlas_info.annotation_path),
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
    command = [
        "env",
        f"SPARK_USER={os.environ['USER']}",
        "dplace",
        "functionalizer",
        "-p",
        "spark.driver.memory=60g",
        str(edges_file),
        edge_population_name,
        "--work-dir",
        str(build_dir),
        "--output-dir",
        str(build_dir),
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
    L.debug("Tool command: %s", " ".join(command))

    subprocess.run(command, check=True)

    parquet_dir = build_dir / "circuit.parquet"
    assert parquet_dir.exists()

    L.info("Parquet files generated in %s", parquet_dir)

    output_edges_file = build_dir / "edges.h5"

    L.info("Running parquet conversion to sonata...")

    command = [
        "parquet2hdf5",
        str(parquet_dir),
        str(output_edges_file),
        edge_population_name,
    ]
    L.debug("Tool command: %s", " ".join(command))

    subprocess.run(command, check=True)

    L.info("Functionalized edges generated at %s", output_edges_file)

    output_config_file = output_dir / "circuit_config.json"
    _write_partial_config(config, output_edges_file, output_config_file)

    forge = nexus.get_forge(force_refresh=True)

    # output circuit
    L.info("Registering partial circuit...")
    circuit_resource = registering.register_partial_circuit(
        name="Partial circuit with functional connectivity",
        brain_region_id=partial_circuit.brainLocation.brainRegion.id,
        atlas_release_id=partial_circuit.atlasRelease.id,
        description="Circuit with nodes and functionalized synapses.",
        sonata_config_path=output_config_file,
    )

    utils.write_resource_to_definition_output(
        json_resource=load_by_id(circuit_resource.get_id()),
        variant=Variant.from_resource_id(forge, variant_config),
        output_dir=output_dir,
    )


def _get_connectome_pathways(edges_file, edge_population_name, nodes_file, node_population_name):
    node_population = libsonata.NodeStorage(nodes_file).open_population(node_population_name)
    edge_population = libsonata.EdgeStorage(edges_file).open_population(edge_population_name)
    return population_utils.get_pathways(
        edge_population=edge_population,
        source_node_population=node_population,
        target_node_population=node_population,
        properties=["hemisphere", "region", "mtype", "etype", "synapse_class"],
    )


def _write_partial_config(config, edges_file, output_file):
    config = copy.deepcopy(config)

    edges = config["networks"]["edges"]

    if len(edges) == 0:
        raise CWLWorkflowError(f"Only one edge population is supported. Found: {len(edges)}")

    edges[0]["edges_file"] = str(edges_file)

    utils.write_json(filepath=output_file, data=config)
