"""Synapse filtering module."""
import copy
import logging
import os
import subprocess
from pathlib import Path

import click

from cwl_registry import Variant, recipes, registering, utils
from cwl_registry.exceptions import CWLWorkflowError
from cwl_registry.nexus import get_forge, get_resource

L = logging.getLogger(__name__)


# pylint: disable=unused-argument


@click.command()
@click.option("--configuration", required=True)
@click.option("--variant-config", required=False)
@click.option("--partial-circuit", required=True)
@click.option("--output-dir", required=True)
def app(configuration, variant_config, partial_circuit, output_dir):
    """Synapse filtering."""
    output_dir = utils.create_dir(Path(output_dir).resolve())
    build_dir = utils.create_dir(output_dir / "build")

    forge = get_forge()

    config = utils.load_json(utils.get_config_path_from_circuit_resource(forge, partial_circuit))

    nodes_file, node_population_name = utils.get_biophysical_partial_population_from_config(config)
    edges_file, edge_population_name = utils.get_first_edge_population_from_config(config)

    morphologies_dir = config["networks"]["nodes"][0]["populations"][node_population_name][
        "morphologies_dir"
    ]

    recipe_file = recipes.write_functionalizer_recipe(output_file=build_dir / "recipe.xml")
    subprocess.run(
        [
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
        ],
        check=True,
    )

    parquet_dir = build_dir / "circuit.parquet"
    assert parquet_dir.exists()

    L.info("Parquet files generated in %s", parquet_dir)

    output_edges_file = build_dir / "edges.h5"

    subprocess.run(
        [
            "parquet2hdf5",
            str(parquet_dir),
            str(output_edges_file),
            edge_population_name,
        ],
        check=True,
    )

    L.info("Functionalized edges generated at %s", output_edges_file)

    output_config_file = output_dir / "circuit_config.json"
    _write_partial_config(config, output_edges_file, edge_population_name, output_config_file)

    forge = get_forge(force_refresh=True)
    partial_circuit = get_resource(forge, partial_circuit)

    # output circuit
    L.info("Registering partial circuit...")
    circuit_resource = registering.register_partial_circuit(
        forge,
        name="Partial circuit with functional connectivity",
        brain_region_id=partial_circuit.brainLocation.brainRegion.id,
        atlas_release_id=partial_circuit.atlasRelease.id,
        description="Circuit with nodes and functionalized synapses.",
        sonata_config_path=output_config_file,
    )

    utils.write_resource_to_definition_output(
        forge=forge,
        resource=circuit_resource,
        variant=Variant.from_resource_id(forge, variant_config),
        output_dir=output_dir,
    )


def _write_partial_config(config, edges_file, population_name, output_file):
    config = copy.deepcopy(config)

    edges = config["networks"]["edges"]

    if len(edges) == 0:
        raise CWLWorkflowError(f"Only one edge population is supported. Found: {len(edges)}")

    edge_population = edges[0]["populations"][population_name]
    edge_population["edges_file"] = str(edges_file)

    utils.write_json(filepath=output_file, data=config)
