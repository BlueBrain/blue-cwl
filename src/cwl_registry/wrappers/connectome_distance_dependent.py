"""Connectome manipulation wrapper."""
import copy
import logging
import subprocess

import click
import h5py
import numpy as np

from cwl_registry import nexus, recipes, registering, staging, utils
from cwl_registry.exceptions import CWLWorkflowError
from cwl_registry.variant import Variant

L = logging.getLogger(__name__)


@click.command()
@click.option("--configuration", required=True)
@click.option("--partial-circuit", required=True)
@click.option("--variant-config", required=True)
@click.option("--output-dir", required=True)
def app(configuration, partial_circuit, variant_config, output_dir):
    """Build micro connectome"""
    output_dir = utils.create_dir(output_dir)
    _app(configuration, partial_circuit, variant_config, output_dir)


def _app(configuration, partial_circuit, variant_config, output_dir):
    forge = nexus.get_forge()

    staging_dir = utils.create_dir(output_dir / "stage")

    L.debug("Staging connectome dataset configuration...")
    configuration = staging.materialize_json_file_from_resource(
        resource=nexus.get_resource(forge, configuration),
        output_file=staging_dir / "configuration.json",
    )
    configuration_df = staging.materialize_connectome_dataset(
        forge=forge,
        dataset=configuration,
        output_file=staging_dir / "materialized_configuration.json",
    )

    config_path = nexus.get_config_path_from_circuit_resource(forge, partial_circuit)
    staging.stage_file(
        source=config_path,
        target=staging_dir / config_path.name,
    )
    config = utils.load_json(config_path)

    build_dir = utils.create_dir(output_dir / "build", clean_if_exists=True)

    L.debug("Generating connectome recipe...")
    recipe_file = build_dir / "manipulation-config.json"
    recipe = recipes.build_connectome_distance_dependent_recipe(
        config_path, configuration_df, build_dir
    )
    utils.write_json(data=recipe, filepath=recipe_file)

    L.info("Running connectome manipulator...")
    edges_file, edge_population_name = _run_connectome_manipulator(recipe_file, build_dir)

    L.info("Writing partial circuit config...")
    sonata_config_file = output_dir / "circuit_config.json"
    _write_partial_config(
        config=config,
        edges_file=edges_file,
        population_name=edge_population_name,
        output_file=sonata_config_file,
    )

    forge = nexus.get_forge(force_refresh=True)

    # input circuit
    partial_circuit = nexus.get_resource(forge, partial_circuit)

    # output circuit
    L.info("Registering partial circuit...")
    circuit_resource = registering.register_partial_circuit(
        forge,
        name="Partial circuit with connectivity",
        brain_region_id=partial_circuit.brainLocation.brainRegion.id,
        atlas_release_id=partial_circuit.atlasRelease.id,
        description="Partial circuit with cell properties, emodels, morphologies and connectivity.",
        sonata_config_path=sonata_config_file,
    )

    utils.write_resource_to_definition_output(
        json_resource=forge.as_json(circuit_resource),
        variant=Variant.from_resource_id(forge, variant_config),
        output_dir=output_dir,
    )


def _run_connectome_manipulator(recipe_file, output_dir):
    subprocess.run(
        [
            "connectome-manipulator",
            "manipulate-connectome",
            "--output-dir",
            str(output_dir),
            str(recipe_file),
            "--convert-to-sonata",
            "--overwrite-edges",
        ],
        check=True,
    )

    edges_file = output_dir / "edges.h5"
    if not edges_file.exists():
        raise CWLWorkflowError(f"Edges file has failed to be generated at {edges_file}")

    edge_population_name = utils.get_edge_population_name(edges_file)

    # Fix for spykfunc, This should not be needed in general.
    _add_efferent_section_type(edges_file, edge_population_name)

    L.debug("Edge population %s generated at %s", edge_population_name, edges_file)

    return edges_file, edge_population_name


def _add_efferent_section_type(edges_file, population_name):
    """Add efferent section type until spykfunc is fixed to not depend on it."""
    with h5py.File(edges_file, "r+") as fd:
        edge_population = fd["edges"][population_name]

        shape = len(edge_population["target_node_id"])

        edge_population["0"].create_dataset(
            "efferent_section_type",
            data=np.full(fill_value=2, shape=shape),
            dtype=np.uint32,
        )


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
