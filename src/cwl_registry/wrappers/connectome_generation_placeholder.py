"""Connectome manipulation wrapper."""
import copy
import logging
import subprocess

import click
import h5py
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
        variant=Variant.from_resource_id(forge, variant_config),
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


def _run_connectome_manipulator(recipe_file, output_dir):
    """Run connectome manipulator."""
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
