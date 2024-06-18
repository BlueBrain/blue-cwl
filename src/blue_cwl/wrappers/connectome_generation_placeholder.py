# SPDX-License-Identifier: Apache-2.0

"""Connectome manipulation wrapper."""

import copy
import logging
from pathlib import Path

import click
import libsonata
import numpy as np
import voxcell
from entity_management.nexus import load_by_id
from entity_management.simulation import DetailedCircuit
from entity_management.util import get_entity

from blue_cwl import (
    brain_regions,
    connectome,
    recipes,
    registering,
    staging,
    utils,
    validation,
)
from blue_cwl.typing import StrOrPath
from blue_cwl.utils import create_dir

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


@click.group(name="connectome-generation-placeholder")
def app():
    """Placeholder micro connectome generation."""


@app.command(name="stage")
@click.option(
    "--configuration-id", required=True, help="Connectome generation configuration NEXUS id."
)
@click.option("--circuit-id", required=True, help="Circuit NEXUS id.")
@click.option("--macro-connectome-config-id", required=True)
@click.option("--stage-dir", required=True, help="Staging directory to output staged data.")
def stage_cli(**kwargs):
    """Connectome generation staging cli."""
    stage(**kwargs)


def stage(
    *,
    configuration_id: str,
    circuit_id: str,
    macro_connectome_config_id: str,
    stage_dir: StrOrPath,
) -> None:
    """Stage NEXUS entities into local files.

    Args:
        configuration_id: mmodel configuration NEXUS id.
        circuit_id: DetailedCircuit NEXUS id.
        macro_connectome_config_id: Macro connectome confi NEXUS id.
        stage_dir: Output directory for staging data.

    Entities required to be staged for mmodel:
        - circuit config
        - atlas info file
        - macro config file
        - micro config file
    """
    circuit = _stage_circuit(circuit_id, stage_dir)

    staging.stage_atlas(
        circuit.atlasRelease,
        output_dir=Path(stage_dir, "atlas"),
        output_file=Path(stage_dir, "atlas.json"),
    )

    _stage_configuration(
        macro_config_id=macro_connectome_config_id,
        micro_config_id=configuration_id,
        output_dir=stage_dir,
    )


def _stage_circuit(circuit_id: str, stage_dir: StrOrPath):
    """Stage NEXUS circuit id into local components.

    Stages the following circuit components:
        - circuit config -> stage_dir/circuit_config.json
        - nodes file -> stage_dir/nodes.h5

    Note: Before staging the nodes are validated wrt to the expected columns.
    """
    entity = get_entity(resource_id=circuit_id, cls=DetailedCircuit)

    circuit_config_file = entity.circuitConfigPath.get_url_as_path()
    circuit_config = utils.load_json(circuit_config_file)

    nodes_file, population_name = utils.get_biophysical_partial_population_from_config(
        circuit_config
    )
    validation.check_properties_in_population(
        population_name, nodes_file, INPUT_NODE_POPULATION_COLUMNS
    )

    circuit_file = Path(stage_dir, "circuit_config.json")
    staging.stage_file(source=circuit_config_file, target=circuit_file)
    L.debug("Circuit %s staged at %s", circuit_id, circuit_file)

    return entity


def _stage_configuration(
    *, macro_config_id: str, micro_config_id: str, output_dir: StrOrPath
) -> None:
    L.info("Materializing macro connectome dataset configuration...")
    macro_file = Path(output_dir, "materialized_macro_config.json")
    staging.materialize_macro_connectome_config(macro_config_id, output_file=macro_file)
    L.info("Materialized macro connectome config %s -> %s", macro_config_id, macro_file)

    L.info("Materializing micro connectome dataset configuration...")
    micro_file = Path(output_dir, "materialized_micro_config.json")
    staging.materialize_micro_connectome_config(micro_config_id, output_file=micro_file)
    L.info("Materialized micro connectome config %s -> %s", micro_config_id, micro_file)


@app.command(name="transform")
@click.option("--atlas-file", required=True)
@click.option("--circuit-config-file", required=True)
@click.option("--macro-config-file", required=True)
@click.option("--micro-config-file", required=True)
@click.option("--transform-dir", required=True)
def transform_cli(**kwargs):
    """Build connectome recipe."""
    transform(**kwargs)


def transform(
    *,
    atlas_file: StrOrPath,
    circuit_config_file: StrOrPath,
    macro_config_file: StrOrPath,
    micro_config_file: StrOrPath,
    transform_dir: StrOrPath,
) -> None:
    """Build connectome recipe."""
    atlas_info = staging.AtlasInfo.from_file(atlas_file)
    macro_config = utils.load_json(macro_config_file)
    micro_config = utils.load_json(micro_config_file)

    L.debug("Assembling macro matrix...")
    macro_matrix = connectome.assemble_macro_matrix(macro_config)

    population = _get_node_population(circuit_config_file=circuit_config_file)

    region_volumes = _get_population_unique_region_volumes(
        population=population,
        ontology_file=atlas_info.ontology_path,
    )

    L.debug("Assembling micro datasets...")
    micro_matrices = connectome.resolve_micro_matrices(
        micro_config=micro_config,
        macro_matrix=macro_matrix,
        population=population,
        region_volumes=region_volumes,
    )

    recipe_dir = create_dir(transform_dir, "recipe")

    L.debug("Generating connectome recipe...")
    recipe_file = Path(transform_dir, "recipe.json")
    recipe = recipes.build_connectome_manipulator_recipe(
        circuit_config_path=str(circuit_config_file),
        micro_matrices=micro_matrices,
        output_dir=recipe_dir,
    )
    utils.write_json(data=recipe, filepath=recipe_file)
    L.debug("Connectome manipulation recipe written at %s", recipe_file)


def _get_node_population(circuit_config_file) -> libsonata.NodePopulation:
    (
        nodes_file,
        node_population_name,
    ) = utils.get_biophysical_partial_population_from_config(utils.load_json(circuit_config_file))
    population = libsonata.NodeStorage(nodes_file).open_population(node_population_name)
    return population


def _get_population_unique_region_volumes(
    population: libsonata.NodePopulation, ontology_file: StrOrPath
):
    regions = np.unique(population.get_attribute("region", population.select_all())).tolist()
    region_volumes = brain_regions.volumes(voxcell.RegionMap.load_json(ontology_file), regions)
    return region_volumes


@app.command(name="register")
@click.option("--circuit-id", required=True)
@click.option("--edges-file", required=True)
@click.option("--output-dir", required=True)
def register_cli(**kwargs):
    """Register circuit resource."""
    register(**kwargs)


def register(*, circuit_id: str, edges_file: StrOrPath, output_dir: StrOrPath):
    """Register circuit resource."""
    input_circuit = get_entity(resource_id=circuit_id, cls=DetailedCircuit)

    input_circuit_config = utils.load_json(input_circuit.circuitConfigPath.get_url_as_path())

    edge_population_name = next(iter(libsonata.EdgeStorage(edges_file).population_names))

    L.info("Writing partial circuit config...")
    sonata_config_file = Path(output_dir, "circuit_config.json")
    _write_partial_config(
        config=input_circuit_config,
        edges_file=edges_file,
        population_name=edge_population_name,
        output_file=sonata_config_file,
    )

    L.info("Registering partial circuit...")
    output_circuit = registering.register_partial_circuit(
        name="Partial circuit with connectivity",
        brain_region_id=utils.get_partial_circuit_region_id(input_circuit),
        atlas_release=input_circuit.atlasRelease,
        description="Partial circuit with cell properties, emodels, morphologies and connectivity.",
        sonata_config_path=sonata_config_file,
    )
    utils.write_json(
        data=load_by_id(output_circuit.get_id()),
        filepath=Path(output_dir, "resource.json"),
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
