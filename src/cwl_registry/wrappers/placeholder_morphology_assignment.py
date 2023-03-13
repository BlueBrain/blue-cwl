"""Placeholder emodel assignment."""
from pathlib import Path

import click
import libsonata
import numpy as np

from cwl_registry import registering, staging, utils, validation
from cwl_registry.nexus import get_forge
from cwl_registry.variant import Variant


@click.command()
@click.option("--region", required=True)
@click.option("--mtype-morphologies", required=True)
@click.option("--partial-circuit", required=True)
@click.option("--variant-config", required=True)
@click.option("--output-dir", required=True)
def app(
    region,
    mtype_morphologies,
    partial_circuit,
    variant_config,
    output_dir,
):
    """Morphoelectrical type generator cli entry."""
    output_dir = utils.create_dir(Path(output_dir).resolve())

    forge = get_forge()

    morphologies_dir = utils.create_dir(output_dir / "morphologies")
    mtype_to_morphologies = staging.stage_mtype_morphologies(
        forge, mtype_morphologies, morphologies_dir
    )

    config = utils.load_json(utils.get_config_path_from_circuit_resource(forge, partial_circuit))
    nodes_file, population_name = utils.get_biophysical_partial_population_from_config(config)

    region_acronym = utils.get_region_resource_acronym(forge, region)
    validation.check_population_name_consistent_with_region(population_name, region_acronym)

    output_nodes_file = output_dir / "nodes.h5"
    properties = _assign_morphologies(nodes_file, population_name, mtype_to_morphologies)
    utils.write_node_population_with_properties(
        nodes_file, population_name, properties, output_nodes_file
    )
    validation.check_population_name_in_nodes(population_name, output_nodes_file)

    sonata_config_file = output_dir / "circuit_config.json"
    _write_partial_config(
        config, output_nodes_file, population_name, morphologies_dir, sonata_config_file
    )
    validation.check_population_name_in_config(population_name, sonata_config_file)

    partial_circuit = forge.retrieve(partial_circuit, cross_bucket=True)
    circuit_resource = registering.register_partial_circuit(
        forge,
        name="Partial circuit with morphologies",
        brain_region_id=region,
        atlas_release_id=partial_circuit.atlasRelease.id,
        description="Partial circuit built with cell properties, and morphologies.",
        sonata_config_path=sonata_config_file,
    )
    utils.write_resource_to_definition_output(
        forge=forge,
        resource=circuit_resource,
        variant=Variant.from_resource_id(forge, variant_config),
        output_dir=output_dir,
    )


def _assign_morphologies(nodes_file: Path, population_name: str, mtype_to_morphologies):
    pop = libsonata.NodeStorage(nodes_file).open_population(population_name)

    mtypes = pop.get_attribute("mtype", libsonata.Selection(np.arange(0, pop.size)))

    properties = {"morphology": [mtype_to_morphologies[mtype][0].stem for mtype in mtypes]}

    return properties


def _write_partial_config(config, nodes_file, population_name, morphologies_dir, output_file):
    """Update partial config with new nodes path and the morphology directory."""
    updated_config = utils.update_circuit_config_population(
        config=config,
        population_name=population_name,
        population_data={
            "partial": ["morphologies"],
            "morphologies_dir": str(morphologies_dir),
        },
        filepath=str(nodes_file),
    )
    utils.write_json(filepath=output_file, data=updated_config)
