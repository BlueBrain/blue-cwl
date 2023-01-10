"""Placeholder emodel assignment."""
from pathlib import Path

import click
import libsonata
import numpy as np

from cwl_registry import registering, staging, utils, validation
from cwl_registry.hashing import get_target_hexdigest
from cwl_registry.nexus import get_forge


@click.command()
@click.option("--region", required=True)
@click.option("--partial-circuit", required=True)
@click.option("--etype-emodels", required=True)
@click.option("--task-digest", required=True)
@click.option("--output-dir", required=True)
def app(
    region,
    partial_circuit,
    etype_emodels,
    task_digest,
    output_dir,
):
    """Placeholder emodel assignment cli."""
    output_dir = utils.create_dir(Path(output_dir).resolve())

    forge = get_forge()

    emodels_dir = utils.create_dir(output_dir / "hoc_files")
    staging.stage_etype_emodels(forge, etype_emodels, emodels_dir)

    config = utils.load_json(utils.get_config_path_from_circuit_resource(forge, partial_circuit))
    nodes_file, population_name = utils.get_biophysical_partial_population_from_config(config)

    region_acronym = utils.get_region_resource_acronym(forge, region)
    validation.check_population_name_consistent_with_region(population_name, region_acronym)

    properties = _create_model_template(nodes_file, population_name)

    output_nodes_file = output_dir / "nodes.h5"
    utils.write_node_population_with_properties(
        nodes_file, population_name, properties, output_nodes_file
    )
    validation.check_population_name_in_nodes(population_name, output_nodes_file)

    sonata_config_file = output_dir / "circuit_config.json"
    _write_partial_config(
        config, output_nodes_file, population_name, emodels_dir, sonata_config_file
    )
    validation.check_population_name_in_config(population_name, sonata_config_file)

    target_digest = get_target_hexdigest(
        task_digest,
        "circuit_emodels_bundle",
    )
    partial_circuit_resource = forge.retrieve(partial_circuit, cross_bucket=True)
    registering.register_partial_circuit(
        forge,
        name="Cell properties | Morphologies | emodels partial circuit",
        brain_region_id=region,
        atlas_release_id=partial_circuit_resource.atlasRelease.id,
        description="Partial circuit built with cell positions, morphologies, and emodels.",
        sonata_config_path=sonata_config_file,
        target_digest=target_digest,
    )


def _create_model_template(nodes_file: Path, population_name: str):

    pop = libsonata.NodeStorage(nodes_file).open_population(population_name)
    etypes = pop.get_attribute("etype", libsonata.Selection(np.arange(0, pop.size)))

    properties = {"model_template": [f"hoc:{etype}" for etype in etypes]}

    return properties


def _write_partial_config(old_config, output_nodes_file, population_name, emodels_dir, output_file):
    new_config = utils.update_circuit_config_population(
        config=old_config,
        population_name=population_name,
        population_data={
            "partial": ["cell-properties", "morphologies", "emodels"],
            "biophysical_neuron_models_dir": str(emodels_dir),
        },
        filepath=output_nodes_file,
    )
    utils.write_json(filepath=output_file, data=new_config)
