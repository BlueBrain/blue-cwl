"""Placeholder emodel assignment."""
from pathlib import Path

import click
import libsonata
import numpy as np

from cwl_registry import registering, staging, utils
from cwl_registry.hashing import get_target_hexdigest
from cwl_registry.nexus import get_forge


@click.command()
@click.option("--region", required=True)
@click.option("--mtype-morphologies", required=True)
@click.option("--partial-circuit", required=True)
@click.option("--nexus-base", envvar="NEXUS_BASE", required=True)
@click.option("--nexus-org", envvar="NEXUS_ORG", required=True)
@click.option("--nexus-project", envvar="NEXUS_PROJ", required=True)
@click.option("--nexus-token", envvar="NEXUS_TOKEN", required=True)
@click.option("--task-digest", required=True)
@click.option("--output-dir", required=True)
def app(
    region,
    mtype_morphologies,
    partial_circuit,
    nexus_base,
    nexus_org,
    nexus_project,
    nexus_token,
    task_digest,
    output_dir,
):
    """Morphoelectrical type generator cli entry."""
    output_dir = utils.create_dir(Path(output_dir).resolve())

    forge = get_forge(
        nexus_base=nexus_base,
        nexus_org=nexus_org,
        nexus_project=nexus_project,
        nexus_token=nexus_token,
    )

    morphologies_dir = utils.create_dir(output_dir / "morphologies")
    mtype_to_morphologies = staging.stage_mtype_morphologies(
        forge, mtype_morphologies, morphologies_dir
    )

    config = utils.load_json(utils.get_config_path_from_circuit_resource(forge, partial_circuit))
    nodes_file, population_name = utils.get_biophysical_partial_population_from_config(config)

    output_nodes_file = output_dir / "nodes.h5"
    properties = _assign_morphologies(nodes_file, population_name, mtype_to_morphologies)
    utils.write_node_population_with_properties(
        nodes_file, population_name, properties, output_nodes_file
    )
    sonata_config_file = output_dir / "circuit_config.json"
    _write_partial_config(
        config, output_nodes_file, population_name, morphologies_dir, sonata_config_file
    )

    target_digest = get_target_hexdigest(
        task_digest,
        "circuit_morphologies_bundle",
    )
    partial_circuit = forge.retrieve(partial_circuit, cross_bucket=True)
    registering.register_partial_circuit(
        forge,
        name="Cell properties | Morphologies  partial circuit",
        brain_region_id=region,
        atlas_release_id=partial_circuit.atlasRelease.id,
        description="Partial circuit built with cell properties, and morphologies.",
        sonata_config_path=sonata_config_file,
        target_digest=target_digest,
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
            "partial": ["cell-properties", "morphologies"],
            "morphologies_dir": str(morphologies_dir),
        },
        filepath=str(nodes_file),
    )
    utils.write_json(filepath=output_file, data=updated_config)
