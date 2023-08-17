"""Placeholder emodel assignment."""
from pathlib import Path

import click
import libsonata
import numpy as np
from entity_management.nexus import load_by_id
from voxcell.math_utils import angles_to_matrices

from cwl_registry import nexus, registering, staging, utils, validation
from cwl_registry.variant import Variant

# these values where found by examining the proj83/Bio_M circuit, and find the
# 25% and 75% percentile value, by INH/EXC
EXC_THRESHOLD_CURRENT_RANGE = (7.009766e-02, 1.742969e-01)
EXC_HOLDING_CURRENT_RANGE = (-8.513693e-02, -1.867439e-02)
INH_THRESHOLD_CURRENT_RANGE = (0.037985, 0.154215)
INH_HOLDING_CURRENT_RANGE = (-0.060696, -0.029930)


@click.command()
@click.option("--seed", default=42)
@click.option("--region", required=True)
@click.option("--mtype-morphologies", required=True)
@click.option("--partial-circuit", required=True)
@click.option("--variant-config", required=True)
@click.option("--output-dir", required=True)
def app(
    seed,
    region,
    mtype_morphologies,
    partial_circuit,
    variant_config,
    output_dir,
):
    """Morphoelectrical type generator cli entry."""
    output_dir = utils.create_dir(Path(output_dir).resolve())

    forge = nexus.get_forge()

    morphologies_dir = utils.create_dir(output_dir / "morphologies")
    mtype_to_morphologies = staging.stage_mtype_morphologies(
        forge, mtype_morphologies, morphologies_dir
    )

    config = utils.load_json(nexus.get_config_path_from_circuit_resource(forge, partial_circuit))
    nodes_file, population_name = utils.get_biophysical_partial_population_from_config(config)

    region_acronym = nexus.get_region_resource_acronym(forge, region)
    validation.check_population_name_consistent_with_region(population_name, region_acronym)

    output_nodes_file = output_dir / "nodes.h5"
    properties = _assign_morphologies(nodes_file, population_name, mtype_to_morphologies, seed)
    orientations = np.repeat(
        angles_to_matrices([np.pi / 2], "y"), repeats=len(properties["morphology"]), axis=0
    )
    utils.write_node_population_with_properties(
        nodes_file=nodes_file,
        population_name=population_name,
        properties=properties,
        orientations=orientations,
        output_file=output_nodes_file,
    )
    validation.check_population_name_in_nodes(population_name, output_nodes_file)

    sonata_config_file = output_dir / "circuit_config.json"
    _write_partial_config(
        config, output_nodes_file, population_name, morphologies_dir, sonata_config_file
    )
    validation.check_population_name_in_config(population_name, sonata_config_file)

    partial_circuit = forge.retrieve(partial_circuit, cross_bucket=True)
    circuit_resource = registering.register_partial_circuit(
        name="Partial circuit with morphologies",
        brain_region_id=region,
        atlas_release_id=partial_circuit.atlasRelease.id,
        description="Partial circuit built with cell properties, and morphologies.",
        sonata_config_path=sonata_config_file,
    )
    utils.write_resource_to_definition_output(
        json_resource=load_by_id(circuit_resource.get_id()),
        variant=Variant.from_resource_id(forge, variant_config),
        output_dir=output_dir,
    )


def _assign_morphologies(nodes_file: Path, population_name: str, mtype_to_morphologies, seed: int):
    rng = np.random.default_rng(seed)

    pop = libsonata.NodeStorage(nodes_file).open_population(population_name)

    exc_mask = pop.get_attribute("synapse_class", pop.select_all()) == "EXC"
    exc_count = np.sum(exc_mask)
    inh_count = pop.size - exc_count

    threshold_current = np.empty(pop.size, dtype=np.float32)
    threshold_current[exc_mask] = rng.uniform(
        EXC_THRESHOLD_CURRENT_RANGE[0], EXC_THRESHOLD_CURRENT_RANGE[1], exc_count
    )
    threshold_current[~exc_mask] = rng.uniform(
        INH_THRESHOLD_CURRENT_RANGE[0], INH_THRESHOLD_CURRENT_RANGE[1], inh_count
    )

    holding_current = np.empty(pop.size, dtype=np.float32)
    holding_current[exc_mask] = rng.uniform(
        EXC_HOLDING_CURRENT_RANGE[0], EXC_HOLDING_CURRENT_RANGE[1], exc_count
    )
    holding_current[~exc_mask] = rng.uniform(
        INH_HOLDING_CURRENT_RANGE[0], INH_HOLDING_CURRENT_RANGE[1], inh_count
    )

    mtypes = pop.get_attribute("mtype", pop.select_all())

    properties = {
        "morphology": [mtype_to_morphologies[mtype][0].stem for mtype in mtypes],
        "@dynamics/threshold_current": threshold_current,
        "@dynamics/holding_current": holding_current,
    }

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
