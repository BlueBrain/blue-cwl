"""Placeholder emodel assignment."""
import logging
import subprocess
from pathlib import Path

import click
import libsonata
import morphio
import numpy as np
import pandas as pd
import voxcell
from entity_management.nexus import load_by_id

from cwl_registry import nexus, registering, staging, utils, validation
from cwl_registry.mmodel import recipe
from cwl_registry.mmodel.entity import MorphologyAssignmentConfig
from cwl_registry.utils import bisect_cell_collection_by_properties, merge_cell_collections
from cwl_registry.variant import Variant

# these values where found by examining the proj83/Bio_M circuit, and find the
# 25% and 75% percentile value, by INH/EXC
EXC_THRESHOLD_CURRENT_RANGE = (7.009766e-02, 1.742969e-01)
EXC_HOLDING_CURRENT_RANGE = (-8.513693e-02, -1.867439e-02)
INH_THRESHOLD_CURRENT_RANGE = (0.037985, 0.154215)
INH_HOLDING_CURRENT_RANGE = (-0.060696, -0.029930)


SEED = 42

L = logging.getLogger(__name__)

# pylint: disable=too-many-arguments


@click.command()
@click.option("--configuration", required=True)
@click.option("--partial-circuit", required=True)
@click.option("--variant-config", required=True)
@click.option("--output-dir", required=True)
@click.option("--parallel", required=False, default=True)
def app(
    configuration,
    partial_circuit,
    variant_config,
    output_dir,
    parallel,
):
    """Morphoelectrical type generator cli entry."""
    return _app(configuration, partial_circuit, variant_config, output_dir, parallel)


def _app(configuration, partial_circuit, variant_config, output_dir, parallel):
    output_dir = utils.create_dir(Path(output_dir).resolve())
    staging_dir = utils.create_dir(output_dir / "stage")
    build_dir = utils.create_dir(output_dir / "build")
    atlas_dir = utils.create_dir(staging_dir / "atlas")
    morphologies_dir = utils.create_dir(build_dir / "morphologies", clean_if_exists=True)

    forge = nexus.get_forge()
    partial_circuit = nexus.get_resource(forge, partial_circuit)

    atlas_info = staging.stage_atlas(
        forge=forge,
        resource_id=partial_circuit.atlasRelease.id,
        output_dir=atlas_dir,
        cell_orientation_field_basename="raw_orientation.nrrd",
    )
    raw_config = MorphologyAssignmentConfig.from_id(configuration).to_model()
    placeholders, canonicals = raw_config.expand(forge).split()

    L.info("Materializing canonical morphology configuration...")
    canonicals = canonicals.materialize(
        forge=forge,
        output_file=staging_dir / "materialize_canonical_config.json",
        labels_only=True,
    )
    L.info("Materializing placeholder morphology configuration...")
    placeholders = placeholders.materialize(
        forge=forge,
        output_file=staging_dir / "materialize_placeholders_config.json",
        labels_only=True,
    )

    circuit_config = utils.load_json(
        nexus.get_config_path_from_circuit_resource(forge, partial_circuit.id)
    )

    nodes_file, population_name = utils.get_biophysical_partial_population_from_config(
        circuit_config
    )

    variant = Variant.from_resource_id(forge, variant_config)

    output_nodes_file = build_dir / "nodes.h5"
    _generate_mmodel_nodes(
        canonicals=canonicals,
        placeholders=placeholders,
        nodes_file=nodes_file,
        population_name=population_name,
        atlas_info=atlas_info,
        output_dir=build_dir,
        output_nodes_file=output_nodes_file,
        output_morphologies_dir=morphologies_dir,
        parallel=parallel,
        variant=variant,
    )

    sonata_config_file = output_dir / "circuit_config.json"
    _write_partial_config(
        config=circuit_config,
        nodes_file=output_nodes_file,
        population_name=population_name,
        morphologies_dir=morphologies_dir,
        output_file=sonata_config_file,
    )
    validation.check_population_name_in_config(population_name, sonata_config_file)

    forge = nexus.get_forge(force_refresh=True)
    circuit = registering.register_partial_circuit(
        name="Partial circuit with morphologies",
        brain_region_id=partial_circuit.brainLocation.brainRegion.id,
        atlas_release_id=partial_circuit.atlasRelease.id,
        description="Partial circuit built with cell properties, and morphologies.",
        sonata_config_path=sonata_config_file,
    )
    utils.write_resource_to_definition_output(
        json_resource=load_by_id(circuit.get_id()),
        variant=variant,
        output_dir=output_dir,
    )


def _generate_mmodel_nodes(
    canonicals,
    placeholders,
    nodes_file,
    population_name,
    atlas_info,
    output_dir,
    output_nodes_file,
    output_morphologies_dir,
    parallel,
    variant,
):
    L.info("Assigning morphologies...")
    _assign_morphologies(
        canonicals=canonicals,
        placeholders=placeholders,
        nodes_file=nodes_file,
        population_name=population_name,
        atlas_info=atlas_info,
        output_dir=output_dir,
        output_nodes_file=output_nodes_file,
        output_morphologies_dir=output_morphologies_dir,
        parallel=parallel,
        seed=SEED,
        variant=variant,
    )
    L.info("Assigning placeholder dynamics...")
    _assign_placeholder_dynamics(
        nodes_file=output_nodes_file,
        population_name=population_name,
        seed=SEED,
    )


def _assign_morphologies(
    canonicals,
    placeholders,
    nodes_file,
    population_name,
    atlas_info,
    output_dir,
    output_nodes_file,
    output_morphologies_dir,
    parallel,
    seed,
    variant,
):
    L.info("Splitting nodes into canonical and placeholders...")
    canonical_group, placeholder_group = _split_circuit(
        canonicals=canonicals,
        nodes_file=nodes_file,
        population_name=population_name,
    )

    if canonical_group is None and placeholder_group is not None:
        L.info("No synthesized nodes. Assigning placeholder morphologies...")
        _assign_placeholder_morphologies(
            placeholders=placeholders,
            placeholder_group=placeholder_group,
            output_morphologies_dir=output_morphologies_dir,
        )
        placeholder_group.cells.save_sonata(output_nodes_file)
        L.info(
            "Output nodes consist of %d placeholder nodes written at %s",
            len(placeholder_group.cells),
            output_nodes_file,
        )
    elif canonical_group is not None and placeholder_group is None:
        L.info("No placeholder nodes. Assigning synthesized morphologies...")
        _run_topological_synthesis(
            canonicals=canonicals,
            input_nodes_file=nodes_file,
            atlas_info=atlas_info,
            output_dir=output_dir,
            output_nodes_file=output_nodes_file,
            output_morphologies_dir=output_morphologies_dir,
            seed=seed,
            parallel=parallel,
            variant=variant,
        )
        L.info(
            "Output nodes consist of %d synthesized nodes written at %s",
            len(canonical_group.cells),
            output_nodes_file,
        )
    elif canonical_group is not None and placeholder_group is not None:
        L.info("Assigning synthesized morphologies...")
        canonical_input_nodes_file = output_dir / "canonical_input_nodes.h5"
        canonical_output_nodes_file = output_dir / "canonical_output_nodes.h5"
        canonical_group.cells.save_sonata(canonical_input_nodes_file)
        _run_topological_synthesis(
            canonicals=canonicals,
            input_nodes_file=canonical_input_nodes_file,
            atlas_info=atlas_info,
            output_dir=output_dir,
            output_nodes_file=canonical_output_nodes_file,
            output_morphologies_dir=output_morphologies_dir,
            seed=seed,
            parallel=parallel,
            variant=variant,
        )
        canonical_group.cells = voxcell.CellCollection.load_sonata(
            canonical_output_nodes_file, population_name
        )

        L.info("Assigning placeholder morphologies...")
        _assign_placeholder_morphologies(
            placeholders=placeholders,
            placeholder_group=placeholder_group,
            output_morphologies_dir=output_morphologies_dir,
        )
        L.info(
            (
                "Merging:\n"
                "Synthesized:\n\tSize: %d\n\tProperties: %s\n\tHas Orientations: %s\n"
                "Placeholder:\n\tSize: %d\n\tProperties: %s\n\tHas Orientations: %s\n"
            ),
            len(canonical_group.cells),
            canonical_group.cells.properties.columns,
            canonical_group.cells.orientations is not None,
            len(placeholder_group.cells),
            placeholder_group.cells.properties.columns,
            placeholder_group.cells.orientations is not None,
        )
        merge_cell_collections(
            splits=[canonical_group, placeholder_group],
            population_name=population_name,
        ).save_sonata(output_nodes_file)
        L.info("Final merged nodes written at %s", output_nodes_file)
    else:
        raise ValueError("Both canonical and placeholder nodes are empty.")


def _run_topological_synthesis(
    canonicals,
    input_nodes_file,
    atlas_info,
    output_dir,
    output_nodes_file,
    output_morphologies_dir,
    seed,
    parallel,
    variant,
):
    L.info("Generating cell orientation field...")
    output_orientations_file = atlas_info.directory / "orientation.nrrd"
    recipe.build_cell_orientation_field(
        brain_regions=voxcell.VoxelData.load_nrrd(atlas_info.annotation_path),
        orientations=voxcell.VoxelData.load_nrrd(atlas_info.cell_orientation_field_path)
        if atlas_info.cell_orientation_field_path
        else None,
    ).save_nrrd(output_orientations_file)
    L.info("Cell orientation field written at %s", output_orientations_file)

    L.info("Generating parameters and distributions inputs...")
    parameters, distributions = recipe.build_synthesis_inputs(
        canonicals,
        region_map=voxcell.RegionMap.load_json(atlas_info.ontology_path),
    )

    tmd_parameters_file = output_dir / "tmd_parameters.json"
    utils.write_json(filepath=tmd_parameters_file, data=parameters)

    tmd_distributions_file = output_dir / "tmd_distributions.json"
    utils.write_json(filepath=tmd_distributions_file, data=distributions)

    L.info("Generating region structure...")
    if atlas_info.ph_catalog:
        region_structure = recipe.build_region_structure(atlas_info.ph_catalog)
    else:
        region_structure = {}
        L.warning("No placement hints found. An empty region_structure will be generated.")

    region_structure_file = output_dir / "region_structure.yaml"
    utils.write_yaml(filepath=region_structure_file, data=region_structure)

    _execute_synthesis_command(
        input_nodes_file=input_nodes_file,
        tmd_parameters_file=tmd_parameters_file,
        tmd_distributions_file=tmd_distributions_file,
        region_structure_file=region_structure_file,
        atlas_dir=atlas_info.directory,
        output_dir=output_dir,
        output_nodes_file=output_nodes_file,
        output_morphologies_dir=output_morphologies_dir,
        seed=seed,
        parallel=parallel,
        variant=variant,
    )


def _execute_synthesis_command(
    input_nodes_file,
    tmd_parameters_file,
    tmd_distributions_file,
    region_structure_file,
    atlas_dir,
    output_dir,
    output_nodes_file,
    output_morphologies_dir,
    seed,
    parallel,
    variant,
):
    L.info("Running topological synthesis...")

    arglist = [
        "region-grower",
        "synthesize-morphologies",
        "--input-cells",
        str(input_nodes_file),
        "--tmd-parameters",
        str(tmd_parameters_file),
        "--tmd-distributions",
        str(tmd_distributions_file),
        "--atlas",
        str(atlas_dir),
        "--seed",
        str(seed),
        "--out-cells",
        str(output_nodes_file),
        "--out-morph-dir",
        str(output_morphologies_dir),
        "--out-morph-ext",
        "h5",
        "--out-morph-ext",
        "asc",
        "--max-files-per-dir",
        "1024",
        "--out-apical",
        str(output_dir / "apical.yaml"),
        "--max-drop-ratio",
        "0.5",
        "--scaling-jitter-std",
        "0.5",
        "--rotational-jitter-std",
        "10",
        "--region-structure",
        str(region_structure_file),
        "--hide-progress-bar",
    ]

    if parallel:
        arglist.append("--with-mpi")

    cmd = " ".join(arglist)
    cmd = utils.build_variant_allocation_command(cmd, variant)

    L.info("Tool full command: %s", cmd)

    subprocess.run(cmd, check=True, shell=True)

    L.info(
        "Topological synthesis completed generating:\n\tNodes:%s\n\tMorphs:%s",
        output_nodes_file,
        output_morphologies_dir,
    )


def _split_circuit(
    canonicals,
    nodes_file,
    population_name,
):
    pairs = pd.DataFrame(
        [(region, mtype) for region, data in canonicals.items() for mtype in data],
        columns=["region", "mtype"],
    )

    cell_collection = voxcell.CellCollection.load_sonata(
        nodes_file,
        population_name=population_name,
    )

    t1, t2 = bisect_cell_collection_by_properties(cell_collection=cell_collection, properties=pairs)

    return t1, t2


def _assign_placeholder_morphologies(placeholders, placeholder_group, output_morphologies_dir):
    properties = placeholder_group.cells.properties

    # TODO: If needed sample from the available placeholders. Currently 1 per mtype.
    morphology_paths = [
        placeholders[region][mtype][0]
        for region, mtype in zip(properties["region"], properties["mtype"])
    ]

    properties["morphology"] = [Path(path).stem for path in morphology_paths]

    # copy the placeholder morphologies to the morphologies directory
    for morphology_path in set(morphology_paths):
        morph = morphio.mut.Morphology(morphology_path)
        morphology_name = Path(morphology_path).stem
        morph.write(output_morphologies_dir / f"{morphology_name}.h5")
        morph.write(output_morphologies_dir / f"{morphology_name}.asc")

    # add unit orientations
    placeholder_group.cells.orientations = np.broadcast_to(np.identity(3), (len(properties), 3, 3))


def _assign_placeholder_dynamics(nodes_file, population_name, seed: int):
    threshold_current, holding_current = _create_dynamics(nodes_file, population_name, seed)

    utils.write_node_population_with_properties(
        nodes_file=nodes_file,
        population_name=population_name,
        properties={
            "dynamics_params/threshold_current": threshold_current.astype(np.float32),
            "dynamics_params/holding_current": holding_current.astype(np.float32),
        },
        output_file=nodes_file,
    )


def _create_dynamics(nodes_file, population_name, seed):
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
    return threshold_current, holding_current


def _write_partial_config(config, nodes_file, population_name, morphologies_dir, output_file):
    """Update partial config with new nodes path and the morphology directory."""
    updated_config = utils.update_circuit_config_population(
        config=config,
        population_name=population_name,
        population_data={
            "partial": ["morphologies"],
            "alternate_morphologies": {
                "h5v1": str(morphologies_dir),
                "neurolucida-asc": str(morphologies_dir),
            },
        },
        filepath=str(nodes_file),
    )
    utils.write_json(filepath=output_file, data=updated_config)
