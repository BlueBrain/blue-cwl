"""Placeholder emodel assignment."""

import logging
import shutil
import subprocess
from pathlib import Path

import click
import numpy as np
import pandas as pd
import voxcell
from entity_management.nexus import load_by_id
from entity_management.simulation import DetailedCircuit
from entity_management.util import get_entity
from morph_tool.converter import convert

from cwl_registry import registering, staging, utils, validation
from cwl_registry.constants import MorphologyProducer
from cwl_registry.exceptions import CWLWorkflowError
from cwl_registry.mmodel import recipe
from cwl_registry.mmodel.entity import MorphologyAssignmentConfig
from cwl_registry.utils import (
    bisect_cell_collection_by_properties,
    get_partial_circuit_region_id,
    merge_cell_collections,
)
from cwl_registry.variant import Variant

SEED = 42
SONATA_MORPHOLOGY = "morphology"
SONATA_MORPHOLOGY_PRODUCER = "morphology_producer"

L = logging.getLogger(__name__)

# pylint: disable=too-many-arguments

INPUT_POPULATION_COLUMNS = [
    "mtype",
    "region",
    "subregion",
    "x",
    "y",
    "z",
]

OUTPUT_POPULATION_COLUMNS = INPUT_POPULATION_COLUMNS + [
    "morphology",
    "morphology_producer",
    "orientation_w",
    "orientation_x",
    "orientation_y",
    "orientation_z",
]


@click.group
def app():
    """Morphology synthesis of neurons."""


@app.command(name="mono-execution")
@click.option("--configuration-id", required=True)
@click.option("--circuit-id", required=True)
@click.option("--variant-id", required=True)
@click.option("--output-dir", required=True)
@click.option("--parallel", required=False, default=True)
def mono_execution(
    configuration_id,
    circuit_id,
    variant_id,
    output_dir,
    parallel,
):
    """Morphology synthesis of neuronal cells."""
    return _app(configuration_id, circuit_id, variant_id, output_dir, parallel)


def _app(configuration, partial_circuit, variant_config, output_dir, parallel):
    output_dir = utils.create_dir(Path(output_dir).resolve())
    staging_dir = utils.create_dir(output_dir / "stage")
    build_dir = utils.create_dir(output_dir / "build")
    atlas_dir = utils.create_dir(staging_dir / "atlas")
    morphologies_dir = utils.create_dir(build_dir / "morphologies", clean_if_exists=True)

    partial_circuit = get_entity(resource_id=partial_circuit, cls=DetailedCircuit)

    atlas_info = staging.stage_atlas(
        partial_circuit.atlasRelease,
        output_dir=atlas_dir,
        cell_orientation_field_basename="raw_orientation.nrrd",
    )
    raw_config = get_entity(resource_id=configuration, cls=MorphologyAssignmentConfig).to_model()
    placeholders, canonicals = raw_config.expand().split()

    L.info("Materializing canonical morphology configuration...")
    canonicals = canonicals.materialize(
        output_file=staging_dir / "materialize_canonical_config.json",
        labels_only=True,
    )
    L.info("Materializing placeholder morphology configuration...")
    placeholders = placeholders.materialize(
        output_file=staging_dir / "materialize_placeholders_config.json",
        labels_only=True,
    )

    circuit_config = utils.load_json(partial_circuit.circuitConfigPath.get_url_as_path())

    nodes_file, population_name = utils.get_biophysical_partial_population_from_config(
        circuit_config
    )
    validation.check_properties_in_population(population_name, nodes_file, INPUT_POPULATION_COLUMNS)

    variant = get_entity(resource_id=variant_config, cls=Variant)

    output_nodes_file = build_dir / "nodes.h5"
    _assign_morphologies(
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
        seed=SEED,
    )
    validation.check_properties_in_population(
        population_name, output_nodes_file, OUTPUT_POPULATION_COLUMNS
    )

    sonata_config_file = build_dir / "circuit_config.json"
    _write_partial_config(
        config=circuit_config,
        nodes_file=output_nodes_file,
        population_name=population_name,
        morphologies_dir=morphologies_dir,
        output_file=sonata_config_file,
    )
    validation.check_population_name_in_config(population_name, sonata_config_file)

    circuit = registering.register_partial_circuit(
        name="Partial circuit with morphologies",
        brain_region_id=get_partial_circuit_region_id(partial_circuit),
        atlas_release_id=partial_circuit.atlasRelease.get_id(),
        description="Partial circuit built with cell properties, and morphologies.",
        sonata_config_path=sonata_config_file,
    )
    utils.write_resource_to_definition_output(
        json_resource=load_by_id(circuit.get_id()),
        variant=variant,
        output_dir=output_dir,
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
    synthesized_file, placeholder_file = _split_circuit(
        canonicals=canonicals,
        nodes_file=nodes_file,
        population_name=population_name,
        output_dir=output_dir,
    )

    built_groups = []

    if synthesized_file:
        L.info("Generating synthesized morphologies...")
        canonical_output_nodes_file = output_dir / "canonicals.h5"
        _run_topological_synthesis(
            canonicals=canonicals,
            input_nodes_file=synthesized_file,
            atlas_info=atlas_info,
            output_dir=output_dir,
            output_nodes_file=canonical_output_nodes_file,
            output_morphologies_dir=output_morphologies_dir,
            seed=seed,
            parallel=parallel,
            variant=variant,
        )
        built_groups.append(canonical_output_nodes_file)

    if placeholder_file:
        L.info("Assigning placeholder morphologies...")
        placeholder_output_nodes_file = output_dir / "placeholders.h5"
        _run_placeholder_assignment(
            placeholders=placeholders,
            input_nodes_file=placeholder_file,
            output_morphologies_dir=output_morphologies_dir,
            output_nodes_file=placeholder_output_nodes_file,
        )
        built_groups.append(placeholder_output_nodes_file)

    if len(built_groups) == 1:
        output_file = built_groups[0]
        shutil.move(output_file, output_nodes_file)
        L.debug("A single population is built. Moved %s -> %s", output_file, output_nodes_file)
    elif len(built_groups) == 2:
        merge_cell_collections(
            splits=[voxcell.CellCollection.load_sonata(p) for p in built_groups],
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
    # create cell orientations in atlas directory
    _generate_cell_orientations(atlas_info)

    tmd_parameters_file, tmd_distributions_file = _generate_synthesis_inputs(
        canonicals,
        hierarchy_file=atlas_info.ontology_path,
        output_dir=output_dir,
    )
    region_structure_file = _generate_region_structure(
        ph_catalog=atlas_info.ph_catalog,
        output_file=output_dir / "region_structure.yaml",
    )
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
    cells = voxcell.CellCollection.load_sonata(output_nodes_file)
    properties = cells.properties

    # Add morphology_producer column if not existent
    if SONATA_MORPHOLOGY_PRODUCER not in properties.columns:
        properties[SONATA_MORPHOLOGY_PRODUCER] = MorphologyProducer.SYNTHESIS
        cells.save_sonata(output_nodes_file)
        L.warning("morphology_producer column did not exist and was added in synthesized nodes.")

    L.info(
        "%d synthesized nodes written at %s",
        len(cells),
        output_nodes_file,
    )


def _generate_cell_orientations(atlas_info):
    """Generate cell orientations from atlas information."""
    L.info("Generating cell orientation field...")

    orientations = (
        voxcell.VoxelData.load_nrrd(atlas_info.cell_orientation_field_path)
        if atlas_info.cell_orientation_field_path
        else None
    )

    orientation_field = recipe.build_cell_orientation_field(
        brain_regions=voxcell.VoxelData.load_nrrd(atlas_info.annotation_path),
        orientations=orientations,
    )

    output_orientations_file = atlas_info.directory / "orientation.nrrd"
    orientation_field.save_nrrd(output_orientations_file)

    L.info("Cell orientation field written at %s", output_orientations_file)

    return output_orientations_file


def _generate_synthesis_inputs(
    canonicals,
    hierarchy_file: Path,
    output_dir: Path,
) -> tuple[Path, Path]:
    """Generate input parameter and distribution files for topological synthesis."""
    L.info("Generating parameters and distributions inputs...")

    parameters, distributions = recipe.build_synthesis_inputs(
        canonicals,
        region_map=voxcell.RegionMap.load_json(hierarchy_file),
    )

    tmd_parameters_file = output_dir / "tmd_parameters.json"
    utils.write_json(filepath=tmd_parameters_file, data=parameters)

    tmd_distributions_file = output_dir / "tmd_distributions.json"
    utils.write_json(filepath=tmd_distributions_file, data=distributions)

    return tmd_parameters_file, tmd_distributions_file


def _generate_region_structure(ph_catalog: dict | None, output_file: Path) -> Path:
    """Generate input region structure for region grower."""
    if ph_catalog is not None:
        region_structure: dict = recipe.build_region_structure(ph_catalog)
        L.debug(
            "Generated synthesis region structure at %s from placement hints at %s",
            output_file,
            ph_catalog,
        )
    else:
        region_structure = {}
        L.warning("No placement hints found. An empty region_structure will be generated.")

    utils.write_yaml(filepath=output_file, data=region_structure)

    return output_file


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
        "10000",
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
    output_dir,
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

    # switch to using files instead of the populations
    if t1 and t2:
        t1_path = output_dir / "canonicals.h5"
        t1.save_sonata(t1_path)
        L.info("Cells to be synthesized: %d", len(t1))

        t2_path = output_dir / "placeholders.h5"
        t2.save_sonata(t2_path)

        L.info("Cells to be assigned placeholders: %d", len(t2))

        return t1_path, t2_path

    if t1 and not t2:
        L.info("Cells to be synthesized: %d", len(cell_collection))
        return nodes_file, None

    if t2 and not t1:
        L.info("Cells to be assigned placeholders: %d", len(cell_collection))
        return None, nodes_file

    raise ValueError("Both splits are empty.")


def _run_placeholder_assignment(
    placeholders, input_nodes_file, output_morphologies_dir, output_nodes_file=None
):
    cells = voxcell.CellCollection.load_sonata(input_nodes_file)

    df_placeholders = pd.DataFrame(
        [
            (mtype, etype, etype_data[0])
            for mtype, mtype_data in placeholders.items()
            for etype, etype_data in mtype_data.items()
        ],
        columns=["region", "mtype", "path"],
    )

    # add morphology column from the path stems
    df_placeholders[SONATA_MORPHOLOGY] = df_placeholders["path"].apply(lambda e: Path(e).stem)

    # get unique values and remove from dataframe
    unique_morphology_paths = df_placeholders["path"].unique()

    # avoid adding the path to the properties df when merging below
    df_placeholders.drop(columns="path", inplace=True)

    if set(df_placeholders.columns) != {"region", "mtype", SONATA_MORPHOLOGY}:
        raise CWLWorkflowError(
            "Unexpected columns encountered:\n"
            f"Expected   : (region, mtype, {SONATA_MORPHOLOGY})\n"
            f"Encountered: {df_placeholders.columns}"
        )

    # add morphology column via merge with the placeholder entries
    cells.properties = pd.merge(
        cells.properties,
        df_placeholders,
        how="left",
        on=["region", "mtype"],
    )

    if cells.properties[SONATA_MORPHOLOGY].isnull().any():
        raise CWLWorkflowError("Null entries encountered in morphology column.")

    cells.properties[SONATA_MORPHOLOGY_PRODUCER] = MorphologyProducer.PLACEHOLDER

    # use morphology unique paths to copy the placeholder morphologies to the morphologies directory
    for morphology_path in unique_morphology_paths:
        morphology_name = Path(morphology_path).stem
        convert(morphology_path, output_morphologies_dir / f"{morphology_name}.h5")
        convert(morphology_path, output_morphologies_dir / f"{morphology_name}.asc")

    # add unit orientations
    cells.orientations = np.broadcast_to(np.identity(3), (len(cells.properties), 3, 3))

    cells.save_sonata(output_nodes_file)

    L.info(
        "%d placeholder nodes written at %s",
        len(cells),
        output_nodes_file,
    )


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
