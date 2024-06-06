# SPDX-License-Identifier: Apache-2.0

"""Placeholder emodel assignment."""

import logging
from pathlib import Path

import click
import numpy as np
import pandas as pd
import voxcell
from entity_management.nexus import load_by_id
from entity_management.simulation import DetailedCircuit
from entity_management.util import get_entity
from morph_tool.converter import convert

from blue_cwl import registering, staging, utils, validation
from blue_cwl.constants import MorphologyProducer
from blue_cwl.exceptions import CWLWorkflowError
from blue_cwl.mmodel import recipe
from blue_cwl.mmodel.entity import MorphologyAssignmentConfig
from blue_cwl.typing import StrOrPath
from blue_cwl.utils import (
    bisect_cell_collection_by_properties,
    get_partial_circuit_region_id,
    merge_cell_collections,
)
from blue_cwl.wrappers import common

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


@app.command(name="setup")
@click.option("--output-dir", required=True, help="Output directory path")
def setup_cli(output_dir):
    """Setup wrapper output directories."""
    dirs = common.setup_directories(output_dir=output_dir)
    utils.create_dir(dirs["build"] / "morphologies", clean_if_exists=True)


@app.command(name="stage")
@click.option("--configuration-id", required=True, help="me-model configuration NEXUS id.")
@click.option("--circuit-id", required=True, help="Circuit NEXUS id.")
@click.option("--stage-dir", required=True, help="Staging directory to output staged data.")
def stage_cli(**kwargs):
    """Mmodel staging cli."""
    stage(**kwargs)


def stage(*, configuration_id: str, circuit_id: str, stage_dir: StrOrPath) -> None:
    """Stage NEXUS entities into local files.

    Args:
        configuration_id: mmodel configuration NEXUS id.
        circuit_id: DetailedCircuit NEXUS id.
        stage_dir: Output directory for staging data.

    Entities required to be staged for mmodel:
        - circuit config
        - nodes file
        - atlas directory
        - atlas info file
        - mmodel configuration split in canonical/placeholder
    """
    circuit = _stage_circuit(circuit_id, stage_dir)

    staging.stage_atlas(
        circuit.atlasRelease,
        output_dir=Path(stage_dir, "atlas"),
        output_file=Path(stage_dir, "atlas.json"),
    )

    _stage_configuration(
        configuration_id=configuration_id,
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
    validation.check_properties_in_population(population_name, nodes_file, INPUT_POPULATION_COLUMNS)

    circuit_file = Path(stage_dir, "circuit_config.json")
    staging.stage_file(source=circuit_config_file, target=circuit_file)
    L.debug("Circuit %s staged at %s", circuit_id, circuit_file)

    staged_nodes_file = Path(stage_dir, "nodes.h5")
    staging.stage_file(source=nodes_file, target=staged_nodes_file)
    L.debug("Staged %s -> %s", nodes_file, staged_nodes_file)
    return entity


def _stage_configuration(configuration_id: str, output_dir: StrOrPath) -> None:
    """Stage mmodel configuration.

    Configuration is staged and split into two configs:
        - output_dir/canonical_config.json
        - output_dir/placeholder_config.json

    Each config contains configuration of synthesis and placeholder assignment respectively.
    """
    raw_config = get_entity(resource_id=configuration_id, cls=MorphologyAssignmentConfig).to_model()
    placeholders, canonicals = raw_config.expand().split()

    canonical_file = Path(output_dir, "canonical_config.json")
    L.info("Materializing canonical morphology configuration...")
    canonicals.materialize(
        output_file=canonical_file,
        labels_only=True,
    )
    L.debug("Staged synthesis config at %s", canonical_file)

    placeholder_file = Path(output_dir, "placeholder_config.json")
    L.info("Materializing placeholder morphology configuration...")
    placeholders.materialize(
        output_file=placeholder_file,
        labels_only=True,
    )
    L.debug("Staged placeholder config at %s", placeholder_file)


@app.command(name="split")
@click.option("--canonical-config-file", required=True)
@click.option("--nodes-file", required=True)
@click.option("--output-dir", required=True)
def split_cli(**kwargs):
    """Split a node population in two."""
    split(**kwargs)


def split(
    *,
    canonical_config_file: StrOrPath,
    nodes_file: StrOrPath,
    output_dir: StrOrPath,
) -> None:
    """Split node population in two.

    Two node files are created:
        - output_dir/canonicals.h5
        - output_dir/palceholders.h5

    where the canonical_config_file determines the region/mtype entries that are part of canonical
    population. The rest are automatically moved into the placeholder population.
    """
    canonicals = utils.load_json(canonical_config_file)

    pairs = pd.DataFrame(
        [(region, mtype) for region, data in canonicals.items() for mtype in data],
        columns=["region", "mtype"],
    )

    cell_collection = voxcell.CellCollection.load_sonata(nodes_file)

    t1, t2 = bisect_cell_collection_by_properties(cell_collection=cell_collection, properties=pairs)

    if t1 is None and t2 is None:
        raise ValueError("Both splits are empty.")

    canonicals_file = Path(output_dir, "canonicals.h5")

    if t1 is None:
        _empty_cell_collection(cell_collection).save_sonata(canonicals_file)
        L.info("Cells to be synthesized: 0")
    else:
        t1.save_sonata(canonicals_file)
        L.info("Cells to be synthesized: %d", len(t1))

    placeholders_file = Path(output_dir, "placeholders.h5")

    if t2 is None:
        _empty_cell_collection(cell_collection).save_sonata(placeholders_file)
        L.info("Cells to be assigned placeholders: 0")
    else:
        t2.save_sonata(placeholders_file)
        L.info("Cells to be assigned placeholders: %d", len(t2))


def _empty_cell_collection(reference: voxcell.CellCollection) -> voxcell.CellCollection:
    """Create an empty CellCollection that conforms to the attributes of the reference one."""
    cells = voxcell.CellCollection(population_name=reference.population_name)

    cells.properties = pd.DataFrame(columns=reference.properties.columns)

    cells.positions = reference.positions[:0]

    if cells.orientations is not None:
        cells.orientations = reference.orientations[:0]

    return cells


@app.command(name="transform")
@click.option("--atlas-file", required=True)
@click.option("--canonical-config-file", required=True)
@click.option("--transform-dir", required=True)
def transform_cli(**kwargs):
    """Transform."""
    transform(**kwargs)


def transform(
    *,
    atlas_file: StrOrPath,
    canonical_config_file: StrOrPath,
    transform_dir: StrOrPath,
) -> None:
    """Transform input datasets to files needed for topological synthesis.

    Files created:
        - atlas_dir/orientation.nrrd is replaced with all orientations
        - transform_dir/tmd_parameters.json
        - transform_dir/tmd_distributions.json
        - transform_dir/region_structure.yml
    """
    atlas_info = staging.AtlasInfo.from_file(atlas_file)

    _generate_cell_orientations(atlas_info)

    canonicals = utils.load_json(canonical_config_file)

    _generate_synthesis_inputs(
        canonicals,
        hierarchy_file=atlas_info.ontology_path,
        output_dir=Path(transform_dir),
    )

    _generate_region_structure(
        ph_catalog=atlas_info.ph_catalog,
        output_file=Path(transform_dir, "region_structure.yml"),
    )


@app.command(name="assign-placeholders")
@click.option("--nodes-file", required=True)
@click.option("--config-file", required=True)
@click.option("--out-nodes-file", required=True)
@click.option("--out-morphologies-dir", required=True)
def assign_placeholders_cli(**kwargs):
    """Assign placeholders."""
    assign_placeholders(**kwargs)


def assign_placeholders(
    *,
    nodes_file: StrOrPath,
    config_file: StrOrPath,
    out_morphologies_dir: StrOrPath,
    out_nodes_file: StrOrPath,
) -> None:
    """Assign placeholder morphologies to node population.

    Args:
        nodes_file: nodes file with a single node population.
        config_file: placeholder configuration for the morphology assignment.
        out_morphologies_dir: Output morphologies directory.
        out_nodes_file: Output nodes file.
    """
    cells = voxcell.CellCollection.load_sonata(nodes_file)

    if len(cells) == 0:
        L.warning("No cells to assign placeholders.")
        staging.stage_file(source=Path(nodes_file), target=Path(out_nodes_file))
        return

    placeholders = utils.load_json(config_file)

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
        convert(morphology_path, Path(out_morphologies_dir, f"{morphology_name}.h5"))
        convert(morphology_path, Path(out_morphologies_dir, f"{morphology_name}.asc"))

    # add unit orientations
    cells.orientations = np.broadcast_to(np.identity(3), (len(cells.properties), 3, 3))

    cells.save_sonata(out_nodes_file)

    L.info(
        "%d placeholder nodes written at %s",
        len(cells),
        out_nodes_file,
    )


@app.command(name="merge")
@click.option("--synthesized-nodes-file", required=True)
@click.option("--placeholder-nodes-file", required=True)
@click.option("--out-nodes-file", required=True)
def merge_cli(**kwargs):
    """Merge."""
    merge(**kwargs)


def merge(
    *,
    synthesized_nodes_file: StrOrPath,
    placeholder_nodes_file: StrOrPath,
    out_nodes_file: StrOrPath,
) -> None:
    """Merge synthesized and placeholder node populations."""
    pairs = []

    for nodes_file in [synthesized_nodes_file, placeholder_nodes_file]:
        cells = voxcell.CellCollection.load_sonata(nodes_file)

        if len(cells) > 0:
            pairs.append((nodes_file, cells))

    if len(pairs) == 1:
        output_file = pairs[0][0]
        staging.stage_file(source=Path(output_file), target=Path(out_nodes_file))
        L.debug(
            "A single population is built. Copied %s -> %s",
            output_file,
            out_nodes_file,
        )
    elif len(pairs) == 2:
        population_name = pairs[0][1].population_name
        merge_cell_collections(
            splits=[cells for _, cells in pairs],
            population_name=population_name,
        ).save_sonata(out_nodes_file)
        L.info("Final merged nodes written at %s", out_nodes_file)

    else:
        raise ValueError("Both canonical and placeholder nodes are empty.")


@app.command(name="register")
@click.option("--circuit-id", required=True)
@click.option("--nodes-file", required=True)
@click.option("--morphologies-dir", required=True)
@click.option("--output-dir", required=True)
def register_cli(**kwargs):
    """Register."""
    register(**kwargs)


def register(
    *, output_dir: StrOrPath, circuit_id: str, nodes_file: StrOrPath, morphologies_dir: StrOrPath
) -> None:
    """Register a new DetailedCircuit with the new nodes_file and morphologies_dir."""
    input_circuit = get_entity(resource_id=circuit_id, cls=DetailedCircuit)

    input_circuit_config = utils.load_json(input_circuit.circuitConfigPath.get_url_as_path())
    _, population_name = utils.get_biophysical_partial_population_from_config(input_circuit_config)

    sonata_config_file = Path(output_dir, "circuit_config.json")
    _write_partial_config(
        config=input_circuit_config,
        nodes_file=nodes_file,
        population_name=population_name,
        morphologies_dir=morphologies_dir,
        output_file=sonata_config_file,
    )
    validation.check_population_name_in_config(population_name, sonata_config_file)

    output_circuit = registering.register_partial_circuit(
        name="Partial circuit with morphologies",
        brain_region_id=get_partial_circuit_region_id(input_circuit),
        atlas_release_id=input_circuit.atlasRelease.get_id(),
        description="Partial circuit built with cell properties, and morphologies.",
        sonata_config_path=sonata_config_file,
    )
    utils.write_json(
        data=load_by_id(output_circuit.get_id()),
        filepath=Path(output_dir, "resource.json"),
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

    if output_orientations_file.exists():
        output_orientations_file.unlink()

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


def _write_partial_config(
    config: dict,
    nodes_file: StrOrPath,
    population_name: str,
    morphologies_dir: StrOrPath,
    output_file: StrOrPath,
) -> None:
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
