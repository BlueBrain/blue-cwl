# SPDX-License-Identifier: Apache-2.0

"""Morphoelectrical type generator function module."""

import logging
from pathlib import Path

import click
import libsonata
import pandas as pd
import voxcell
from brainbuilder.app.cells import place
from brainbuilder.app.targets import node_sets
from entity_management.atlas import CellComposition
from entity_management.util import get_entity
from voxcell.nexus.voxelbrain import Atlas

from blue_cwl import nexus, recipes, registering, staging, utils, validation
from blue_cwl.statistics import (
    mtype_etype_url_mapping,
    node_population_composition_summary,
)
from blue_cwl.typing import StrOrPath
from blue_cwl.wrappers import common

SEED = 42
STAGE_DIR_NAME = "stage"
TRANSFORM_DIR_NAME = "transform"
EXECUTE_DIR_NAME = "build"


L = logging.getLogger(__name__)


OUTPUT_POPULATION_COLUMNS = [
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
]


@click.group
def app():
    """Cell placement."""


@app.command(name="stage")
@click.option("--region-id", required=True, help="Region NEXUS ID")
@click.option("--cell-composition-id", required=True, help="CellComposition entity id to stage.")
@click.option("--configuration-id", required=True)
@click.option("--stage-dir", required=True, help="Staging directory to use.")
def stage_cli(**kwargs):
    """Stage placement entities."""
    stage(**kwargs)


def stage(
    *,
    region_id: str,
    cell_composition_id: str,
    configuration_id: str,
    stage_dir: StrOrPath,
) -> None:
    """Stage entities."""
    utils.create_dir(stage_dir)

    region_acronym = nexus.get_region_acronym(region_id)
    region_file = Path(stage_dir, "region.txt")
    utils.write_text(text=region_acronym, filepath=region_file)
    L.debug("Region %s acronym '%s' written at %s", region_id, region_acronym, region_file)

    cell_composition = get_entity(resource_id=cell_composition_id, cls=CellComposition)

    atlas = cell_composition.atlasRelease
    atlas_dir = utils.create_dir(Path(stage_dir, "atlas"))
    atlas_file = Path(stage_dir, "atlas.json")
    staging.stage_atlas(
        atlas,
        output_dir=atlas_dir,
        output_file=atlas_file,
    )
    L.debug("Atlas %s staged at %s.", atlas.get_id(), atlas_file)

    cell_composition_volume = cell_composition.cellCompositionVolume
    cell_composition_volume_file = Path(stage_dir, "densities.parquet")
    staging.materialize_cell_composition_volume(
        cell_composition_volume,
        output_file=cell_composition_volume_file,
    )
    L.debug(
        "Cell composition's %s volume %s staged at %s.",
        cell_composition.get_id(),
        cell_composition_volume.get_id(),
        cell_composition_volume_file,
    )

    config_file = Path(stage_dir, "config.json")
    staging.stage_distribution_file(
        configuration_id,
        output_dir=stage_dir,
        filename=config_file.name,
    )
    L.debug("Configuration staged at %s", config_file)


@app.command(name="transform")
@click.option("--region-file", required=True)
@click.option("--densities-file", required=True)
@click.option("--transform-dir", required=True)
def transform_cli(**kwargs):
    """Transform CLI."""
    transform(**kwargs)


def transform(*, region_file: str, densities_file: StrOrPath, transform_dir: StrOrPath):
    """Create cell composition and taxonomy files."""
    utils.create_dir(transform_dir)

    region_acronym = utils.load_text(region_file)

    me_type_densities = pd.read_parquet(densities_file)
    composition_file = Path(transform_dir, "mtype_composition.yml")
    composition = recipes.build_cell_composition_from_me_densities(
        region_acronym, me_type_densities
    )
    utils.write_yaml(composition_file, composition)
    L.debug("Cell composition recipe written at %s", composition_file)

    mtypes = me_type_densities["mtype"].drop_duplicates().values.tolist()

    mtype_taxonomy_file = Path(transform_dir, "mtype_taxonomy.tsv")
    mtype_taxonomy = recipes.build_mtype_taxonomy(mtypes)
    mtype_taxonomy.to_csv(mtype_taxonomy_file, sep=" ", index=False)
    L.debug("MType taxonomy file written at %s", mtype_taxonomy_file)


@app.command(name="build")
@click.option("--build-dir", required=True)
@click.option("--atlas-file", required=True)
@click.option("--region-file", required=True)
@click.option("--composition-file", required=True)
@click.option("--mtype-taxonomy-file", required=True)
@click.option("--densities-file", required=True)
@click.option("--configuration-file", required=True)
def build_cli(**kwargs):
    """Place cells CLI."""
    build(**kwargs)


def build(
    *,
    build_dir: StrOrPath,
    atlas_file: StrOrPath,
    region_file: StrOrPath,
    composition_file: StrOrPath,
    mtype_taxonomy_file: StrOrPath,
    densities_file: StrOrPath,
    configuration_file: StrOrPath,
) -> None:
    """Place cells."""
    utils.create_dir(build_dir)

    atlas_info = staging.AtlasInfo.from_file(atlas_file)

    configuration = utils.load_json(configuration_file)["place_cells"]

    region = utils.load_text(region_file)
    node_population_name = f"{region}__neurons"
    L.info("Region: %s Population Name: %s", region, node_population_name)

    L.info("Initializing cell population...")
    init_cells_file = Path(build_dir, "init_cells.h5")
    _init_cells(output_file=init_cells_file, node_population_name=node_population_name)

    atlas_cache_dir = utils.create_dir(Path(build_dir, ".atlas"))

    sort_by = configuration.get("sort_by", None)

    L.info("Placing cell population...")
    nodes_file = Path(build_dir, "nodes.h5")
    place(
        composition=str(composition_file),
        mtype_taxonomy=str(mtype_taxonomy_file),
        atlas=str(atlas_info.directory),
        mini_frequencies=None,
        atlas_cache=str(atlas_cache_dir),
        region=region,
        mask=None,
        density_factor=float(configuration.get("density_factor", 1.0)),
        soma_placement=configuration.get("soma_placement", "basic"),
        atlas_property=[
            ("region", "~brain_regions"),
            ("hemisphere", "hemisphere"),
        ],
        sort_by=",".join(sort_by) if sort_by else None,
        append_hemisphere=False,
        seed=int(configuration.get("seed", SEED)),
        output=str(nodes_file),
        input_path=str(init_cells_file),
    )

    L.info("Validating nodes at %s", nodes_file)
    validation.check_population_name_in_nodes(node_population_name, nodes_file)
    validation.check_properties_in_population(
        node_population_name, nodes_file, OUTPUT_POPULATION_COLUMNS
    )
    L.info("Validation of generated nodes completed successfully.")

    L.info("Generating node sets...")
    node_sets_file = Path(build_dir, "node_sets.json")
    node_sets(
        cells_path=str(nodes_file),
        full_hierarchy=str(atlas_info.ontology_path),
        atlas=str(atlas_info.directory),
        atlas_cache=str(atlas_cache_dir),
        allow_empty=True,
        population=node_population_name,
        output=str(node_sets_file),
        targets=None,
    )
    L.info("Node sets written at %s", node_sets_file)

    L.info("Generating circuit config...")
    sonata_config_file = Path(build_dir, "circuit_config.json")
    _generate_circuit_config(
        node_sets_file=node_sets_file,
        node_population_name=node_population_name,
        nodes_file=nodes_file,
        output_file=sonata_config_file,
    )

    L.info("Validating circuit config at %s", sonata_config_file)
    validation.check_population_name_in_config(node_population_name, sonata_config_file)

    L.info("Generating cell composition summary...")
    mtype_urls, etype_urls = mtype_etype_url_mapping(pd.read_parquet(densities_file))

    composition_summary_file = Path(build_dir, "cell_composition_summary.json")
    _generate_cell_composition_summary(
        nodes_file=nodes_file,
        node_population_name=node_population_name,
        atlas_dir=atlas_info.directory,
        mtype_urls=mtype_urls,
        etype_urls=etype_urls,
        output_file=composition_summary_file,
    )


def _init_cells(*, output_file: StrOrPath, node_population_name: str) -> None:
    voxcell.CellCollection(population_name=node_population_name).save_sonata(output_file)
    L.debug("Initialized node population '%s' at %s", node_population_name, output_file)


@app.command(name="register")
@click.option("--region-id", required=True)
@click.option("--cell-composition-id", required=True)
@click.option("--circuit-file", required=True)
@click.option("--summary-file", required=True)
@click.option("--output-dir", required=True)
@click.option("--output-resource-file", required=True)
def register_cli(**kwargs):
    """Register entities."""
    register(**kwargs)


def register(
    *,
    region_id,
    cell_composition_id,
    circuit_file,
    summary_file,
    output_dir: StrOrPath,
    output_resource_file,
):
    """Register outputs to nexus."""
    cell_composition = get_entity(cell_composition_id, cls=CellComposition)
    atlas_release = cell_composition.atlasRelease

    circuit = registering.register_partial_circuit(
        name="Cell properties partial circuit",
        brain_region_id=region_id,
        atlas_release=atlas_release,
        description="Partial circuit built with cell positions and me properties.",
        sonata_config_path=circuit_file,
    )

    output_summary_resource_file = Path(output_dir, "summary_resource.json")
    # pylint: disable=no-member
    summary = registering.register_cell_composition_summary(
        name="Cell composition summary",
        description="Cell composition summary",
        distribution_file=summary_file,
        atlas_release=atlas_release,
        derivation_entity=circuit,
    )
    common.write_entity_id_to_file(entity=summary, output_file=output_summary_resource_file)
    L.debug("Summary jsonld resource written at %s", output_summary_resource_file)

    # this is the required workflow output resource.
    common.write_entity_id_to_file(
        entity=circuit,
        output_file=output_resource_file,
    )
    L.debug("Circuit jsonld resource written at %s", output_resource_file)


def _generate_circuit_config(
    node_sets_file: StrOrPath,
    node_population_name: str,
    nodes_file: StrOrPath,
    output_file: StrOrPath,
):
    config = {
        "version": 2,
        "manifest": {"$BASE_DIR": "."},
        "node_sets_file": str(node_sets_file),
        "networks": {
            "nodes": [
                {
                    "nodes_file": str(nodes_file),
                    "populations": {
                        node_population_name: {
                            "type": "biophysical",
                            "partial": ["cell-properties"],
                        }
                    },
                }
            ],
            # TODO: To be removed when libsonata==0.1.17 is widely deployed
            "edges": [],
        },
        "metadata": {"status": "partial"},
    }

    utils.write_json(filepath=output_file, data=config)

    return config


def _generate_cell_composition_summary(
    nodes_file,
    node_population_name,
    atlas_dir,
    mtype_urls,
    etype_urls,
    output_file: Path,
):
    atlas = Atlas.open(str(atlas_dir))
    population = libsonata.NodeStorage(nodes_file).open_population(node_population_name)

    composition_summary = node_population_composition_summary(
        population, atlas, mtype_urls, etype_urls
    )
    utils.write_json(filepath=output_file, data=composition_summary)
