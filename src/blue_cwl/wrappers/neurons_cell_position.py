# SPDX-License-Identifier: Apache-2.0

"""Morphoelectrical type generator function module."""

import logging
import subprocess
from copy import deepcopy
from pathlib import Path
from typing import Any

import click
import libsonata
import numpy as np
import pandas as pd
import voxcell
from brainbuilder.app.cells import _place as place
from entity_management.atlas import AtlasRelease, CellComposition
from entity_management.nexus import load_by_id
from entity_management.util import get_entity
from voxcell.nexus.voxelbrain import Atlas

from blue_cwl import Variant, nexus, recipes, registering, staging, utils, validation
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
@click.option("--staging-dir", required=True, help="Staging directory to use.")
def stage_cli(**kwargs):
    """Stage placement entities."""
    stage(**kwargs)


def stage(
    *,
    region_id: str,
    cell_composition_id: str,
    configuration_id: str,
    staging_dir: StrOrPath,
) -> None:
    """Stage entities."""
    utils.create_dir(staging_dir)

    region_acronym = nexus.get_region_acronym(region_id)
    region_file = Path(staging_dir, "region.txt")
    utils.write_text(text=region_acronym, filepath=region_file)
    L.debug("Region %s acronym '%s' written at %s", region_id, region_acronym, region_file)

    cell_composition = get_entity(resource_id=cell_composition_id, cls=CellComposition)

    atlas = cell_composition.atlasRelease
    atlas_dir = utils.create_dir(Path(staging_dir, "atlas"))
    atlas_file = Path(staging_dir, "atlas.json")
    staging.stage_atlas(
        atlas,
        output_dir=atlas_dir,
        output_file=atlas_file,
    )
    L.debug("Atlas %s staged at %s.", atlas.get_id(), atlas_file)

    cell_composition_volume = cell_composition.cellCompositionVolume
    cell_composition_volume_file = Path(staging_dir, "densities.parquet")
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

    config_file = Path(staging_dir, "config.json")
    staging.stage_distribution_file(
        configuration_id,
        output_dir=staging_dir,
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


@app.command(name="generate")
@click.option("--build-dir", required=True)
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
) -> None:
    """Place cells."""
    utils.create_dir(build_dir)

    atlas_info = staging.AtlasInfo.from_file(atlas_file)

    region = utils.load_text(region_file)
    node_population_name = f"{region}__neurons"
    L.info("Region: %s Population Name: %s", region, node_population_name)

    L.info("Initializing cell population...")
    init_cells_file = _init_cells(output_dir=build_dir, node_population_name=node_population_name)

    L.info("Placing cell population...")
    nodes_file = _place_cells(
        region=region,
        atlas_dir=atlas_info.directory,
        init_cells_file=init_cells_file,
        composition_file=composition_file,
        mtype_taxonomy_file=mtype_taxonomy_file,
        output_dir=build_dir,
    )

    L.info("Validating nodes at %s", nodes_file)
    validation.check_population_name_in_nodes(node_population_name, nodes_file)
    validation.check_properties_in_population(
        node_population_name, nodes_file, OUTPUT_POPULATION_COLUMNS
    )

    L.info("Generating node sets...")
    node_sets_file = _generate_node_sets(
        nodes_file=nodes_file,
        population_name=node_population_name,
        atlas_dir=atlas_info.directory,
        output_dir=build_dir,
    )

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


def _init_cells(output_dir: StrOrPath, node_population_name: str):
    init_cells_file = Path(output_dir, "init_nodes.h5")
    cells = voxcell.CellCollection(node_population_name)
    cells.save(init_cells_file)

    L.debug("Initialized node population '%s' at %s", node_population_name, init_cells_file)

    return init_cells_file


def _place_cells(
    *,
    region: str,
    atlas_dir: StrOrPath,
    init_cells_file: StrOrPath,
    composition_file: StrOrPath,
    mtype_taxonomy_file: StrOrPath,
    output_dir: StrOrPath,
):
    nodes_file = Path(output_dir, "nodes.h5")

    np.random.seed(SEED)

    cells = place(
        input_path=init_cells_file,
        composition_path=composition_file,
        mtype_taxonomy_path=mtype_taxonomy_file,
        mini_frequencies_path=None,
        atlas_url=atlas_dir,
        atlas_cache=Path(output_dir, ".atlas"),
        region=region,
        mask_dset=None,
        soma_placement="basic",
        density_factor=1.0,
        atlas_properties=[
            ("region", "~brain_regions"),
            ("hemisphere", "hemisphere"),
        ],
        sort_by=["region", "mtype"],
    )
    cells.save(nodes_file)

    L.debug("Placed cell population at %s", nodes_file)

    return nodes_file


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


@app.command(name="mono-execution")
@click.option("--region", required=True)
@click.option("--variant-id", required=False)
@click.option("--configuration-id", required=True)
@click.option("--cell-composition-id", required=True)
@click.option("--output-dir", required=True)
def mono_execution(region, variant_id, configuration_id, cell_composition_id, output_dir):
    """Morphoelectrical type generator cli entry."""
    output_dir = utils.create_dir(Path(output_dir).resolve())

    L.warning("Configuration %s is currently not taken into account.", configuration_id)

    staged_entities = _extract(
        region,
        variant_id,
        cell_composition_id,
        output_dir,
    )

    transform_dir = utils.create_dir(output_dir / TRANSFORM_DIR_NAME)
    transformed_entities = _transform(staged_entities, output_dir=transform_dir)

    generated_entities = _generate(transformed_entities, output_dir)

    _register(
        region,
        generated_entities,
        output_dir,
    )


def _extract(
    brain_region_id: str,
    variant_config_id: str,
    cell_composition_id: str,
    output_dir: Path,
) -> dict[str, Any]:
    """Stage resources from the knowledge graph."""
    staging_dir = utils.create_dir(output_dir / STAGE_DIR_NAME)
    atlas_dir = utils.create_dir(staging_dir / "atlas")
    me_type_densities_file = staging_dir / "mtype-densities.parquet"

    variant = get_entity(resource_id=variant_config_id, cls=Variant)

    region = nexus.get_region_acronym(brain_region_id)

    cell_composition = get_entity(resource_id=cell_composition_id, cls=CellComposition)

    staging.stage_atlas(
        cell_composition.atlasRelease,
        output_dir=atlas_dir,
    )
    staging.materialize_cell_composition_volume(
        cell_composition.cellCompositionVolume,
        output_file=me_type_densities_file,
    )

    atlas_release = cell_composition.atlasRelease

    full_atlas_id = utils.url_with_revision(url=atlas_release.get_id(), rev=atlas_release.get_rev())

    return {
        "atlas-id": full_atlas_id,
        "region": region,
        "atlas-dir": atlas_dir,
        "me-type-densities-file": me_type_densities_file,
        "variant": variant,
    }


def _transform(staged_data: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    """Trasform the staged resources into the algorithm's inputs, if needed."""
    region = staged_data["region"]

    me_type_densities = pd.read_parquet(staged_data["me-type-densities-file"])

    composition_file = output_dir / "cell_composition.yaml"
    composition = recipes.build_cell_composition_from_me_densities(region, me_type_densities)
    utils.write_yaml(composition_file, composition)

    mtypes = me_type_densities["mtype"].drop_duplicates().values.tolist()

    mtype_taxonomy_file = output_dir / "mtype_taxonomy.tsv"
    mtype_taxonomy = recipes.build_mtype_taxonomy(mtypes)
    mtype_taxonomy.to_csv(mtype_taxonomy_file, sep=" ", index=False)

    transformed_data = deepcopy(staged_data)
    transformed_data.update(
        {
            "composition-file": composition_file,
            "mtype-taxonomy-file": mtype_taxonomy_file,
        }
    )
    return transformed_data


def _generate(transformed_data: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    """Generation step where the algorithm is executed and outputs are created."""
    build_dir = utils.create_dir(output_dir / "build")

    region = transformed_data["region"]

    nodes_file = build_dir / "nodes.h5"
    node_population_name = f"{region}__neurons"

    init_cells_file = build_dir / "init_nodes.h5"
    cells = voxcell.CellCollection(node_population_name)
    cells.save(init_cells_file)

    cmd = list(
        map(
            str,
            (
                "brainbuilder",
                "cells",
                "place",
                "--composition",
                transformed_data["composition-file"],
                "--mtype-taxonomy",
                transformed_data["mtype-taxonomy-file"],
                "--atlas",
                transformed_data["atlas-dir"],
                "--atlas-cache",
                output_dir / ".atlas",
                "--region",
                region,
                "--soma-placement",
                "basic",
                "--density-factor",
                1.0,
                "--atlas-property",
                "region ~brain_regions",
                "--atlas-property",
                "hemisphere hemisphere",
                "--sort-by",
                "region,mtype",
                "--seed",
                SEED,
                "--output",
                nodes_file,
                "--input",
                init_cells_file,
            ),
        )
    )
    str_command = " ".join(cmd)
    L.debug("Command: %s", str_command)
    subprocess.run(
        str_command,
        check=True,
        capture_output=False,
        shell=True,
    )

    validation.check_population_name_in_nodes(node_population_name, nodes_file)
    validation.check_properties_in_population(
        node_population_name, nodes_file, OUTPUT_POPULATION_COLUMNS
    )

    node_sets_file = _generate_node_sets(
        nodes_file=nodes_file,
        population_name=node_population_name,
        atlas_dir=transformed_data["atlas-dir"],
        output_dir=build_dir,
    )

    L.info("Generating partial circuit config...")
    sonata_config_file = build_dir / "config.json"
    _generate_circuit_config(
        node_sets_file=node_sets_file,
        node_population_name=node_population_name,
        nodes_file=nodes_file,
        output_file=sonata_config_file,
    )
    validation.check_population_name_in_config(node_population_name, sonata_config_file)

    L.info("Generating cell composition summary...")
    mtype_urls, etype_urls = mtype_etype_url_mapping(
        pd.read_parquet(transformed_data["me-type-densities-file"])
    )
    composition_summary_file = build_dir / "cell_composition_summary.json"
    _generate_cell_composition_summary(
        nodes_file=nodes_file,
        node_population_name=node_population_name,
        atlas_dir=transformed_data["atlas-dir"],
        mtype_urls=mtype_urls,
        etype_urls=etype_urls,
        output_file=composition_summary_file,
    )

    ret = deepcopy(transformed_data)
    ret.update(
        {
            "partial-circuit": sonata_config_file,
            "composition-summary-file": composition_summary_file,
        }
    )
    return ret


def _generate_node_sets(
    nodes_file: Path, population_name: str, atlas_dir: Path, output_dir: StrOrPath
):
    output_path = Path(output_dir, "node_sets.json")

    L.info("Generating node sets for the placed cells at %s", output_path)

    cmd = list(
        map(
            str,
            (
                "brainbuilder",
                "targets",
                "node-sets",
                "--atlas",
                atlas_dir,
                "--full-hierarchy",
                "--allow-empty",
                "--population",
                population_name,
                "--output",
                output_path,
                nodes_file,
            ),
        )
    )
    str_command = " ".join(cmd)
    L.debug("Command: %s", str_command)
    subprocess.run(
        str_command,
        check=True,
        capture_output=False,
        shell=True,
    )

    return output_path


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


def _register(
    region_id,
    generated_data,
    output_dir,
):
    """Register outputs to nexus."""
    atlas_release = get_entity(generated_data["atlas-id"], cls=AtlasRelease)

    circuit_resource = registering.register_partial_circuit(
        name="Cell properties partial circuit",
        brain_region_id=region_id,
        atlas_release=atlas_release,
        description="Partial circuit built with cell positions and me properties.",
        sonata_config_path=generated_data["partial-circuit"],
    )
    # write the circuit resource to the respective output file specified by the definition
    utils.write_resource_to_definition_output(
        json_resource=load_by_id(circuit_resource.get_id()),
        variant=generated_data["variant"],
        output_dir=output_dir,
    )
    # pylint: disable=no-member
    registering.register_cell_composition_summary(
        name="Cell Composition Summary",
        description="Cell Composition Summary of Node Population",
        distribution_file=generated_data["composition-summary-file"],
        atlas_release=atlas_release,
        derivation_entity=circuit_resource,
    )
