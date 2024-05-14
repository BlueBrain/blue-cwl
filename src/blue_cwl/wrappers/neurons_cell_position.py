"""Morphoelectrical type generator function module."""

import logging
import subprocess
from copy import deepcopy
from pathlib import Path
from typing import Any

import click
import libsonata
import pandas as pd
import voxcell
from entity_management.atlas import CellComposition
from entity_management.nexus import load_by_id
from entity_management.util import get_entity
from voxcell.nexus.voxelbrain import Atlas

from blue_cwl import Variant, nexus, recipes, registering, staging, utils, validation
from blue_cwl.statistics import mtype_etype_url_mapping, node_population_composition_summary
from blue_cwl.typing import StrOrPath

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

    return {
        "atlas-id": cell_composition.atlasRelease.get_id(),
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


def _generate_node_sets(nodes_file: Path, population_name: str, atlas_dir: Path, output_dir: Path):
    output_path = output_dir / "node_sets.json"

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
    nodes_file, node_population_name, atlas_dir, mtype_urls, etype_urls, output_file: Path
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
    circuit_resource = registering.register_partial_circuit(
        name="Cell properties partial circuit",
        brain_region_id=region_id,
        atlas_release_id=generated_data["atlas-id"],
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
        name="Cell composition summary",
        summary_file=generated_data["composition-summary-file"],
        atlas_release_id=generated_data["atlas-id"],
        derivation_entity_id=circuit_resource.get_id(),
    )
