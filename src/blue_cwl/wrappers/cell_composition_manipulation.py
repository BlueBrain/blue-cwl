# SPDX-License-Identifier: Apache-2.0

"""Composition manipulation."""

import logging
from pathlib import Path

import click
import pandas as pd
import voxcell
from entity_management.atlas import CellComposition
from entity_management.config import BrainRegionSelectorConfig, CellCompositionConfig

# pylint: disable=no-name-in-module
from entity_management.core import Entity
from entity_management.nexus import load_by_id
from entity_management.util import get_entity

from blue_cwl import density_manipulation, registering, staging, statistics, utils
from blue_cwl.density_manipulation import read_density_manipulation_recipe
from blue_cwl.exceptions import CWLRegistryError, CWLWorkflowError, SchemaValidationError
from blue_cwl.nexus import get_distribution_as_dict
from blue_cwl.typing import StrOrPath
from blue_cwl.validation import validate_schema

L = logging.getLogger(__name__)


@click.group
def app():
    """Cell composition manipulation."""


@app.command(name="stage")
@click.option("--configuration-id", required=True)
@click.option("--base-cell-composition-id", required=True)
@click.option("--brain-region-selector-config-id", required=False)
@click.option("--stage-dir", required=True)
def stage_cli(**kwargs):
    """Stage cell composition entities."""
    stage(**kwargs)


def stage(
    *,
    configuration_id: str,
    base_cell_composition_id: str,
    brain_region_selector_config_id: str | None = None,
    stage_dir: StrOrPath,
) -> None:
    """Stage cell composition entities."""
    base_composition = _stage_base_cell_composition(
        entity_id=base_cell_composition_id,
        output_dir=stage_dir,
    )
    staging.stage_atlas(
        base_composition.atlasRelease,
        output_dir=Path(stage_dir, "atlas"),
        output_file=Path(stage_dir, "atlas.json"),
    )
    _stage_manipulation_config(
        entity_id=configuration_id,
        output_file=Path(stage_dir, "recipe.parquet"),
    )
    _stage_region_selector_config(
        entity_id=brain_region_selector_config_id,
        output_file=Path(stage_dir, "region_selection.json"),
    )


def _stage_base_cell_composition(entity_id: str, output_dir: StrOrPath) -> Entity:
    base_composition = get_entity(entity_id, cls=CellComposition)
    _validate_cell_composition_schemas(base_composition)

    composition_volume = base_composition.cellCompositionVolume

    # stage first original file
    staging.stage_distribution_file(
        composition_volume,
        output_dir=output_dir,
        filename="cell_composition_volume.json",
        encoding_format="application/json",
    )

    # then materialize with paths instead of ids
    output_file = Path(output_dir, "cell_composition_volume.parquet")
    staging.materialize_cell_composition_volume(
        composition_volume,
        output_file=output_file,
    )
    L.debug("Base CellCompositionVolume materialized at %s", output_file)

    return base_composition


def _stage_region_selector_config(entity_id: str | None, output_file: StrOrPath) -> None:
    if entity_id is None:
        region_selection = []
    else:
        distribution_payload = get_distribution_as_dict(entity_id, cls=BrainRegionSelectorConfig)

        validate_schema(
            distribution_payload, schema_name="brain_region_selector_config_distribution.yml"
        )

        region_selection = [
            int(e["@id"].removeprefix("http://api.brain-map.org/api/v2/data/Structure/"))
            for e in distribution_payload["selection"]
        ]

    utils.write_json(data=region_selection, filepath=output_file)
    L.debug("Region selection written at %s", output_file)


def _stage_manipulation_config(entity_id: str, output_file: StrOrPath) -> None:
    manipulation_recipe = read_density_manipulation_recipe(
        get_distribution_as_dict(entity_id, cls=CellCompositionConfig)
    )
    manipulation_recipe.to_parquet(path=output_file)


@app.command("manipulate-cell-composition")
@click.option("--atlas-file", required=True)
@click.option("--manipulation-file", required=True)
@click.option("--region-selection-file", required=True)
@click.option("--cell-composition-volume-file", required=True)
@click.option("--materialized-cell-composition-volume-file", required=True)
@click.option("--output-dir", required=True)
def manipulate_cell_composition_cli(**kwargs):
    """Manipulate CellComposition datasets."""
    manipulate_cell_composition(**kwargs)


def manipulate_cell_composition(
    *,
    atlas_file: StrOrPath,
    manipulation_file: StrOrPath,
    region_selection_file: StrOrPath,
    cell_composition_volume_file: StrOrPath,
    materialized_cell_composition_volume_file: StrOrPath,
    output_dir: StrOrPath,
) -> None:
    """Manipulate CellComposition datasets."""
    atlas_info = staging.AtlasInfo.from_file(atlas_file)

    region_map = voxcell.RegionMap.load_json(atlas_info.ontology_path)
    brain_regions = voxcell.VoxelData.load_nrrd(atlas_info.annotation_path)

    manipulation_recipe = pd.read_parquet(manipulation_file)

    original_densities = pd.read_parquet(materialized_cell_composition_volume_file)
    original_density_release = utils.load_json(cell_composition_volume_file)

    _check_recipe_compatibility_with_density_distribution(original_densities, manipulation_recipe)

    region_selection = utils.load_json(region_selection_file) or None

    updated_densities_dir = utils.create_dir(Path(output_dir, "nrrds"))
    updated_densities, updated_density_release = density_manipulation.density_manipulation(
        updated_densities_dir,
        brain_regions,
        manipulation_recipe,
        original_densities,
        original_density_release,
        region_selection,
    )
    updated_density_release_path = Path(output_dir, "cell_composition_volume.json")
    utils.write_json(
        data=updated_density_release,
        filepath=updated_density_release_path,
    )
    L.debug("Updated CellCompositionVolume payload written at %s", updated_density_release_path)

    cell_composition_summary = statistics.atlas_densities_composition_summary(
        density_distribution=updated_densities,
        region_map=region_map,
        brain_regions=brain_regions,
        map_function="auto",
    )

    updated_cell_composition_summary_path = Path(output_dir, "cell_composition_summary.json")
    utils.write_json(
        data=cell_composition_summary,
        filepath=updated_cell_composition_summary_path,
    )
    L.debug(
        "Updated CellCompositionSummary payload written at %s",
        updated_cell_composition_summary_path,
    )


@app.command(name="register")
@click.option("--base-cell-composition-id", required=True)
@click.option("--cell-composition-volume-file", required=True)
@click.option("--cell-composition-summary-file", required=True)
@click.option("--output-dir", required=True)
def register_cli(**kwargs):
    """Register new cell composition."""
    register(**kwargs)


def register(
    *,
    base_cell_composition_id: str,
    cell_composition_volume_file: StrOrPath,
    cell_composition_summary_file: StrOrPath,
    output_dir: StrOrPath,
):
    """Register new cell composition."""
    base_cell_composition = get_entity(
        base_cell_composition_id, cls=CellComposition, resolve_context=True
    )

    atlas_release = base_cell_composition.atlasRelease

    registered_cell_composition_volume_file = Path(
        output_dir, "registered_cell_composition_volume.json"
    )
    registering.register_densities(
        atlas_release=atlas_release,
        cell_composition_volume_file=cell_composition_volume_file,
        output_file=registered_cell_composition_volume_file,
    )
    cell_composition = registering.register_cell_composition(
        atlas_release=atlas_release,
        cell_composition_volume_file=registered_cell_composition_volume_file,
        cell_composition_summary_file=cell_composition_summary_file,
    )

    _validate_cell_composition_schemas(cell_composition)

    utils.write_json(
        data=load_by_id(cell_composition.get_id()),
        filepath=Path(output_dir, "resource.json"),
    )


def _validate_cell_composition_schemas(cell_composition):
    volume_id = cell_composition.cellCompositionVolume.get_id()
    _validate_cell_composition_volume_schema(volume_id)

    summary_id = cell_composition.cellCompositionSummary.get_id()
    _validate_cell_composition_summary_schema(summary_id)


def _validate_cell_composition_summary_schema(resource_id):
    summary_data = get_distribution_as_dict(resource_id)
    try:
        validate_schema(
            data=summary_data,
            schema_name="cell_composition_summary_distribution.yml",
        )
    except SchemaValidationError as e:
        raise CWLWorkflowError(
            "Schema validation failed for CellComposition's summary.\n"
            f"CellCompositionSummary failing the validation: {resource_id}"
        ) from e


def _validate_cell_composition_volume_schema(resource_id):
    volume_data = get_distribution_as_dict(resource_id)
    try:
        validate_schema(
            data=volume_data,
            schema_name="cell_composition_volume_distribution.yml",
        )
    except SchemaValidationError as e:
        raise CWLWorkflowError(
            "Schema validation failed for CellComposition's volume distribution.\n"
            f"CellCompositionVolume failing the validation: {resource_id}"
        ) from e


def _check_recipe_compatibility_with_density_distribution(
    density_distribution: pd.DataFrame, recipe: pd.DataFrame
):
    """Check if the me combinations in recipe are present in the base density distribution."""
    merged = recipe.merge(density_distribution, on=["mtype", "etype"], indicator=True, how="left")

    only_in_recipe = recipe[merged["_merge"] == "left_only"]

    if len(only_in_recipe) > 0:

        def format_combos(df):
            rows = [
                f"('{row.mtype_url}={row.mtype}', '{row.etype_url}={row.etype}')"
                for row in df.drop_duplicates().itertuples(index=False)
            ]
            return "[\n\t" + "\n\t".join(rows) + "\n]"

        not_in_distribution = format_combos(only_in_recipe)

        raise CWLRegistryError(
            "Cell composition recipe entries not present in the cell composition volume dataset:\n"
            f"Missing entries: {not_in_distribution}"
        )
