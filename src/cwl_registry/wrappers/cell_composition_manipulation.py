"""Composition manipulation."""
import logging
from pathlib import Path

import click
import pandas as pd
import voxcell

# pylint: disable=no-name-in-module
from bba_data_push.bba_dataset_push import push_cellcomposition
from bba_data_push.push_cellComposition import register_densities
from entity_management.atlas import CellComposition
from entity_management.config import CellCompositionConfig
from entity_management.nexus import load_by_id

from cwl_registry import density_manipulation, staging, utils
from cwl_registry.density_manipulation import read_density_manipulation_recipe
from cwl_registry.exceptions import CWLRegistryError, CWLWorkflowError, SchemaValidationError
from cwl_registry.nexus import get_distribution_as_dict, get_entity, get_forge, get_resource
from cwl_registry.validation import validate_schema
from cwl_registry.variant import Variant

L = logging.getLogger(__name__)


@click.command()
@click.option("--region", required=True)
@click.option("--base-cell-composition", required=True)
@click.option("--configuration", help="Recipe for manipulations")
@click.option("--variant-config", required=True)
@click.option("--output-dir", required=True)
def app(  # pylint: disable=too-many-arguments
    region,  # pylint: disable=unused-argument
    base_cell_composition,
    configuration,
    variant_config,
    output_dir,
):
    """Density Manipulation CLI"""
    output_dir = utils.create_dir(Path(output_dir).resolve())
    staging_dir = utils.create_dir(output_dir / "stage")
    atlas_dir = utils.create_dir(staging_dir / "atlas")
    build_dir = utils.create_dir(output_dir / "build")

    cell_composition = get_entity(base_cell_composition, cls=CellComposition)
    _validate_cell_composition_schemas(cell_composition)

    original_cell_composition_summary = staging.materialize_cell_composition_summary(
        cell_composition.cellCompositionSummary,
        output_file=staging_dir / "original_cell_composition_summary.parquet",
    )

    # the materialized version that has gpfs paths instead of ids
    original_densities = staging.materialize_cell_composition_volume(
        cell_composition.cellCompositionVolume,
        output_file=staging_dir / "original_cell_composition_volume.parquet",
    )

    L.info("Staging atlas to  %s", atlas_dir)
    atlas_info = staging.stage_atlas(
        cell_composition.atlasRelease,
        output_dir=atlas_dir,
    )

    manipulation_recipe = read_density_manipulation_recipe(
        get_distribution_as_dict(configuration, cls=CellCompositionConfig)
    )
    manipulation_recipe.to_parquet(path=staging_dir / "manipulation_recipe.parquet")

    _check_recipe_compatibility_with_density_distribution(original_densities, manipulation_recipe)

    # the original registered version
    original_density_release = cell_composition.cellCompositionVolume.distribution.as_dict()
    utils.write_json(
        data=original_density_release,
        filepath=staging_dir / "original_density_release.json",
    )

    region_map = voxcell.RegionMap.load_json(atlas_info.ontology_path)
    brain_regions = voxcell.VoxelData.load_nrrd(atlas_info.annotation_path)

    L.info("Updating density distribution...")
    updated_densities_dir = utils.create_dir(build_dir / "updated_densities_dir")
    updated_densities, updated_density_release = density_manipulation.density_manipulation(
        updated_densities_dir,
        brain_regions,
        manipulation_recipe,
        original_densities,
        original_density_release,
    )
    updated_density_release_path = build_dir / "updated_density_release.json"
    utils.write_json(
        data=updated_density_release,
        filepath=updated_density_release_path,
    )
    L.info("Updated CellCompositionVolume release written at %s", updated_density_release_path)

    L.info("Updating cell composition summary statistics...")
    cell_composition_summary = density_manipulation.update_composition_summary_statistics(
        brain_regions,
        region_map,
        original_cell_composition_summary,
        updated_densities,
    )

    updated_cell_composition_summary_path = build_dir / "updated_cell_composition_summary.json"
    utils.write_json(
        data=cell_composition_summary,
        filepath=updated_cell_composition_summary_path,
    )

    cell_composition_id = _register_cell_composition(
        volume_path=updated_density_release_path,
        summary_path=updated_cell_composition_summary_path,
        base_cell_composition=cell_composition,
        hierarchy_path=atlas_info.ontology_path,
    )

    cell_composition = get_entity(cell_composition_id, cls=CellComposition)
    _validate_cell_composition_schemas(cell_composition)

    utils.write_resource_to_definition_output(
        json_resource=load_by_id(cell_composition_id),
        variant=get_entity(variant_config, cls=Variant),
        output_dir=output_dir,
    )


def _register_cell_composition(volume_path, summary_path, hierarchy_path, base_cell_composition):
    L.info("Registering nrrd densities...")

    forge = get_forge(force_refresh=True)

    atlas_release_id = base_cell_composition.atlasRelease.get_id()
    atlas_release_rev = base_cell_composition.atlasRelease.get_rev()
    atlas_release = get_resource(
        forge=forge,
        resource_id=utils.url_with_revision(
            url=atlas_release_id,
            rev=atlas_release_rev,
        ),
    )

    # convert the entity to forge resource to pass into the push module
    base_cell_composition = get_resource(
        forge=forge,
        resource_id=utils.url_with_revision(
            url=base_cell_composition.get_id(),
            rev=base_cell_composition.get_rev(),
        ),
    )

    L.debug("Atlas release id in CellComposition: %s", atlas_release.id)

    output_volume_path = Path(str(volume_path).replace(".json", "_id_only.json"))

    L.info("Registering nrrd densities...")
    register_densities(
        volume_path=volume_path,
        atlas_release_prop=atlas_release,
        forge=forge,
        subject=atlas_release.subject,
        brain_location_prop=atlas_release.brainLocation,
        reference_system_prop=None,
        contribution=None,
        derivation=None,
        resource_tag=None,
        force_registration=True,
        dryrun=False,
        output_volume_path=output_volume_path,
    )

    L.info("Registering CellComposition...")
    cell_composition_resource = push_cellcomposition(
        forge=get_forge(force_refresh=True),
        atlas_release_id=atlas_release_id,
        atlas_release_rev=atlas_release_rev,
        cell_composition_id=None,
        brain_region=atlas_release.brainLocation.brainRegion.id,
        hierarchy_path=hierarchy_path,
        reference_system_id=base_cell_composition.atlasSpatialReferenceSystem.id,
        volume_path=output_volume_path,
        species=base_cell_composition.subject.species.id,
        summary_path=summary_path,
        name=("CellComposition", "CellCompositionSummary", "CellCompositionVolume"),
        description="Cell Composition",
        resource_tag=None,
        logger=L,
        force_registration=True,
        dryrun=False,
    )

    return cell_composition_resource.id


def _get_summary_id(entry):
    """Handle the summary being a list or a single entry dict."""
    if isinstance(entry, list):
        return entry[0].id

    return entry.id


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
