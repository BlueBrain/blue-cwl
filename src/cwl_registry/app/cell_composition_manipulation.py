"""Composition manipulation."""
import logging
from pathlib import Path

import click
import voxcell

# pylint: disable=no-name-in-module
from bba_data_push.bba_dataset_push import push_cellcomposition

from cwl_registry import density_manipulation, staging, statistics, utils
from cwl_registry.nexus import get_forge, get_resource, read_json_file_from_resource
from cwl_registry.variant import Variant

L = logging.getLogger(__name__)


@click.command()
@click.option("--region", required=True)
@click.option("--base-composition-summary", required=True)
@click.option("--base-density-distribution", required=True)
@click.option("--atlas-release", required=True)
@click.option("--recipe", help="Recipe for manipulations")
@click.option("--variant-config", required=True)
@click.option("--output-dir", required=True)
def app(  # pylint: disable=too-many-arguments
    region,  # pylint: disable=unused-argument
    base_composition_summary,
    base_density_distribution,
    atlas_release,
    recipe,
    variant_config,
    output_dir,
):
    """Density Manipulation CLI"""
    output_dir = utils.create_dir(Path(output_dir).resolve())

    forge = get_forge()
    density_distribution_file = output_dir / "density_distribution.json"
    L.info("Staging density distribution to  %s", density_distribution_file)
    staging.stage_me_type_densities(
        forge=forge,
        resource_id=base_density_distribution,
        output_file=density_distribution_file,
    )
    atlas_dir = utils.create_dir(output_dir / "atlas")
    L.info("Staging atlas to  %s", atlas_dir)
    hierarchy_path, annotations_path = staging.stage_atlas(
        forge=forge,
        resource_id=atlas_release,
        output_dir=atlas_dir,
        parcellation_ontology_basename="hierarchy.json",
        parcellation_volume_basename="brain_regions.nrrd",
    )

    manipulation_recipe = read_json_file_from_resource(get_resource(forge, recipe))

    L.info("Getting mtype, etype url mapping from nexus...")
    mtype_urls, etype_urls = statistics.mtype_etype_url_mapping_from_nexus(forge)

    # the materialized version
    materialized_cell_composition_volume = utils.load_json(density_distribution_file)

    # the original registered version
    original_density_release = read_json_file_from_resource(
        get_resource(forge, base_density_distribution)
    )

    region_map = voxcell.RegionMap.load_json(hierarchy_path)
    brain_regions = voxcell.VoxelData.load_nrrd(annotations_path)

    L.info("Manipulation densities...")
    updated_densities_dir = utils.create_dir(output_dir / "updated_densities_dir")
    updated_densities, updated_density_release = density_manipulation.density_manipulation(
        updated_densities_dir,
        brain_regions,
        manipulation_recipe,
        materialized_cell_composition_volume,
        original_density_release,
        mtype_urls,
        etype_urls,
    )

    original_cell_composition_summary = read_json_file_from_resource(
        get_resource(forge, base_composition_summary)
    )

    L.info("Updating cell composition summary statistics...")
    cell_composition_summary = density_manipulation.update_composition_summary_statistics(
        brain_regions,
        region_map,
        original_cell_composition_summary,
        updated_densities,
        mtype_urls,
        etype_urls,
    )

    updated_density_release_path = output_dir / "updated_density_release.json"
    utils.write_json(
        data=updated_density_release,
        filepath=updated_density_release_path,
    )

    updated_cell_composition_summary_path = output_dir / "updated_cell_composition_summary.json"
    utils.write_json(
        data=cell_composition_summary,
        filepath=updated_cell_composition_summary_path,
    )

    updated_density_release_path = output_dir / "updated_density_release.json"
    updated_cell_composition_summary_path = output_dir / "updated_cell_composition_summary.json"
    cell_composition_id = push_cellcomposition(
        forge,
        atlasrelease_id=atlas_release,
        volume_path=updated_density_release_path,
        summary_path=updated_cell_composition_summary_path,
        densities_dir=updated_densities_dir,
        name="Cell Composition",
        description="Cell Composition",
        resource_tag=None,
        output_dir=output_dir,
        L=L,
    )

    resource = forge.retrieve(cell_composition_id, cross_bucket=True)
    utils.write_resource_to_definition_output(
        forge=forge,
        resource=resource,
        variant=Variant.from_resource_id(forge, variant_config),
        output_dir=output_dir,
    )
