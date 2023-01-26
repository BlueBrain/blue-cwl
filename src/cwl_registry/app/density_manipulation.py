"""Density manipulation."""

import click
import voxcell

from cwl_registry import density_manipulation, statistics, utils
from cwl_registry.nexus import get_forge


@click.command()
@click.option("--base-stats", required=True)
@click.option("--base-atlas-densities", required=True)
@click.option("--base-atlas-densities-materialized", required=True)
@click.option("--annotations", help="Annotations atlas")
@click.option("--hierarchy", help="hierarchy")
@click.option("--recipe", help="Recipe for manipulations")
@click.option("--output-dir", required=True)
def app(  # pylint: disable=too-many-arguments
    base_stats,
    base_atlas_densities,
    base_atlas_densities_materialized,
    annotations,
    hierarchy,
    recipe,
    output_dir,
):
    """Density Manipulation CLI"""
    region_map = voxcell.RegionMap.load_json(hierarchy)
    brain_regions = voxcell.VoxelData.load_nrrd(annotations)

    forge = get_forge()
    manipulation_recipe = utils.load_json(recipe)
    mtype_urls, etype_urls = statistics.mtype_etype_url_mapping_from_nexus(forge)

    materialized_cell_composition_volume = utils.load_json(base_atlas_densities_materialized)
    original_density_release = utils.load_json(base_atlas_densities)

    updated_densities, updated_density_release = density_manipulation.density_manipulation(
        output_dir,
        region_map,
        brain_regions,
        manipulation_recipe,
        materialized_cell_composition_volume,
        original_density_release,
        mtype_urls,
        etype_urls,
    )

    original_cell_composition_summary = utils.load_json(base_stats)
    cell_composition_summary = density_manipulation.update_composition_summary_statistics(
        brain_regions,
        region_map,
        original_cell_composition_summary,
        updated_densities,
        mtype_urls,
        etype_urls,
    )

    # XXX: once we have DKE post cell composition stuff
    updated_density_release, cell_composition_summary = (
        cell_composition_summary,
        updated_density_release,
    )
    # create_cellCompositionVolume(
    # updated_density_release
    #    forge,
    #    volume_path,
    #    resource_tag,
    #    verbose,
    #    )
