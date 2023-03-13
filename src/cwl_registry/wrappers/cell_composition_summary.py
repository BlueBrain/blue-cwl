"""Cell composition summary app."""
from pathlib import Path

import click
from voxcell.nexus.voxelbrain import LocalAtlas

from cwl_registry import registering, staging, statistics, utils
from cwl_registry.nexus import get_forge


@click.group()
def app():
    """The CLI object."""


@app.command()
@click.option("--atlas-release", help="Atlas release KG resource id.", required=True)
@click.option("--density-distribution", help="Density distribution KG dataset id.", required=True)
@click.option("--output-dir", required=True)
def from_density_distribution(
    atlas_release,
    density_distribution,
    output_dir,
):
    """Calculate summary statistics from density distribution."""
    forge = get_forge()
    output_dir = utils.create_dir(output_dir)

    atlas_dir = utils.create_dir(output_dir / "atlas")
    staging.stage_atlas(
        forge=forge,
        resource_id=atlas_release,
        output_dir=atlas_dir,
        parcellation_ontology_basename="hierarchy.json",
        parcellation_volume_basename="brain_regions.nrrd",
    )
    atlas = LocalAtlas.open(str(atlas_dir))

    density_distribution_file = Path(output_dir / "density_distribution.json")
    staging.stage_me_type_densities(
        forge=forge,
        resource_id=density_distribution,
        output_file=density_distribution_file,
    )
    densities = utils.load_json(density_distribution_file)

    summary = statistics.atlas_densities_composition_summary(
        density_distribution=densities,
        region_map=atlas.load_region_map(),
        brain_regions=atlas.load_data("brain_regions"),
    )
    composition_summary_file = output_dir / "cell_composition_summary.json"
    utils.write_json(filepath=composition_summary_file, data=summary)

    # pylint: disable=no-member
    registering.register_cell_composition_summary(
        forge,
        name="Cell composition summary",
        summary_file=composition_summary_file,
        atlas_release_id=atlas_release,
        derivation_entity_id=density_distribution,
    )
