"""Density manipulation."""
import click


@click.command()
@click.option("--region", required=True)
@click.option("--variant-config", required=False)
@click.option("--me-type-densities", required=True)
@click.option("--atlas", required=True)
@click.option("--nexus-base", required=True)
@click.option("--nexus-project", required=True)
@click.option("--nexus-org", required=True)
@click.option("--nexus-token", required=True)
@click.option("--task-digest", required=True)
@click.option("--output-dir", required=True)
def app(
    # pylint: disable=unused-argument
    region,
    variant_config,
    me_type_densities,
    atlas,
    nexus_base,
    nexus_project,
    nexus_org,
    nexus_token,
    task_digest,
    output_dir,
):
    """Morphoelectrical type generator cli entry."""
