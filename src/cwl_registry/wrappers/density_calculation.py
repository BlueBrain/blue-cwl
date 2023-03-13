"""Density calculation app."""
import json

import click
import libsonata
import voxcell
from voxcell.nexus.voxelbrain import Atlas

from cwl_registry import statistics, utils
from cwl_registry.nexus import get_forge


@click.group()
def app():
    """The CLI object."""


@app.command()
@click.argument("nodes_path")
@click.option("--output", help="output")
@click.option("--atlas-dir", help="Annotations atlas directory")
def from_nodes_file(nodes_path, output, atlas_dir):
    """Calculate summary statistics on [nodes]."""
    atlas = Atlas.open(str(atlas_dir))

    ns = libsonata.NodeStorage(nodes_path)
    population = ns.open_population(next(iter(ns.population_names)))

    forge = get_forge()
    mtype_urls, etype_urls = statistics.mtype_etype_url_mapping_from_nexus(forge)

    summary_statistics = statistics.node_population_composition_summary(
        population, atlas, mtype_urls, etype_urls
    )

    with open(output, "w", encoding="utf-8") as fd:
        json.dump(summary_statistics, fd)

    click.secho(f"Wrote {output}", fg="green")


@app.command()
@click.option("--output", help="output")
@click.option("--hierarchy", help="hierarchy")
@click.option("--annotations", help="Annotations atlas")
@click.option("--density-distribution", help="Materialized density distribution")
def from_atlas_density(output, hierarchy, annotations, density_distribution):
    """Calculate counts."""
    brain_regions = voxcell.VoxelData.load_nrrd(annotations)
    region_map = voxcell.RegionMap.load_json(hierarchy)

    density_distribution = utils.load_json(density_distribution)

    summary_statistics = statistics.atlas_densities_composition_summary(
        density_distribution, region_map, brain_regions
    )

    with open(output, "w", encoding="utf-8") as fd:
        json.dump(summary_statistics, fd)

    click.secho(f"Wrote {output}", fg="green")
