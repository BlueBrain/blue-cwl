"""Density calculation app."""

import multiprocessing

import click
import libsonata
import pandas as pd
import voxcell
from voxcell.nexus.voxelbrain import Atlas

from cwl_registry import statistics, utils


@click.group()
def app():
    """The CLI object."""


@app.command()
@click.argument("nodes_path")
@click.option(
    "--population-name",
    required=False,
    default=None,
    help="Population name. By default the first population is used.",
)
@click.option("--output", help="output")
@click.option("--atlas-dir", help="Annotations atlas directory")
def from_nodes_file(nodes_path, population_name, output, atlas_dir):
    """Calculate summary statistics on [nodes]."""
    atlas = Atlas.open(str(atlas_dir))

    ns = libsonata.NodeStorage(nodes_path)

    if not population_name:
        population_name = next(iter(ns.population_names))

    population = ns.open_population(population_name)

    mtype_urls, etype_urls = statistics.mtype_etype_url_mapping_from_nexus()

    summary_statistics = statistics.node_population_composition_summary(
        population, atlas, mtype_urls, etype_urls
    )

    utils.write_json(filepath=output, data=summary_statistics)

    click.secho(f"Wrote {output}", fg="green")


@app.command()
@click.option("--output-file", help="output")
@click.option("--hierarchy", help="hierarchy")
@click.option("--annotation", help="Annotations atlas")
@click.option("--density-distribution", help="Materialized density distribution")
@click.option(
    "--processes",
    help="Number of processes",
    default=multiprocessing.cpu_count() - 2,
    required=False,
)
def from_atlas_density(output_file, hierarchy, annotation, density_distribution, processes):
    """Calculate counts."""
    _from_atlas_density(output_file, hierarchy, annotation, density_distribution, processes)
    click.secho(f"Wrote {output_file}", fg="green")


def _from_atlas_density(output_file, hierarchy, annotation, density_distribution, processes):
    brain_regions = voxcell.VoxelData.load_nrrd(annotation)
    region_map = voxcell.RegionMap.load_json(hierarchy)

    density_distribution = pd.read_parquet(density_distribution)

    with multiprocessing.Pool(processes=processes) as pool:
        summary_statistics = statistics.atlas_densities_composition_summary(
            density_distribution,
            region_map,
            brain_regions,
            map_function=pool.imap,
        )

    utils.write_json(filepath=output_file, data=summary_statistics)
