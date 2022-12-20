"""Density calculation app."""
import json

import click
import libsonata
import voxcell
from voxcell.nexus.voxelbrain import Atlas

from cwl_registry import statistics, utils
from cwl_registry.nexus import get_forge


def _get_mtype_etype(forge, include_rev=True):
    def get_type_ids(type_):
        known_types = (
            "MType",
            "EType",
        )
        assert type_ in known_types, f"type_ must be {known_types}"

        query = f"""
        SELECT ?type_id ?type_ ?revision
        WHERE {{
            ?type_id label ?type_;
            subClassOf* {type_} ;
            _rev ?revision ;
            _deprecated false .
        }}
        """
        res = forge.sparql(query, limit=10000)
        if include_rev:
            res = {a.type_: f"{a.type_id}?rev={a.revision}" for a in res}
        else:
            res = {a.type_: a.type_id for a in res}
        return res

    mtype_urls = get_type_ids("MType")
    etype_urls = get_type_ids("EType")

    return mtype_urls, etype_urls


@click.group()
def app():
    """The CLI object."""


@app.command()
@click.argument("nodes_path")
@click.option("--nexus-base", envvar="NEXUS_BASE", required=True)
@click.option("--nexus-project", envvar="NEXUS_PROJ", required=True)
@click.option("--nexus-org", envvar="NEXUS_ORG", required=True)
@click.option("--nexus-token", envvar="NEXUS_TOKEN", required=True)
@click.option("--output", help="output")
@click.option("--atlas-dir", help="Annotations atlas directory")
def from_nodes_file(
    nexus_base, nexus_project, nexus_org, nexus_token, output, nodes_path, atlas_dir
):
    """Calculate summary statistics on [nodes]."""
    atlas = Atlas.open(str(atlas_dir))

    ns = libsonata.NodeStorage(nodes_path)
    population = ns.open_population(next(iter(ns.population_names)))

    forge = get_forge(
        nexus_base,
        nexus_org,
        nexus_project,
        nexus_token,
    )
    mtype_urls, etype_urls = _get_mtype_etype(forge, include_rev=True)

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
