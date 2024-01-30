"""Composition summary statistics."""
import logging
from functools import partial

import numpy as np
import pandas as pd
import voxcell
from entity_management.nexus import sparql_query

L = logging.getLogger(__name__)


def mtype_etype_url_mapping_from_nexus(base: str = None, token: str = None) -> tuple[dict, dict]:
    """Get mtype and etype labels mapped to their uris, from nexus"""

    def get_type_ids(type_):
        known_types = (
            "MType",
            "EType",
        )
        assert type_ in known_types, f"type_ must be {known_types}"

        query = f"""
        PREFIX nsg: <https://neuroshapes.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?type_id ?type_ ?revision
        WHERE {{
            ?type_id rdfs:label ?type_ ;
            rdfs:subClassOf* nsg:{type_} ;
            <https://bluebrain.github.io/nexus/vocabulary/rev> ?revision ;
            <https://bluebrain.github.io/nexus/vocabulary/deprecated> false .
        }}
        LIMIT 10000
        """
        res = sparql_query(
            query, base=base, org="neurosciencegraph", proj="datamodels", token=token
        )
        res = {r["type_"]["value"]: r["type_id"]["value"] for r in res["results"]["bindings"]}
        return res

    mtype_urls = get_type_ids("MType")
    etype_urls = get_type_ids("EType")

    return mtype_urls, etype_urls


def mtype_etype_url_mapping(density_distribution: pd.DataFrame) -> tuple[dict, dict]:
    """Get mtype and etype labels mapped to their uris."""
    mtype_urls = {}
    etype_urls = {}

    df = density_distribution[["mtype", "mtype_url", "etype", "etype_url"]].drop_duplicates()

    for row in df.itertuples(index=False):
        mtype_urls[row.mtype] = row.mtype_url
        etype_urls[row.etype] = row.etype_url

    return mtype_urls, etype_urls


def node_population_composition_summary(population, atlas, mtype_urls, etype_urls):
    """Calculate the composition summary statistics of a node population."""
    node_counts = _get_node_counts(population)

    region_map = atlas.load_region_map()
    brain_regions = atlas.load_data("brain_regions")

    volumes = _get_atlas_region_volumes(region_map, brain_regions)

    cell_counts = pd.merge(node_counts, volumes, left_index=True, right_index=True)
    cell_counts["density"] = cell_counts["count"] / cell_counts["volume"] * 1e9

    # expand dataframe with additional information needed to construct the summary
    cell_counts = _expand_me_dataframe(cell_counts, region_map, mtype_urls, etype_urls)

    return density_summary_stats_region(cell_counts)


def _expand_me_dataframe(df, region_map, mtype_urls, etype_urls):
    df = df.copy()

    region_ids = [
        next(iter(region_map.find(r, "acronym"))) for r in df.index.get_level_values("region")
    ]
    df["region_url"] = pd.Categorical([_region_url(rid) for rid in region_ids])
    df["region_label"] = pd.Categorical([region_map.get(rid, "name") for rid in region_ids])

    df["mtype_url"] = df.index.get_level_values("mtype").map(mtype_urls)
    df["etype_url"] = df.index.get_level_values("etype").map(etype_urls)

    return df


def density_summary_stats_region(df_region_mtype_etype):
    """Serialize `df_region_mtype_etype` to required summary statistics format

    Args:
        region_map: voxcell.region map to use
        df_region_mtype_etype(DataFrame): specifying the region/mtype/etype of
        the statistics
        mtype_urls(dict): mapping for mtype labels to mtype urls
        etype_urls(dict): mapping for etype labels to etype urls
    """
    ret = {
        "version": 1,
        "unitCode": {
            "density": "mm^-3",
        },
        "hasPart": {},
    }
    hasPart = ret["hasPart"]

    groups = df_region_mtype_etype.groupby(["region", "region_url", "region_label"])

    for (region_notation, region_url, region_label), df_mtype_etype in groups:
        hasPart[region_url] = {
            "label": region_label,
            "notation": region_notation,
            "about": "BrainRegion",
            "hasPart": _density_summary_stats_mtype(
                df_mtype_etype.droplevel(0),
            ),
        }

    return ret


def _region_url(region_id):
    return f"http://api.brain-map.org/api/v2/data/Structure/{region_id}"


def _get_node_counts(population):
    df = pd.DataFrame()
    for attr in (
        "region",
        "mtype",
        "etype",
    ):
        df[attr] = pd.Categorical.from_codes(
            codes=population.get_enumeration(attr, population.select_all()),
            categories=population.enumeration_values(attr),
        )
    count = df.value_counts(["region", "mtype", "etype"])
    count.name = "count"

    return count


def _get_atlas_region_volumes(region_map, brain_regions):
    ids, counts = np.unique(brain_regions.raw, return_counts=True)

    volumes = {}
    for id_, count in zip(ids, counts):
        if not id_:
            continue
        acronym = region_map.get(id_, "acronym")
        volumes[acronym] = count * brain_regions.voxel_volume

    volumes = pd.DataFrame.from_dict(
        volumes,
        orient="index",
        columns=[
            "volume",
        ],
    )
    volumes.index.name = "region"

    return volumes


def _density_summary_stats_mtype(df_mtype_etype):
    mtype_groups = df_mtype_etype.groupby(["mtype", "mtype_url"], observed=True)
    return {
        mtype_url: {
            "label": mtype,
            "about": "MType",
            "hasPart": _density_summary_stats_etype(df_etype.droplevel(0)),
        }
        for (mtype, mtype_url), df_etype in mtype_groups
    }


def _density_summary_stats_etype(df_etype):
    ret = {}
    for (etype, etype_url), df in df_etype.groupby(["etype", "etype_url"], observed=True):
        assert len(df) == 1, df
        neuron = {
            "density": float(df["density"].iloc[0]),
        }

        if "count" in df_etype and not np.isnan(df["count"].iloc[0]):
            neuron["count"] = int(df["count"].iloc[0])

        ret[etype_url] = {
            "label": etype,
            "about": "EType",
            "composition": {
                "neuron": neuron,
            },
        }
    return ret


def atlas_densities_composition_summary(
    density_distribution, region_map, brain_regions, map_function=map
):
    """Calculate the composition summary statistics of a density distribution."""
    L.info("Extracting statistics from density distribution.")
    nrrd_stats = _get_nrrd_statistics(
        region_map=region_map,
        brain_regions=brain_regions,
        densities=density_distribution,
        map_function=map_function,
    )

    # expand dataframe with additional information needed to construct the summary
    mtype_urls, etype_urls = mtype_etype_url_mapping(density_distribution)
    nrrd_stats = _expand_me_dataframe(nrrd_stats, region_map, mtype_urls, etype_urls)

    L.info("Converting statistics from dataframe to json format.")
    summary_statistics = density_summary_stats_region(nrrd_stats)
    return summary_statistics


def _get_nrrd_statistics(region_map, brain_regions, densities, map_function):
    result = map_function(
        partial(_worker_function, region_map=region_map, brain_regions=brain_regions),
        densities.iterrows(),
    )

    n_volumes = len(densities)
    L.info("Statistics from %d nrrd volumes will be calculated.", n_volumes)

    df = []
    for i, (index, chunk) in enumerate(result):
        df.extend(chunk)
        L.info(
            "Completed [%d|%d] : %s",
            i + 1,
            n_volumes,
            densities.iloc[index][["mtype", "etype", "path"]].values.tolist(),
        )

    df = pd.DataFrame.from_records(df).set_index(["region", "mtype", "etype"])
    return df


def _worker_function(data, region_map, brain_regions):
    index, entry = data
    return index, get_statistics_from_nrrd_volume(
        region_map, brain_regions, entry.mtype, entry.etype, entry.path
    )


def get_statistics_from_nrrd_volume(region_map, brain_regions, mtype, etype, nrrd_path):
    """Get statistics about an nrrd volume

    Args:
        region_map: voxcell.region map to use
        brain_regions: annotation atlas
        mtype(str): label to apply to values
        etype(str): label to apply to values
        nrrd_path: path to nrrd file
    """
    unique_region_ids, total_densities, voxel_counts = _calculate_statistics(
        brain_regions, nrrd_path
    )

    cell_counts = np.round(total_densities * brain_regions.voxel_volume * 1e-9)

    return [
        {
            "region": region_map.get(region_id, "acronym"),
            "mtype": mtype,
            "etype": etype,
            "density": total_densities[i] / voxel_counts[i],
            "count": int(cell_counts[i]),
        }
        for i, region_id in enumerate(unique_region_ids)
    ]


def _extract_densities(brain_regions, nrrd_path):
    v = voxcell.VoxelData.load_nrrd(nrrd_path)
    mask = brain_regions.raw != 0
    return brain_regions.raw[mask], v.raw[mask]


def _calculate_statistics(brain_regions, nrrd_path):
    region_ids, densities = _extract_densities(brain_regions, nrrd_path)

    # faster than np.unique
    codes, uniques = pd.factorize(region_ids)

    sums = np.zeros(uniques.size, dtype=float)
    counts = np.zeros(uniques.size, dtype=int)

    # add.at allows accumulating when the same index is encountered
    np.add.at(sums, codes, densities)
    np.add.at(counts, codes, 1)

    # ignore regions that are completely empty
    nonzero = sums != 0.0
    return uniques[nonzero], sums[nonzero], counts[nonzero]
