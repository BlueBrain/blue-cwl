"""Composition summary statistics."""
import logging
from functools import partial

import numpy as np
import pandas as pd
import voxcell

from cwl_registry.utils import url_without_revision

L = logging.getLogger(__name__)


def cell_composition_summary_to_df(
    cell_composition_summary, region_map, mtype_urls_inverse, etype_urls_inverse
):
    """Load a cell composition summary json"""
    assert cell_composition_summary["version"] == 1

    ret = []

    for region_url, mtype_payload in cell_composition_summary["hasPart"].items():
        region_acronym = region_map.get(int(region_url.split("/")[-1]), "acronym")
        for mtype_url, etype_payload in mtype_payload["hasPart"].items():
            mtype_label = mtype_urls_inverse[url_without_revision(mtype_url)]
            for etype_url, payload in etype_payload["hasPart"].items():
                etype_label = etype_urls_inverse[url_without_revision(etype_url)]
                composition = payload["composition"]["neuron"]
                ret.append(
                    (
                        region_acronym,
                        mtype_label,
                        etype_label,
                        composition["density"],
                    )
                )

    ret = pd.DataFrame(ret, columns=["region", "mtype", "etype", "density"])

    return ret


def mtype_etype_url_mapping_from_nexus(forge):
    """Get mtype and etype labels mapped to their uris, from nexus"""

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
        res = {a.type_: a.type_id for a in res}
        return res

    mtype_urls = get_type_ids("MType")
    etype_urls = get_type_ids("EType")

    return mtype_urls, etype_urls


def mtype_etype_url_mapping(density_distribution):
    """Get mtype and etype labels mapped to their uris."""
    mtype_urls = {}
    etype_urls = {}

    for mtype_url, mtype_data in density_distribution["mtypes"].items():
        mtype_label = mtype_data["label"]
        assert mtype_label not in mtype_urls
        mtype_urls[mtype_label] = mtype_url

        for etype_url, etype_data in mtype_data["etypes"].items():
            if etype_url not in etype_urls:
                etype_label = etype_data["label"]
                etype_urls[etype_label] = etype_url

    return mtype_urls, etype_urls


def node_population_composition_summary(population, atlas, mtype_urls, etype_urls):
    """Calculate the composition summary statistics of a node population."""
    node_counts = _get_node_counts(population)

    region_map = atlas.load_region_map()
    brain_regions = atlas.load_data("brain_regions")

    volumes = _get_atlas_region_volumes(region_map, brain_regions)

    cell_counts = pd.merge(node_counts, volumes, left_index=True, right_index=True)
    cell_counts["density"] = cell_counts["count"] / cell_counts["volume"] * 1e9

    return density_summary_stats_region(region_map, cell_counts, mtype_urls, etype_urls)


def density_summary_stats_region(region_map, df_region_mtype_etype, mtype_urls, etype_urls):
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
    for acronym, df_mtype_etype in df_region_mtype_etype.groupby("region"):
        region_id = next(iter(region_map.find(acronym, "acronym")))
        hasPart[_region_url(region_id)] = {
            "label": region_map.get(region_id, "name"),
            "notation": acronym,
            "about": "BrainRegion",
            "hasPart": _density_summary_stats_mtype(
                mtype_urls,
                etype_urls,
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


def _density_summary_stats_mtype(mtype_urls, etype_urls, df_mtype_etype):
    ret = {}
    for mtype, df_etype in df_mtype_etype.groupby("mtype", observed=True):
        url = mtype_urls.get(mtype)
        if url is None:
            L.info("Missing: %s", mtype)
            continue

        ret[url] = {
            "label": mtype,
            "about": "MType",
            "hasPart": _density_summary_stats_etype(etype_urls, df_etype.droplevel(0)),
        }
    return ret


def _density_summary_stats_etype(etype_urls, df_etype):
    ret = {}
    for etype, df in df_etype.groupby("etype", observed=True):
        assert len(df) == 1, df
        url = etype_urls.get(etype)
        if url is None:
            L.info("Missing: %s", etype)
            continue

        neuron = {
            "density": float(df["density"].iloc[0]),
        }

        if "count" in df_etype and not np.isnan(df["count"].iloc[0]):
            neuron["count"] = int(df["count"].iloc[0])

        ret[url] = {
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
    mtype_urls, etype_urls = mtype_etype_url_mapping(density_distribution)

    L.info("Extracting statistics from density distribution.")
    nrrd_stats = _get_nrrd_statistics(
        region_map=region_map,
        brain_regions=brain_regions,
        density_distribution=density_distribution,
        map_function=map_function,
    )

    L.info("Converting statistics from dataframe to json format.")
    summary_statistics = density_summary_stats_region(
        region_map, nrrd_stats, mtype_urls, etype_urls
    )
    return summary_statistics


def _get_nrrd_statistics(region_map, brain_regions, density_distribution, map_function):
    def _triplets():
        index = 0
        res = []
        for mtype in density_distribution["mtypes"].values():
            for etype in mtype["etypes"].values():
                res.append((index, (mtype["label"], etype["label"], etype["path"])))
                index += 1
        return res

    return get_statistics_from_triplets(
        triplets=_triplets(),
        region_map=region_map,
        brain_regions=brain_regions,
        map_function=map_function,
    )


def get_statistics_from_triplets(triplets, region_map, brain_regions, map_function=map):
    """Get statistics from (mtype, etype, nrrd_path) triplets."""
    result = map_function(
        partial(_worker_function, region_map=region_map, brain_regions=brain_regions),
        triplets,
    )

    n_triplets = len(triplets)
    L.info("Statistics from %d nrrd volumes will be calculated.", n_triplets)

    df = []
    for i, (index, chunk) in enumerate(result):
        df.extend(chunk)
        L.info("Completed [%d|%d] : %s", i + 1, n_triplets, triplets[index][1])

    df = pd.DataFrame.from_records(df).set_index(["region", "mtype", "etype"])
    return df


def _worker_function(data, region_map, brain_regions):
    index, (mtype, etype, nrrd_path) = data
    return index, get_statistics_from_nrrd_volume(
        region_map, brain_regions, mtype, etype, nrrd_path
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
