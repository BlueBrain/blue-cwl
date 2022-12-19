"""Composition summary statistics."""
import itertools
import logging

import numpy as np
import pandas as pd
import voxcell
from joblib import Parallel, delayed

L = logging.getLogger(__name__)


def mtype_etype_url_mapping(density_distribution, invert=False):
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

    if invert:
        mtype_urls = {v: k for k, v in mtype_urls.items()}
        etype_urls = {v: k for k, v in etype_urls.items()}

    return mtype_urls, etype_urls


def node_population_composition_summary(population, atlas, mtype_urls, etype_urls):
    """Calculate the composition summary statistics of a node population."""
    node_counts = _get_node_counts(population)

    region_map = atlas.load_region_map()
    brain_regions = atlas.load_data("brain_regions")

    volumes = _get_atlas_region_volumes(region_map, brain_regions)

    cell_counts = pd.merge(node_counts, volumes, left_index=True, right_index=True)
    cell_counts["density"] = cell_counts["count"] / cell_counts["volume"] * 1e9

    return _density_summary_stats_region(region_map, cell_counts, mtype_urls, etype_urls)


def _density_summary_stats_region(
    region_map, df_region_mtype_etype, mtype_urls, etype_urls
):  # pragma: no cover
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
    for mtype, df_etype in df_mtype_etype.groupby("mtype"):
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
    for etype, df in df_etype.groupby("etype"):
        url = etype_urls.get(etype)
        if url is None:
            L.info("Missing: %s", etype)
            continue

        neuron = {
            "density": float(df.density),
        }
        if "count" in df_etype:
            neuron["count"] = int(df["count"])

        ret[url] = {
            "label": etype,
            "about": "EType",
            "composition": {
                "neuron": neuron,
            },
        }
    return ret


def atlas_densities_composition_summary(density_distribution, region_map, brain_regions):
    """Calculate the composition summary statistics of a density distribution."""
    mtype_urls, etype_urls = mtype_etype_url_mapping(density_distribution)

    nrrd_stats = _get_nrrd_statistics(region_map, brain_regions, density_distribution)

    summary_statistics = _density_summary_stats_region(
        region_map, nrrd_stats, mtype_urls, etype_urls
    )
    return summary_statistics


def _get_nrrd_statistics(region_map, brain_regions, density_distribution):

    p = Parallel(
        n_jobs=-2,
        backend="multiprocessing",
    )

    worker_function = delayed(_get_statistics_from_nrrd_volume)
    work = []

    for mtype in density_distribution["mtypes"].values():
        for etype in mtype["etypes"].values():
            work.append(
                worker_function(
                    region_map=region_map,
                    brain_regions=brain_regions,
                    mtype=mtype["label"],
                    etype=etype["label"],
                    nrrd_path=etype["path"],
                ),
            )

    df = list(itertools.chain.from_iterable(p(work)))
    df = pd.DataFrame.from_records(df).set_index(["region", "mtype", "etype"])
    return df


def _get_statistics_from_nrrd_volume(region_map, brain_regions, mtype, etype, nrrd_path):

    v = voxcell.VoxelData.load_nrrd(nrrd_path)
    region_ids = np.unique(brain_regions.raw[np.nonzero(v.raw)])
    res = []

    for region_id in region_ids:
        if not region_id:
            continue

        region_densities = v.raw[brain_regions.raw == region_id]

        mean_density = np.mean(region_densities)
        counts = np.round(np.sum(region_densities) * brain_regions.voxel_volume * 1e-9)

        res.append(
            {
                "region": region_map.get(region_id, "acronym"),
                "mtype": mtype,
                "etype": etype,
                "density": mean_density,
                "count": counts,
            }
        )
    return res


def _density_summary_stats_region(region_map, df_region_mtype_etype, mtype_urls, etype_urls):
    ret = {
        "version": 1,
        "unitCode": {
            "density": "mm^-3",
        },
        "hasPart": {},
    }
    hasPart = ret["hasPart"]
    for acronym, df_mtype_etype in df_region_mtype_etype.groupby("region"):
        id_ = next(iter(region_map.find(acronym, "acronym")))
        hasPart[_region_url(id_)] = {
            "label": region_map.get(id_, "name"),
            "about": "BrainRegion",
            "hasPart": _density_summary_stats_mtype(
                mtype_urls,
                etype_urls,
                df_mtype_etype.droplevel(0),
            ),
        }

    return ret
