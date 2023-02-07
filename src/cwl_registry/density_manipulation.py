"""Density Manipulation of nrrd files"""
import copy
import logging
import os
from urllib.parse import parse_qs, urlsplit, urlunparse

import numpy as np
import pandas as pd
import voxcell

from cwl_registry import statistics

L = logging.getLogger(__name__)


def _read_density_manipulation_recipe(recipe):
    """Read the density recipe dictionary, and transform it into a DataFrame"""
    assert recipe["version"] == 1

    df = []
    for id_, mtype_etypes in recipe["overrides"].items():
        assert (
            "/Structure" in id_
        ), f"ID should match something like api/v2/data/Structure/500, is {id_}"
        region_id = int(id_.split("/")[-1])
        for etypes in mtype_etypes["hasPart"].values():
            mtype_label = etypes["label"]
            for etype in etypes["hasPart"].values():
                etype_label = etype["label"]
                if "density" in etype:
                    df.append((region_id, mtype_label, etype_label, "density", etype["density"]))
                elif "density_ratio" in etype:
                    df.append(
                        (
                            region_id,
                            mtype_label,
                            etype_label,
                            "density_ratio",
                            etype["density_ratio"],
                        )
                    )
                else:
                    raise KeyError(
                        "Neither `density` or `density_ratio` exist in "
                        f"{(region_id, mtype_label, etype_label)}"
                    )

    df = pd.DataFrame(df, columns=["region", "mtype", "etype", "operation", "value"])

    return df


def _cell_composition_volume_to_df(cell_composition_volume, mtype_urls_inverse, etype_urls_inverse):
    """Read a CellCompositionVolume to Dataframe"""
    df = []
    for mtype_id, etypes in cell_composition_volume["mtypes"].items():
        mtype_label = mtype_urls_inverse[mtype_id]
        for etype_id, etype in etypes["etypes"].items():
            etype_label = etype_urls_inverse[etype_id]
            path = etype["path"]
            df.append((mtype_label, etype_label, path))

    return pd.DataFrame(
        df,
        columns=[
            "mtype",
            "etype",
            "path",
        ],
    ).set_index(["mtype", "etype"])


def _create_updated_densities(output_dir, brain_regions, all_operations, materialized_densities):
    """Apply the operations to the NRRD files"""
    updated = []
    for (mtype, etype), operations in all_operations.groupby(
        [
            "mtype",
            "etype",
        ]
    ):
        path = str(materialized_densities.loc[(mtype, etype)].path)
        L.info("doing: %s, %s: [%s]", mtype, etype, path)
        nrrd = voxcell.VoxelData.load_nrrd(path)
        for row in operations.itertuples():
            idx = np.nonzero(brain_regions.raw == row.region)

            if row.operation == "density":
                nrrd.raw[idx] = row.value
            elif row.operation == "density_ratio":
                current_density = np.mean(nrrd.raw[idx])
                if current_density > 0:
                    nrrd.raw[idx] *= row.value / current_density

        new_path = os.path.join(output_dir, os.path.basename(path))
        updated.append((mtype, etype, new_path))
        nrrd.save_nrrd(new_path)

    return pd.DataFrame(updated, columns=["mtype", "etype", "path"])


def _update_density_release(original_density_release, updated_densities, mtype_urls, etype_urls):
    """For the updated densities, update the `original_density_release`

    * add `path` attribute, so it can be consumed by push_cellcomposition
    """
    density_release = copy.deepcopy(original_density_release)

    def split_rev(id_):
        split = urlsplit(id_)
        rev = parse_qs(split.query).get("rev")
        if rev:
            rev = int(rev[0])
        id_ = urlunparse((split.scheme, split.netloc, split.path, "", "", ""))
        return id_, rev

    def find_node(dataset, id_, rev):
        for haystack in dataset["hasPart"]:
            if haystack["@id"] == id_:
                if "_rev" in haystack and rev is not None and haystack["_rev"] != rev:
                    continue
                return haystack
        return None

    for mtype_label, mtype_df in updated_densities.groupby("mtype"):
        mtype_id, rev = split_rev(mtype_urls[mtype_label])
        mtype_node = find_node(density_release, mtype_id, rev)
        assert mtype_node
        for etype_label, etype_df in mtype_df.groupby("etype"):
            assert etype_df.shape[0] == 1
            etype_id, rev = split_rev(etype_urls[etype_label])
            node = find_node(mtype_node, etype_id, rev)
            assert len(node["hasPart"]) == 1
            node["hasPart"][0]["path"] = etype_df.iloc[0].path
            del node["hasPart"][0]["@id"]
            del node["hasPart"][0]["_rev"]

    return density_release


def density_manipulation(
    output_dir,
    brain_regions,
    manipulation_recipe,
    materialized_cell_composition_volume,
    original_density_release,
    mtype_urls,
    etype_urls,
):
    """Manipulate the densities in a CellCompositionVolume

    Args:
        output_dir(str): where to output the updated densities
        brain_regions: annotation atlas
        manipulation_recipe(dict): recipe containing the manipulations to perform
        materialized_cell_composition_volume(dict): a cell composition, where
        the ids have been materialized to concrete paths
        original_density_release(dict): what
        materialized_cell_composition_volume was generated from
        mtype_urls(dict): mapping for mtype labels to mtype urls
        etype_urls(dict): mapping for etype labels to etype urls
    """
    mtype_urls_inverse = {v: k for k, v in mtype_urls.items()}
    etype_urls_inverse = {v: k for k, v in etype_urls.items()}

    manipulation_operations = _read_density_manipulation_recipe(manipulation_recipe)
    materialized_densities = _cell_composition_volume_to_df(
        materialized_cell_composition_volume, mtype_urls_inverse, etype_urls_inverse
    )

    updated_densities = _create_updated_densities(
        output_dir, brain_regions, manipulation_operations, materialized_densities
    )

    updated_density_release = _update_density_release(
        original_density_release, updated_densities, mtype_urls, etype_urls
    )

    return updated_densities, updated_density_release


def _update_density_summary_statistics(
    original_cell_composition_summary,
    brain_regions,
    region_map,
    updated_densities,
):
    """Ibid"""
    res = []
    for _, mtype_label, etype_label, nrrd_path in updated_densities.itertuples():
        res.extend(
            statistics.get_statistics_from_nrrd_volume(
                region_map, brain_regions, mtype_label, etype_label, nrrd_path
            )
        )

    res = pd.DataFrame.from_records(res)
    res = (
        res.set_index(["region", "mtype", "etype"])
        .combine_first(original_cell_composition_summary.set_index(["region", "mtype", "etype"]))
        .sort_index()
    )

    return res


def update_composition_summary_statistics(
    brain_regions,
    region_map,
    original_cell_composition_summary,
    updated_densities,
    mtype_urls,
    etype_urls,
):
    """Update the summary statistics, after manipulations

    Args:
        region_map: voxcell.region map to use
        brain_regions: annotation atlas
        original_cell_composition_summary(dict): original composition statistics
        updated_densities(DataFrame): which metypes and paths that were changed
        mtype_urls(dict): mapping for mtype labels to mtype urls
        etype_urls(dict): mapping for etype labels to etype urls
    """
    mtype_urls_inverse = {v: k for k, v in mtype_urls.items()}
    etype_urls_inverse = {v: k for k, v in etype_urls.items()}

    original_cell_composition_summary = statistics.cell_composition_summary_to_df(
        original_cell_composition_summary, region_map, mtype_urls_inverse, etype_urls_inverse
    )

    updated_summary = _update_density_summary_statistics(
        original_cell_composition_summary,
        brain_regions,
        region_map,
        updated_densities,
    )

    cell_composition_summary = statistics.density_summary_stats_region(
        region_map, updated_summary, mtype_urls, etype_urls
    )

    return cell_composition_summary
