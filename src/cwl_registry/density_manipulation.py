"""Density Manipulation of nrrd files"""
import copy
import itertools
import logging
import os

import joblib
import numpy as np
import pandas as pd
import voxcell

from cwl_registry import statistics

L = logging.getLogger(__name__)


def read_density_manipulation_recipe(recipe):
    """Read the density recipe dictionary, and transform it into a DataFrame"""
    assert recipe["version"] == 1

    df = []
    for region_url, mtype_etypes in recipe["overrides"].items():
        assert (
            "/Structure" in region_url
        ), f"ID should match something like api/v2/data/Structure/500, is {region_url}"
        region_id = int(region_url.split("/")[-1])
        for mtype_url, etypes in mtype_etypes["hasPart"].items():
            mtype_label = etypes["label"]
            for etype_url, etype in etypes["hasPart"].items():
                etype_label = etype["label"]

                if "density" in etype:
                    operation = "density"
                    value = etype["density"]
                elif "density_ratio" in etype:
                    operation = "density_ratio"
                    value = etype["density_ratio"]
                else:
                    raise KeyError(
                        "Neither `density` or `density_ratio` exist in "
                        f"{(region_id, mtype_label, etype_label)}"
                    )
                df.append(
                    (
                        region_id,
                        region_url,
                        mtype_label,
                        mtype_url,
                        etype_label,
                        etype_url,
                        operation,
                        value,
                    )
                )

    df = pd.DataFrame(
        df,
        columns=[
            "region_id",
            "region_url",
            "mtype",
            "mtype_url",
            "etype",
            "etype_url",
            "operation",
            "value",
        ],
    )
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
    p = joblib.Parallel(
        n_jobs=-2,
        backend="multiprocessing",
    )

    worker_function = joblib.delayed(_create_updated_density)

    materialized_densities = materialized_densities.set_index(["mtype", "etype"])

    work = []
    updated = {}
    for (mtype, etype), operations in all_operations.groupby(
        [
            "mtype",
            "etype",
        ]
    ):
        row = materialized_densities.loc[(mtype, etype)]

        path = str(row.path)
        new_path = os.path.join(output_dir, os.path.basename(path))

        updated[path] = mtype, etype, new_path, row.mtype_url, row.etype_url

        work.append(
            worker_function(
                input_nrrd_path=path,
                output_nrrd_path=new_path,
                operations=operations,
                brain_regions=brain_regions,
            )
        )

    L.debug("Densities to be updated: %d", len(updated))

    for path in p(work):
        mtype, etype, *_ = updated[path]
        L.info("Completed: %s, %s: [%s]", mtype, etype, path)

    updated_densities = pd.DataFrame(
        list(updated.values()), columns=["mtype", "etype", "path", "mtype_url", "etype_url"]
    )

    return updated_densities


def _create_updated_density(input_nrrd_path, output_nrrd_path, operations, brain_regions):
    nrrd = voxcell.VoxelData.load_nrrd(input_nrrd_path)
    for row in operations.itertuples():
        idx = np.nonzero(brain_regions.raw == row.region_id)

        if row.operation == "density":
            nrrd.raw[idx] = row.value
        elif row.operation == "density_ratio":
            nrrd.raw[idx] *= row.value

    nrrd.save_nrrd(output_nrrd_path)

    return input_nrrd_path


def _update_density_release(original_density_release, updated_densities):
    """For the updated densities, update the `original_density_release`

    * add `path` attribute, so it can be consumed by push_cellcomposition
    """
    density_release = copy.deepcopy(original_density_release)

    def find_node(dataset, id_):
        for haystack in dataset["hasPart"]:
            if haystack["@id"] == id_:
                return haystack
        return None

    for mtype_id, mtype_df in updated_densities.groupby("mtype_url"):
        mtype_node = find_node(density_release, mtype_id)
        assert mtype_node
        for etype_id, etype_df in mtype_df.groupby("etype_url"):
            assert etype_df.shape[0] == 1
            node = find_node(mtype_node, etype_id)
            assert len(node["hasPart"]) == 1
            node["hasPart"][0]["path"] = etype_df.iloc[0].path
            del node["hasPart"][0]["@id"]
            del node["hasPart"][0]["_rev"]

    return density_release


def density_manipulation(
    output_dir,
    brain_regions,
    manipulation_recipe,
    materialized_densities,
    original_density_release,
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
    updated_densities = _create_updated_densities(
        output_dir, brain_regions, manipulation_recipe, materialized_densities
    )
    updated_density_release = _update_density_release(
        original_density_release,
        updated_densities,
    )
    return updated_densities, updated_density_release


def _update_density_summary_statistics(
    original_cell_composition_summary,
    brain_regions,
    region_map,
    updated_densities,
):
    """Ibid"""
    p = joblib.Parallel(
        n_jobs=-2,
        backend="multiprocessing",
    )

    worker_function = joblib.delayed(statistics.get_statistics_from_nrrd_volume)

    work = []

    for row in updated_densities.itertuples():
        work.append(
            worker_function(
                region_map,
                brain_regions,
                row.mtype,
                row.etype,
                row.path,
            )
        )

    res = pd.DataFrame.from_records(list(itertools.chain.from_iterable(p(work))))
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
    updated_summary = _update_density_summary_statistics(
        original_cell_composition_summary,
        brain_regions,
        region_map,
        updated_densities,
    )

    cell_composition_summary = statistics.density_summary_stats_region(updated_summary)

    return cell_composition_summary
