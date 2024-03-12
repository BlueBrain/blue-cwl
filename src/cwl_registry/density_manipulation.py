"""Density Manipulation of nrrd files"""

import copy
import logging
import os

import joblib
import numpy as np
import pandas as pd
import voxcell

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
    )


class _FlatGroupsMapper:
    """Class for efficient access to indices of unique values of the values array."""

    def __init__(self, values):
        ids = np.argsort(values, kind="stable")

        uniques, counts = np.unique(values, return_counts=True)

        offsets = np.empty(len(counts) + 1, dtype=np.int64)
        offsets[0] = 0
        offsets[1:] = np.cumsum(counts)

        mapping = {v: i for i, v in enumerate(uniques)}

        self._ids = ids
        self._offsets = offsets
        self._mapping = mapping

    def get_group_indices_by_value(self, value):
        """Return the values array indices corresponding to the 'value'."""
        group_index = self._mapping[value]
        return self._ids[self._offsets[group_index] : self._offsets[group_index + 1]]


def _create_updated_densities(
    output_dir, brain_regions, all_operations, materialized_densities, region_selection=None
):
    """Apply the operations to the NRRD files"""
    p = joblib.Parallel(
        n_jobs=-2,
        backend="multiprocessing",
    )
    worker_function = joblib.delayed(_create_updated_density)

    if region_selection is None:
        to_zero_mask = None
    else:
        to_zero_mask = ~np.isin(brain_regions.raw, region_selection)
        all_operations = all_operations[all_operations["region_id"].isin(region_selection)]

    # allow to efficiently access all the voxel indices corresponding to a region id
    region_groups = _FlatGroupsMapper(brain_regions.raw.ravel())

    grouped_operations = all_operations.groupby(["mtype", "etype"])

    work = []
    processed = {}

    for row_dict in materialized_densities.to_dict(orient="records"):
        path = row_dict["path"]
        mtype = row_dict["mtype"]
        etype = row_dict["etype"]

        row_dict["remove"] = False
        row_dict["updated"] = True

        # get the group of mtype/etype operations
        if (mtype, etype) in grouped_operations.groups:
            operations = grouped_operations.get_group((mtype, etype))
        else:
            if to_zero_mask is None:
                # nothing will be changed, do not update and keep old path
                row_dict["updated"] = False
                processed[path] = row_dict
                continue
            operations = pd.DataFrame()

        new_path = os.path.join(output_dir, os.path.basename(path))
        row_dict["path"] = new_path
        processed[path] = row_dict

        work.append(
            worker_function(
                input_nrrd_path=path,
                output_nrrd_path=new_path,
                operations=operations,
                region_groups=region_groups,
                to_zero_mask=to_zero_mask,
            )
        )

    L.debug("Densities to be processed: %d", len(processed))

    for path, is_empty in p(work):
        info = processed[path]
        info["remove"] = is_empty
        L.info("Processed: %s, %s: [%s]", info["mtype"], info["etype"], info["path"])

    res = pd.DataFrame.from_records(data=list(processed.values()))
    res = res[~res["remove"]].drop(columns="remove").reset_index(drop=True)

    L.debug("Densities after processing: %d", res.shape[0])

    return res


def _create_updated_density(
    input_nrrd_path, output_nrrd_path, operations, region_groups, to_zero_mask
):
    nrrd = voxcell.VoxelData.load_nrrd(input_nrrd_path)

    densities = nrrd.raw.ravel()

    if not operations.empty:
        _apply_operations_on_densities(operations, densities, region_groups)

    if to_zero_mask is not None:
        densities[to_zero_mask.ravel()] = 0.0

    if np.allclose(nrrd.raw, 0.0):
        return (input_nrrd_path, True)

    # nrrd.raw arrays are usually not c-contiguous, therefore a copy has been made when ravelled
    nrrd.raw = densities.reshape(nrrd.raw.shape)

    nrrd.save_nrrd(output_nrrd_path)

    return (input_nrrd_path, False)


def _apply_operations_on_densities(operations, densities, region_groups):
    operations = operations.drop_duplicates(keep="last", subset=["region_id"])

    for operation, df in operations.groupby("operation"):
        if operation == "density":
            for row in df.itertuples():
                voxel_ids = region_groups.get_group_indices_by_value(row.region_id)
                densities[voxel_ids] = row.value
        elif operation == "density_ratio":
            for row in df.itertuples():
                voxel_ids = region_groups.get_group_indices_by_value(row.region_id)
                densities[voxel_ids] *= row.value
        else:
            raise ValueError(f"Unsuppored operation {operation}")


def _copy_level_info(dataset, with_children=None):
    data = {k: v for k, v in dataset.items() if k != "hasPart"}
    if with_children:
        data["hasPart"] = with_children
    return data


def _update_density_release(original_density_release, updated_densities):
    """For the updated densities, update the `original_density_release`

    * add `path` attribute, so it can be consumed by push_cellcomposition
    """

    def find_node(dataset, id_):
        for haystack in dataset["hasPart"]:
            if haystack["@id"] == id_:
                return haystack
        raise KeyError(f"ID {id_} was not found in dataset.")

    mtypes = []
    for mtype_url, mtype_df in updated_densities.groupby("mtype_url"):
        mtype_node = find_node(original_density_release, mtype_url)

        etypes = []
        for etype_url, etype_df in mtype_df.groupby("etype_url"):
            assert etype_df.shape[0] == 1

            nrrd_info = etype_df.iloc[0]

            etype_node = find_node(mtype_node, etype_url)
            assert len(etype_node["hasPart"]) == 1

            if nrrd_info.updated:
                nrrd_entry = {"path": nrrd_info.path}

                # ignore id related entries if a local path is added
                for k, v in _copy_level_info(etype_node["hasPart"][0]).items():
                    if k not in {"@id", "_rev", "path"}:
                        nrrd_entry[k] = v

                new_etype_node = _copy_level_info(
                    dataset=etype_node,
                    with_children=[nrrd_entry],
                )

            # if no changes, use the resource from the initial release
            else:
                new_etype_node = copy.deepcopy(etype_node)

            etypes.append(new_etype_node)

        if etypes:
            new_mtype_node = _copy_level_info(dataset=mtype_node, with_children=etypes)
            mtypes.append(new_mtype_node)

    density_release = _copy_level_info(original_density_release, with_children=mtypes)
    return density_release


def density_manipulation(
    output_dir,
    brain_regions,
    manipulation_recipe,
    materialized_densities,
    original_density_release,
    region_selection=None,
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
        output_dir,
        brain_regions,
        manipulation_recipe,
        materialized_densities,
        region_selection,
    )
    updated_density_release = _update_density_release(
        original_density_release,
        updated_densities,
    )
    return updated_densities, updated_density_release
