"""Utils for building mmodel tool inputs."""

from pathlib import Path

import numpy as np

from cwl_registry.mmodel.overrides import apply_overrides
from cwl_registry.mmodel.schemas import CanonicalMorphologyModel
from cwl_registry.utils import load_json

# TODO: Use region_map to group identical parameter regions
# pylint: disable=unused-argument


def build_synthesis_inputs(
    configuration: dict[str, dict[str, CanonicalMorphologyModel]], region_map
) -> tuple[dict, dict]:
    """Build synthesis inputs."""
    parameters, distributions = {}, {}
    for region, region_data in configuration.items():
        param_mtypes, distr_mtypes = {}, {}
        for mtype, canonical_model in region_data.items():
            params = load_json(canonical_model.parameters)

            # Remove the constraints because we don't have ordered layer info to construct
            # the region structure and layer constraints.
            if "context_constraints" in params:
                del params["context_constraints"]

            distrs = load_json(canonical_model.distributions)

            if canonical_model.overrides:
                params, distrs = apply_overrides(params, distrs, canonical_model.overrides)

            param_mtypes[mtype] = params
            distr_mtypes[mtype] = distrs

        parameters[region] = param_mtypes
        distributions[region] = distr_mtypes

    return parameters, distributions


def build_region_structure(ph_catalog: dict) -> dict:
    """Build region structure."""
    region_structure = {}
    visited = set()

    # hardcoded isocortex thicknesses for all layers
    # Note: will assign isocortex thicknesses to non-isocortex layers
    thicknesses = [165, 149, 353, 190, 525, 700]

    def get_entry(region):
        if region in region_structure:
            entry = region_structure[region]
        else:
            entry = {"layers": [], "names": {}, "thicknesses": {}}
            region_structure[region] = entry
            visited.add(region)

        return entry

    def update_entry(i, entry, layer_id, layer_name):
        entry["layers"].append(layer_id)
        entry["names"][layer_id] = layer_name
        entry["thicknesses"][layer_id] = thicknesses[i]

    for i, ph_entry in enumerate(ph_catalog["placement_hints"]):
        # region's structure layer if is the X in [PH]X.nrrd
        layer_id = Path(ph_entry["path"]).stem.removeprefix("[PH]")

        for region, region_data in ph_entry["regions"].items():
            layer_name = region_data["layer"]
            update_entry(i, get_entry(region), layer_id, layer_name)

            for leaf_region in region_data.get("hasLeafRegionPart", []):
                if leaf_region not in visited:
                    update_entry(i, get_entry(leaf_region), layer_id, layer_name)

    return region_structure


def build_cell_orientation_field(brain_regions, orientations=None):
    """Create a cell orientation field."""
    final = np.full(list(brain_regions.shape) + [4], fill_value=np.nan)

    in_brain = brain_regions.raw != 0

    # placeholder quaternions for all in-brain voxels
    final[in_brain] = (1.0, 0.0, 0.0, 0.0)

    # overwrite with non-nan quaternions
    if orientations:
        not_nan = in_brain & ~np.any(np.isnan(orientations.raw), axis=-1)
        final[not_nan] = orientations.raw[not_nan]

    return brain_regions.with_data(final)
