import pytest
import numpy as np

from cwl_registry.mmodel import recipe as test_module


@pytest.fixture
def materialized_ph_catalog():
    return {
        "placement_hints": [
            {
                "path": "/my-dir/[PH]1.nrrd",
                "regions": {
                    "Isocortex": {"hasLeafRegionPart": ["SSp-ll1", "AUDd1"], "layer": "L1"}
                },
            },
            {
                "path": "/my-dir/[PH]2.nrrd",
                "regions": {
                    "Isocortex": {
                        "hasLeafRegionPart": ["PL2", "ILA2", "ORBm2", "RSPv2"],
                        "layer": "L2",
                    }
                },
            },
            {
                "path": "/my-dir/[PH]3.nrrd",
                "regions": {"Isocortex": {"hasLeafRegionPart": ["FRP3", "MOp3"], "layer": "L3"}},
            },
            {
                "path": "/my-dir/[PH]4.nrrd",
                "regions": {
                    "Isocortex": {"hasLeafRegionPart": ["AUDp4", "SSp-ul4"], "layer": "L4"}
                },
            },
            {
                "path": "/my-dir/[PH]5.nrrd",
                "regions": {
                    "Isocortex": {"hasLeafRegionPart": ["VISpor5", "ORBm5"], "layer": "L5"}
                },
            },
            {
                "path": "/my-dir/[PH]6.nrrd",
                "regions": {"Isocortex": {"hasLeafRegionPart": ["ACA6b", "AUDp6a"], "layer": "L6"}},
            },
        ],
        "voxel_distance_to_region_bottom": {"path": "/my-dir/[PH]y.nrrd"},
    }


def test_build_region_structure(materialized_ph_catalog):
    res = test_module.build_region_structure(materialized_ph_catalog)

    expected = {
        "Isocortex": {
            "layers": ["1", "2", "3", "4", "5", "6"],
            "names": ["L1", "L2", "L3", "L4", "L5", "L6"],
            "thicknesses": [165, 149, 353, 190, 525, 700],
        },
        "SSp-ll1": {"layers": ["1"], "names": ["L1"], "thicknesses": [165]},
        "AUDd1": {"layers": ["1"], "names": ["L1"], "thicknesses": [165]},
        "PL2": {"layers": ["2"], "names": ["L2"], "thicknesses": [149]},
        "ILA2": {"layers": ["2"], "names": ["L2"], "thicknesses": [149]},
        "ORBm2": {"layers": ["2"], "names": ["L2"], "thicknesses": [149]},
        "RSPv2": {"layers": ["2"], "names": ["L2"], "thicknesses": [149]},
        "FRP3": {"layers": ["3"], "names": ["L3"], "thicknesses": [353]},
        "MOp3": {"layers": ["3"], "names": ["L3"], "thicknesses": [353]},
        "AUDp4": {"layers": ["4"], "names": ["L4"], "thicknesses": [190]},
        "SSp-ul4": {"layers": ["4"], "names": ["L4"], "thicknesses": [190]},
        "VISpor5": {"layers": ["5"], "names": ["L5"], "thicknesses": [525]},
        "ORBm5": {"layers": ["5"], "names": ["L5"], "thicknesses": [525]},
        "ACA6b": {"layers": ["6"], "names": ["L6"], "thicknesses": [700]},
        "AUDp6a": {"layers": ["6"], "names": ["L6"], "thicknesses": [700]},
    }

    assert res == expected


def test_build_cell_orientation_field(annotation):
    res = test_module.build_cell_orientation_field(annotation, orientations=None)

    in_brain = annotation.raw != 0

    # outside brain NaN
    assert np.isnan(res.raw[~in_brain]).all()

    # inside brain (1, 0, 0, 0)
    assert np.all((res.raw[in_brain] - np.array([1.0, 0.0, 0.0, 0.0])) == 0.0)


def test_build_cell_orientation_field__existing_orientations(annotation):
    in_brain = annotation.raw != 0

    orientations_raw = np.full(list(annotation.shape) + [4], fill_value=np.nan)

    default_orientation = np.array([1.0, 0.0, 0.0, 0.0])
    external_orientation = np.array([0.4, 0.3, 0.2, 0.1])

    orientations_raw[in_brain] = external_orientation

    # induce a default by setting this to nan
    orientations_raw[1, 137, 198, :] = np.nan

    orientations = annotation.with_data(orientations_raw)

    res = test_module.build_cell_orientation_field(annotation, orientations=orientations)

    in_brain_res = res.raw[in_brain]

    # first one corresponds to the nan we set above -> default
    assert (in_brain_res[0, :] == default_orientation).all()

    # the rest inside the brain are overwritten by the external orientation
    assert (in_brain_res[1:, :] == external_orientation).all()
