import pytest
from pathlib import Path
from cwl_registry.wrappers import placeholder_morphology_assignment as test_module

DATA_DIR = Path(__file__).parent.parent / "data"


def test__assign_morphologies():
    mtype_to_morphologies = {
        k: [Path(f"morph_{k}.swc")]
        for k in [
            "L23_MC",
            "L5_TPC:A",
        ]
    }
    res = test_module._assign_morphologies(
        nodes_file=DATA_DIR / "morphology-in-space/nodes__me_type_property.h5",
        population_name="mc2_Column_neurons",
        mtype_to_morphologies=mtype_to_morphologies,
        seed=42,
    )

    assert "morphology" in res
    assert "@dynamics/holding_current" in res
    assert "@dynamics/threshold_current" in res
