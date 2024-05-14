from copy import deepcopy
from pathlib import Path

import pytest
import numpy as np
from blue_cwl.mmodel import overrides as test_module

from blue_cwl.mmodel import schemas
from blue_cwl.utils import load_json
from numpy import testing as npt


DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture
def synthesis_overrides():
    return {
        "apical_dendrite": schemas.SynthesisOverrides(
            total_extent=10.0,
            randomness=0.001,
            orientation=[(0.0, 0.0, 1.0)],
            step_size={"norm": {"mean": 1.5, "std": 0.1}},
            radius=0.5,
        )
    }


def test_scale_barcode_list():
    barcode_list = [[[2.0, 1.0, 3.0, 4.0], [0.2, 0.1, 0.3, 0.4], [0.002, 0.001, np.nan, np.nan]]]

    res = test_module._scale_barcode_list(barcode_list, 10.0)

    expected = [[[10.0, 5.0, 3.0, 4.0], [1.0, 0.5, 0.3, 0.4], [0.01, 0.005, np.nan, np.nan]]]

    npt.assert_allclose(res, expected)


def test_apply_overrides__None(parameters, distributions):
    # by default all overrides are None
    overrides = {"basal_dendrite": schemas.SynthesisOverrides()}

    datasets = deepcopy(synthesis_datasets)

    new_parameters, new_distributions = test_module.apply_overrides(
        parameters, distributions, overrides
    )

    assert new_parameters == parameters
    assert new_distributions == distributions


def test_apply_overrides__None(parameters, distributions, synthesis_overrides):
    new_parameters, new_distributions = test_module.apply_overrides(
        parameters, distributions, synthesis_overrides
    )

    assert new_parameters["basal_dendrite"] == parameters["basal_dendrite"]
    assert new_distributions["basal_dendrite"] == distributions["basal_dendrite"]

    apical_params = new_parameters["apical_dendrite"]
    apical_distrs = new_distributions["apical_dendrite"]
    apical_overrides = synthesis_overrides["apical_dendrite"]

    assert apical_distrs != distributions["apical_dendrite"]

    assert apical_params["randomness"] == apical_overrides.randomness
    assert apical_params["radius"] == apical_overrides.radius
    assert apical_params["step_size"] == apical_overrides.step_size
    assert apical_params["orientation"] == {
        "mode": "normal_pia_constraint",
        "values": {"direction": {"mean": 0.5 * np.pi, "std": 0.0}},
    }
