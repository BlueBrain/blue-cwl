import json
import uuid
import pytest
from pathlib import Path
from copy import deepcopy

from cwl_registry.mmodel import schemas as test_module
from cwl_registry.utils import load_json, write_json


MOCK_ID = "https://bbp.epfl.ch/my-id"


def test_variant_info():
    """Test VariantInfo schema."""
    res = test_module.VariantInfo.parse_obj({"algorithm": "foo", "version": "v1"})
    assert res.algorithm == "foo"
    assert res.version == "v1"


def _create_canonical_model(parameters, distributions, overrides, out_dir):
    """Create a CanonicalMorphologyModel with random file names."""
    parameters_path = out_dir / f"parameters_{uuid.uuid4()}.json"
    write_json(filepath=parameters_path, data=parameters)

    distributions_path = out_dir / f"distributions_{uuid.uuid4()}.json"
    write_json(filepath=distributions_path, data=distributions)

    return test_module.CanonicalMorphologyModel.parse_obj(
        {
            "parameters": parameters_path,
            "distributions": distributions_path,
            "overrides": overrides,
        }
    )


@pytest.fixture
def canonical_out_dir(tmp_path):
    """Output directory for canonical morphology model files."""
    out_dir = tmp_path / "canonical_morphology_model"
    out_dir.mkdir()
    return out_dir


def test_canonical_morphology_model__init(parameters, distributions, canonical_out_dir):
    """Test initialization of CanonicalMorphologyModel."""
    model = _create_canonical_model(parameters, distributions, None, canonical_out_dir)

    assert load_json(model.parameters) == parameters
    assert load_json(model.distributions) == distributions
    assert model.overrides is None


def test_canonical_morphology_model__eq__true(parameters, distributions, canonical_out_dir):
    """Test equality between two CanonicalMorphologyModel instances.

    The two models have the same parameters and distributions contents and no overrides.
    """
    model1 = _create_canonical_model(parameters, distributions, None, canonical_out_dir)
    model2 = _create_canonical_model(parameters, distributions, None, canonical_out_dir)
    assert model1.checksum() == model2.checksum()
    assert model1 == model2


def test_canonical_morphology_model__eq__false(parameters, distributions, canonical_out_dir):
    """Test equality between two CanonicalMorphologyModel instances.

    The two models have different parameters contents and no overrides.
    """
    parameters2 = deepcopy(parameters)
    parameters2["some-value"] = "some-value"

    model1 = _create_canonical_model(parameters, distributions, None, canonical_out_dir)
    model2 = _create_canonical_model(parameters2, distributions, None, canonical_out_dir)
    assert model1.checksum() != model2.checksum()
    assert model1 != model2


@pytest.mark.parametrize(
    "overrides1, overrides2, expected",
    [
        (None, None, True),
        ({}, {}, True),
        ({"apical": {}}, {}, True),
        ({"apical": {}}, {"basal": {}}, True),
        ({"apical": {"step_size": None}}, {"basal": {}}, True),
        ({"apical": {"step_size": None}}, {"basal": {"radius": None}}, True),
        ({"apical": {"radius": 1.5}}, {"apical": {"radius": 1.5}}, True),
        ({"apical": {"radius": 1.5}}, {"apical": {"radius": 1.5, "step_size": None}}, True),
        ({"apical": {"radius": 1.5}}, {"basal": {"radius": 1.5}}, False),
        ({"apical": {"radius": 1.5}}, {"apical": {"radius": 1.0}}, False),
    ],
)
def test_canonical_morphology_model__eq__overrides(
    overrides1, overrides2, expected, parameters, distributions, canonical_out_dir
):
    """Test combinations of overrides and expected equivalence.

    Models should be equivalent when:
    - Parameters and distribution checsums are identical
    - No actual overrides are present. None is not considered as an actual override.
    - Actual overrides are equivalent.
    """
    model1 = _create_canonical_model(parameters, distributions, overrides1, canonical_out_dir)
    model2 = _create_canonical_model(parameters, distributions, overrides2, canonical_out_dir)

    res = model1.checksum() == model2.checksum()
    assert res is expected, (model1.overrides, model2.overrides)

    res = model1 == model2
    assert res is expected
