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


def test_entity_info__with_rev():
    """Test EntityInfo schema with revision."""
    res = test_module.EntityInfo.parse_obj({"id": MOCK_ID, "_rev": 2})
    assert str(res.id) == MOCK_ID
    assert res.rev == 2


def test_entity_info__no_rev():
    """Test EntityInfo schema without revision."""
    res = test_module.EntityInfo.parse_obj({"id": MOCK_ID})
    assert str(res.id) == MOCK_ID
    assert res.rev is None


def test_entity_info__full_id():
    """Test EntityInfo's full_id."""
    res = test_module.EntityInfo.parse_obj({"id": MOCK_ID}).full_id
    assert str(res) == MOCK_ID

    res = test_module.EntityInfo.parse_obj({"id": MOCK_ID, "_rev": 10}).full_id
    assert res == f"{MOCK_ID}?rev=10"


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


@pytest.mark.parametrize(
    "inputs, expected",
    [
        ({"id": MOCK_ID}, {"id": MOCK_ID, "rev": None, "overrides": None}),
        ({"id": MOCK_ID, "_rev": 2}, {"id": MOCK_ID, "rev": 2, "overrides": None}),
        (
            {"id": MOCK_ID, "_rev": 2, "overrides": {"apical": {"radius": 2.0}}},
            {
                "id": MOCK_ID,
                "rev": 2,
                "overrides": {
                    "apical": {
                        "radius": 2.0,
                        "total_extent": None,
                        "randomness": None,
                        "orientation": None,
                        "step_size": None,
                    }
                },
            },
        ),
    ],
)
def test_configuration_entry(inputs, expected):
    res = test_module.ConfigurationEntry.parse_obj(inputs)
    json_data = res.dict()
    json_data["id"] = str(json_data["id"])
    assert json_data == expected


def test_canonical_distribution_config(canonical_distribution_config):
    data = json.loads(canonical_distribution_config.json())
    assert data == {
        "hasPart": {
            "http://api.brain-map.org/api/v2/data/Structure/935": {
                "hasPart": {
                    "http://uri.interlex.org/base/ilx_0383192": {
                        "hasPart": {
                            "https://bbp.epfl.ch/model-id-1": {
                                "_rev": 6,
                                "about": "CanonicalMorphologyModel",
                            },
                        },
                    },
                    "http://uri.interlex.org/base/ilx_0383194": {
                        "hasPart": {
                            "https://bbp.epfl.ch/model-id-2": {
                                "_rev": 6,
                                "about": "CanonicalMorphologyModel",
                            },
                        },
                    },
                    "http://uri.interlex.org/base/ilx_0383196": {
                        "hasPart": {
                            "https://bbp.epfl.ch/model-id-3": {
                                "_rev": 6,
                                "about": "CanonicalMorphologyModel",
                            },
                        },
                    },
                },
            },
            "http://api.brain-map.org/api/v2/data/Structure/927": {
                "hasPart": {
                    "http://uri.interlex.org/base/ilx_0383228": {
                        "hasPart": {
                            "https://bbp.epfl.ch/model-id-4": {
                                "_rev": 6,
                                "about": "CanonicalMorphologyModel",
                            },
                        },
                    },
                    "http://uri.interlex.org/base/ilx_0381376": {
                        "hasPart": {
                            "https://bbp.epfl.ch/model-id-5": {
                                "_rev": 6,
                                "about": "CanonicalMorphologyModel",
                            },
                        },
                    },
                },
            },
        }
    }


def test_placeholder_distribution_config(placeholder_distribution_config):
    data = json.loads(placeholder_distribution_config.json())

    assert data == {
        "hasPart": {
            "http://api.brain-map.org/api/v2/data/Structure/984": {
                "hasPart": {
                    "https://bbp.epfl.ch/ontologies/core/bmo/GenericExcitatoryNeuronMType": {
                        "hasPart": {
                            "https://bbp.epfl.ch/placeholder-1": {"about": "NeuronMorphology"}
                        },
                    },
                    "https://bbp.epfl.ch/ontologies/core/bmo/GenericInhibitoryNeuronMType": {
                        "hasPart": {
                            "https://bbp.epfl.ch/placeholder-2": {"about": "NeuronMorphology"}
                        },
                    },
                },
            },
            "http://api.brain-map.org/api/v2/data/Structure/279": {
                "hasPart": {
                    "http://uri.interlex.org/base/ilx_0383230": {
                        "hasPart": {
                            "https://bbp.epfl.ch/placeholder-3": {"about": "NeuronMorphology"}
                        },
                    },
                    "http://uri.interlex.org/base/ilx_0381376": {
                        "hasPart": {
                            "https://bbp.epfl.ch/placeholder-4": {"about": "NeuronMorphology"}
                        },
                    },
                },
            },
            "http://api.brain-map.org/api/v2/data/Structure/1035": {
                "hasPart": {
                    "http://uri.interlex.org/base/ilx_0383220": {
                        "hasPart": {
                            "https://bbp.epfl.ch/placeholder-5": {"about": "NeuronMorphology"}
                        },
                    }
                },
            },
        },
    }
