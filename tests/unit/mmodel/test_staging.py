from unittest.mock import patch
import pytest

from cwl_registry.mmodel import staging as test_module
from cwl_registry.exceptions import CWLWorkflowError
from cwl_registry.utils import load_json


MORPH_ID = "https://bbp.epfl.ch/neurosciencegraph/data/50d708b6-1868-419b-8995-aea05972f100"
MOCK_CANONICAL_ID = "https://bbp.epfl.ch/my-canonical-id"


def test_get_parameter_distributions():
    def get_path(entry_id, *args, **kwargs):
        if entry_id == "distr-id":
            return "distr-path"
        if entry_id == "param-id?rev=2":
            return "param-path"

        raise ValueError(entry_id)

    metadata = {
        "morphologyModelDistribution": {"@id": "distr-id", "@type": "MorphologyModelDistribution"},
        "morphologyModelParameter": {
            "@id": "param-id",
            "@type": "MorphologyModelParameter",
            "_rev": 2,
        },
    }

    with (
        patch("cwl_registry.mmodel.staging.load_by_id", return_value=metadata),
        patch("cwl_registry.mmodel.staging.get_distribution_location_path", side_effect=get_path),
    ):
        res = test_module._get_parameters_distributions(
            entry_id="my-id",
            entry_data={"overrides": "my-overrides"},
            model_class=dict,
        )

        assert res == {
            "parameters": "param-path",
            "distributions": "distr-path",
            "overrides": "my-overrides",
        }


@pytest.fixture
def canonicals_config():
    return {
        "version": 2,
        "hasPart": {
            "http://api.brain-map.org/api/v2/data/Structure/935": {
                "label": "Anterior cingulate area, dorsal part, layer 1",
                "notation": "ACAd1",
                "about": "BrainRegion",
                "hasPart": {
                    "http://uri.interlex.org/base/ilx_0383192": {
                        "label": "L1_DAC",
                        "about": "NeuronMorphologicalType",
                        "hasPart": {
                            MOCK_CANONICAL_ID: {"about": "CanonicalMorphologyModel", "_rev": 3}
                        },
                    },
                    "http://uri.interlex.org/base/ilx_0383193": {
                        "label": "L1_HAC",
                        "about": "NeuronMorphologicalType",
                        "hasPart": {
                            MOCK_CANONICAL_ID: {"about": "CanonicalMorphologyModel", "_rev": 3}
                        },
                    },
                },
            },
            "http://api.brain-map.org/api/v2/data/Structure/614454342": {
                "label": "Anterior cingulate area, dorsal part, layer 2",
                "notation": "ACAd2",
                "about": "BrainRegion",
                "hasPart": {
                    "http://uri.interlex.org/base/ilx_0383198": {
                        "label": "L23_BP",
                        "about": "NeuronMorphologicalType",
                        "hasPart": {
                            MOCK_CANONICAL_ID: {"about": "CanonicalMorphologyModel", "_rev": 3}
                        },
                    }
                },
            },
        },
    }


@pytest.fixture
def materialized_canonical_config():
    return {
        "hasPart": {
            "http://api.brain-map.org/api/v2/data/Structure/935": {
                "notation": "ACAd1",
                "hasPart": {
                    "http://uri.interlex.org/base/ilx_0383192": {
                        "label": "L1_DAC",
                        "hasPart": {
                            "https://bbp.epfl.ch/my-canonical-id": {
                                "parameters": "foo",
                                "distributions": "bar",
                                "overrides": None,
                            }
                        },
                    },
                    "http://uri.interlex.org/base/ilx_0383193": {
                        "label": "L1_HAC",
                        "hasPart": {
                            "https://bbp.epfl.ch/my-canonical-id": {
                                "parameters": "foo",
                                "distributions": "bar",
                                "overrides": None,
                            }
                        },
                    },
                },
            },
            "http://api.brain-map.org/api/v2/data/Structure/614454342": {
                "notation": "ACAd2",
                "hasPart": {
                    "http://uri.interlex.org/base/ilx_0383198": {
                        "label": "L23_BP",
                        "hasPart": {
                            "https://bbp.epfl.ch/my-canonical-id": {
                                "parameters": "foo",
                                "distributions": "bar",
                                "overrides": None,
                            }
                        },
                    }
                },
            },
        }
    }


def test__materialize_canonical_config(canonicals_config, materialized_canonical_config):
    def get_params_distrs(entry_id, entry_data, model_class, *args, **kwargs):
        if entry_id == f"{MOCK_CANONICAL_ID}?rev=3":
            return {
                "parameters": "foo",
                "distributions": "bar",
                "overrides": None,
            }
        raise ValueError(entry_id)

    with patch(
        "cwl_registry.mmodel.staging._get_parameters_distributions", side_effect=get_params_distrs
    ):
        res = test_module._materialize_canonical_config(canonicals_config, None)
        assert res == materialized_canonical_config


def test_materialize_canonical_config(canonicals_config, materialized_canonical_config, tmp_path):
    output_file = tmp_path / "out.json"

    with patch(
        "cwl_registry.mmodel.staging._materialize_canonical_config",
        return_value=materialized_canonical_config,
    ):
        res = test_module.materialize_canonical_config(
            canonicals_config,
            output_file=output_file,
            labels_only=True,
            model_class=dict,
        )

        assert res == {
            "ACAd1": {
                "L1_DAC": {"parameters": "foo", "distributions": "bar", "overrides": None},
                "L1_HAC": {"parameters": "foo", "distributions": "bar", "overrides": None},
            },
            "ACAd2": {"L23_BP": {"parameters": "foo", "distributions": "bar", "overrides": None}},
        }

        assert res == load_json(output_file)


@pytest.fixture
def placeholders_config():
    """Placeholders config"""
    return {
        "version": 2,
        "hasPart": {
            "http://api.brain-map.org/api/v2/data/Structure/23": {
                "notation": "AAA",
                "hasPart": {
                    "https://bbp.epfl.ch/ontologies/core/bmo/GenericExcitatoryNeuronMType": {
                        "label": "GEN_mtype",
                        "hasPart": {MORPH_ID: {"about": "NeuronMorphology", "_rev": 4}},
                    },
                    "https://bbp.epfl.ch/ontologies/core/bmo/GenericInhibitoryNeuronMType": {
                        "label": "GIN_mtype",
                        "hasPart": {MORPH_ID: {"about": "NeuronMorphology", "_rev": 4}},
                    },
                },
            },
            "http://api.brain-map.org/api/v2/data/Structure/935": {
                "notation": "ACAd1",
                "hasPart": {
                    "http://uri.interlex.org/base/ilx_0383192": {
                        "label": "L1_DAC",
                        "hasPart": {MORPH_ID: {"about": "NeuronMorphology", "_rev": 4}},
                    },
                },
            },
        },
    }


@pytest.fixture
def materialized_config():
    return {
        "hasPart": {
            "http://api.brain-map.org/api/v2/data/Structure/23": {
                "notation": "AAA",
                "hasPart": {
                    "https://bbp.epfl.ch/ontologies/core/bmo/GenericExcitatoryNeuronMType": {
                        "label": "GEN_mtype",
                        "hasPart": {MORPH_ID: {"path": "foo"}},
                    },
                    "https://bbp.epfl.ch/ontologies/core/bmo/GenericInhibitoryNeuronMType": {
                        "label": "GIN_mtype",
                        "hasPart": {MORPH_ID: {"path": "foo"}},
                    },
                },
            },
            "http://api.brain-map.org/api/v2/data/Structure/935": {
                "notation": "ACAd1",
                "hasPart": {
                    "http://uri.interlex.org/base/ilx_0383192": {
                        "label": "L1_DAC",
                        "hasPart": {MORPH_ID: {"path": "foo"}},
                    }
                },
            },
        }
    }


@pytest.fixture
def materialized_config_labels():
    return {
        "hasPart": {
            "http://api.brain-map.org/api/v2/data/Structure/23": {
                "label": "AAA",
                "hasPart": {
                    "https://bbp.epfl.ch/ontologies/core/bmo/GenericExcitatoryNeuronMType": {
                        "label": "GEN_mtype",
                        "hasPart": {MORPH_ID: {"path": "foo"}},
                    },
                    "https://bbp.epfl.ch/ontologies/core/bmo/GenericInhibitoryNeuronMType": {
                        "label": "GIN_mtype",
                        "hasPart": {MORPH_ID: {"path": "foo"}},
                    },
                },
            },
            "http://api.brain-map.org/api/v2/data/Structure/935": {
                "label": "ACAd1",
                "hasPart": {
                    "http://uri.interlex.org/base/ilx_0383192": {
                        "label": "L1_DAC",
                        "hasPart": {MORPH_ID: {"path": "foo"}},
                    }
                },
            },
        }
    }


def test_materialize_placeholders_config(placeholders_config, materialized_config, tmp_path):
    output_file = tmp_path / "out.json"

    with patch(
        "cwl_registry.mmodel.staging._materialize_placeholders_config",
        return_value=materialized_config,
    ):
        res = test_module.materialize_placeholders_config(
            placeholders_config, output_file=output_file, labels_only=True
        )

        assert res == {
            "AAA": {"GEN_mtype": ["foo"], "GIN_mtype": ["foo"]},
            "ACAd1": {"L1_DAC": ["foo"]},
        }

        assert res == load_json(output_file)


def test__materialize_placeholders_config(placeholders_config, materialized_config):
    def get_path(entry_id, *args, **kwargs):
        if entry_id == f"{MORPH_ID}?rev=4":
            return {"path": "foo"}

        raise ValueError()

    with patch("cwl_registry.mmodel.staging.get_distribution_path_entry", side_effect=get_path):
        res = test_module._materialize_placeholders_config(placeholders_config)
        assert res == materialized_config
