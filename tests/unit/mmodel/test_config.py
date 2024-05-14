import re
from copy import deepcopy
from blue_cwl.mmodel import config as test_module
import pytest
from blue_cwl.exceptions import CWLWorkflowError


@pytest.fixture
def defaults():
    return {
        "topological_synthesis": {
            "hasPart": {
                "r1": {
                    "notation": "r1",
                    "hasPart": {
                        "m1": {
                            "label": "m1",
                            "hasPart": {
                                "m1-id-c": {"about": "CanonicalMorphologyModel", "_rev": 3}
                            },
                        },
                        "m2": {
                            "label": "m2",
                            "hasPart": {
                                "m2-id-c": {"about": "CanonicalMorphologyModel", "_rev": 3}
                            },
                        },
                    },
                },
                "r2": {
                    "notation": "r2",
                    "hasPart": {
                        "m1": {
                            "label": "m1",
                            "hasPart": {
                                "m1-id-c": {"about": "CanonicalMorphologyModel", "_rev": 3}
                            },
                        },
                        "m3": {
                            "label": "m3",
                            "hasPart": {
                                "m3-id-c": {"about": "CanonicalMorphologyModel", "_rev": 3}
                            },
                        },
                    },
                },
                "r3": {
                    "notation": "r3",
                    "hasPart": {
                        "m2": {
                            "label": "m2",
                            "hasPart": {
                                "m2-id-c": {"about": "CanonicalMorphologyModel", "_rev": 3}
                            },
                        },
                        "m3": {
                            "label": "m3",
                            "hasPart": {
                                "m3-id-c": {"about": "CanonicalMorphologyModel", "_rev": 3}
                            },
                        },
                    },
                },
            },
        },
        "placeholder_assignment": {
            "hasPart": {
                "r1": {
                    "notation": "r1",
                    "hasPart": {
                        "m1": {
                            "label": "m1",
                            "hasPart": {"m1-id-p": {"about": "Placeholder", "_rev": 1}},
                        },
                        "m2": {
                            "label": "m2",
                            "hasPart": {"m2-id-p": {"about": "Placeholder", "_rev": 1}},
                        },
                    },
                },
                "r2": {
                    "notation": "r2",
                    "hasPart": {
                        "m1": {
                            "label": "m1",
                            "hasPart": {"m1-id-p": {"about": "Placeholder", "_rev": 1}},
                        },
                        "m3": {
                            "label": "m3",
                            "hasPart": {"m3-id-p": {"about": "Placeholder", "_rev": 1}},
                        },
                    },
                },
                "r3": {
                    "notation": "r3",
                    "hasPart": {
                        "m2": {
                            "label": "m2",
                            "hasPart": {"m2-id-p": {"about": "Placeholder", "_rev": 1}},
                        },
                        "m3": {
                            "label": "m3",
                            "hasPart": {"m3-id-p": {"about": "Placeholder", "_rev": 1}},
                        },
                    },
                },
            },
        },
    }


@pytest.fixture
def configuration():
    return {
        "topological_synthesis": {
            "r1": {
                "m2": {
                    "overrides": {
                        "apical_dendrite": {
                            "randomness": 0.11,
                            "step_size": {"norm": {"mean": 7.6, "std": 0.2}},
                            "targeting": 0.34,
                        },
                    },
                    "@id": "override-m2-id",
                    "_rev": 5,
                },
            },
            "r3": {
                "m2": {"@id": "override-m1-id", "_rev": 2},
                "m3": {"_rev": 1},
            },
        }
    }


def test_split_config(defaults, configuration):
    canonical_key = "topological_synthesis"
    placeholder_key = "placeholder_assignment"
    placeholders, canonicals = test_module.split_config(
        defaults, configuration, canonical_key, placeholder_key
    )

    assert placeholders == {
        "hasPart": {
            "r1": {
                "notation": "r1",
                "hasPart": {
                    "m1": {
                        "label": "m1",
                        "hasPart": {"m1-id-p": {"about": "Placeholder", "_rev": 1}},
                    }
                },
            },
            "r2": {
                "notation": "r2",
                "hasPart": {
                    "m1": {
                        "label": "m1",
                        "hasPart": {"m1-id-p": {"about": "Placeholder", "_rev": 1}},
                    },
                    "m3": {
                        "label": "m3",
                        "hasPart": {"m3-id-p": {"about": "Placeholder", "_rev": 1}},
                    },
                },
            },
        }
    }
    canonical_configuration = configuration["topological_synthesis"]
    # check consistency of key hierarchy
    assert canonicals["hasPart"].keys() == canonical_configuration.keys()
    for k, v in canonicals["hasPart"].items():
        assert v["hasPart"].keys() == canonical_configuration[k].keys()

    assert canonicals == {
        "hasPart": {
            "r1": {
                "notation": "r1",
                "hasPart": {
                    "m2": {
                        "label": "m2",
                        "hasPart": {
                            "override-m2-id": {
                                "overrides": {
                                    "apical_dendrite": {
                                        "randomness": 0.11,
                                        "step_size": {"norm": {"mean": 7.6, "std": 0.2}},
                                        "targeting": 0.34,
                                    }
                                },
                                "_rev": 5,
                            }
                        },
                    }
                },
            },
            "r3": {
                "notation": "r3",
                "hasPart": {
                    "m2": {
                        "label": "m2",
                        "hasPart": {
                            "override-m1-id": {"_rev": 2},
                        },
                    },
                    "m3": {
                        "label": "m3",
                        "hasPart": {"m3-id-c": {"_rev": 1}},
                    },
                },
            },
        }
    }


def test_check_consistency__raises_regions(defaults):
    canonicals = deepcopy(defaults["topological_synthesis"])
    placeholders = deepcopy(defaults["placeholder_assignment"])

    del canonicals["hasPart"]["r1"]
    del placeholders["hasPart"]["r3"]

    expected_message = (
        "Default canonical and placeholder regions differ:\n"
        "canonicals - placeholders: {'r3'}\n"
        "placeholders - canonicals: {'r1'}"
    )
    with pytest.raises(CWLWorkflowError, match=expected_message):
        test_module._check_consistency(canonicals, placeholders, {})


def test_check_consistency__raises_mtypes(defaults):
    canonicals = deepcopy(defaults["topological_synthesis"])
    placeholders = deepcopy(defaults["placeholder_assignment"])

    del canonicals["hasPart"]["r1"]["hasPart"]["m2"]
    del placeholders["hasPart"]["r3"]["hasPart"]["m3"]

    expected_message = (
        "Default canonical and placeholder mtypes differ:\n"
        "Region: r1\n\tplaceholders - canonicals: {'m2'}\n"
        "Region: r3\n\tcanonicals - placeholders: {'m3'}"
    )
    with pytest.raises(CWLWorkflowError, match=expected_message):
        test_module._check_consistency(canonicals, placeholders, {})


def test_check_consistency__raises_configuration_regions(defaults, configuration):
    canonicals = deepcopy(defaults["topological_synthesis"])
    placeholders = deepcopy(defaults["placeholder_assignment"])

    del canonicals["hasPart"]["r1"]
    del placeholders["hasPart"]["r1"]

    expected_message = re.escape("Configuration regions not in default canonicals:\n['r1']")
    with pytest.raises(CWLWorkflowError, match=expected_message):
        test_module._check_consistency(
            canonicals, placeholders, configuration["topological_synthesis"]
        )


def test_check_consistency__raises_configuration_mtypes(defaults, configuration):
    canonicals = deepcopy(defaults["topological_synthesis"])
    placeholders = deepcopy(defaults["placeholder_assignment"])

    del canonicals["hasPart"]["r1"]["hasPart"]["m2"]
    del placeholders["hasPart"]["r1"]["hasPart"]["m2"]

    expected_message = (
        "Mtypes in configuration that are not in the default canonicals:\n"
        "Region: r1\n\tIn config but not in canonicals: {'m2'}"
    )
    with pytest.raises(CWLWorkflowError, match=expected_message):
        test_module._check_consistency(
            canonicals, placeholders, configuration["topological_synthesis"]
        )
