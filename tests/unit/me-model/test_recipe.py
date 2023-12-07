from copy import deepcopy
from cwl_registry.me_model import recipe as test_module


def test_build_me_model_recipe(materialized_me_model_config):
    res = test_module.build_me_model_recipe(materialized_me_model_config)
    assert res == {
        "AAA": {
            "GEN_mtype": {
                "GEN_etype": {
                    "assignmentAlgorithm": "assignOne",
                    "eModel": "AAA__GEN_mtype__GEN_etype__emodel",
                }
            },
            "GIN_mtype": {
                "GIN_etype": {
                    "assignmentAlgorithm": "assignOne",
                    "eModel": "AAA__GIN_mtype__GIN_etype__emodel",
                }
            },
        },
        "ACAd1": {
            "L1_DAC": {
                "bNAC": {
                    "assignmentAlgorithm": "assignOne",
                    "eModel": "ACAd1__L1_DAC__bNAC__override",
                    "axonInitialSegmentAssignment": {"fixedValue": {"value": 1}},
                },
                "cNAC": {"assignmentAlgorithm": "assignOne", "eModel": "ACAd1__L1_DAC__cNAC"},
            }
        },
    }


def test_build_me_model_recipe__no_overrides(materialized_me_model_config):
    config = deepcopy(materialized_me_model_config)
    config["overrides"]["neurons_me_model"] = {}

    res = test_module.build_me_model_recipe(config)
    assert res == {
        "AAA": {
            "GEN_mtype": {
                "GEN_etype": {
                    "assignmentAlgorithm": "assignOne",
                    "eModel": "AAA__GEN_mtype__GEN_etype__emodel",
                }
            },
            "GIN_mtype": {
                "GIN_etype": {
                    "assignmentAlgorithm": "assignOne",
                    "eModel": "AAA__GIN_mtype__GIN_etype__emodel",
                }
            },
        },
        "ACAd1": {
            "L1_DAC": {
                "bNAC": {
                    "assignmentAlgorithm": "assignOne",
                    "eModel": "ACAd1__L1_DAC__bNAC__emodel",
                },
                "cNAC": {"assignmentAlgorithm": "assignOne", "eModel": "ACAd1__L1_DAC__cNAC"},
            }
        },
    }
