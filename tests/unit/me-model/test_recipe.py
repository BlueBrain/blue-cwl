from copy import deepcopy
from cwl_registry.me_model import recipe as test_module


def test_build_me_model_recipe(materialized_me_model_config):
    res = test_module.build_me_model_recipe(materialized_me_model_config)
    assert res == {
        "library": {
            "eModel": {
                "emodel_8f840b": "AAA__GEN_mtype__GEN_etype__emodel",
                "emodel_23da5a": "AAA__GIN_mtype__GIN_etype__emodel",
                "emodel_371f77": "ACAd1__L1_DAC__bNAC__override",
                "emodel_0ed829": "ACAd1__L1_DAC__cNAC",
            }
        },
        "configuration": {
            "AAA": {
                "GEN_mtype": {
                    "GEN_etype": {"assignmentAlgorithm": "assignOne", "eModel": "emodel_8f840b"}
                },
                "GIN_mtype": {
                    "GIN_etype": {"assignmentAlgorithm": "assignOne", "eModel": "emodel_23da5a"}
                },
            },
            "ACAd1": {
                "L1_DAC": {
                    "bNAC": {
                        "assignmentAlgorithm": "assignOne",
                        "eModel": "emodel_371f77",
                        "axonInitialSegmentAssignment": {"fixedValue": {"value": 1}},
                    },
                    "cNAC": {"assignmentAlgorithm": "assignOne", "eModel": "emodel_0ed829"},
                }
            },
        },
    }


def test_build_me_model_recipe__no_overrides(materialized_me_model_config):
    config = deepcopy(materialized_me_model_config)
    config["overrides"]["neurons_me_model"] = {}

    res = test_module.build_me_model_recipe(config)
    assert res == {
        "library": {
            "eModel": {
                "emodel_8f840b": "AAA__GEN_mtype__GEN_etype__emodel",
                "emodel_23da5a": "AAA__GIN_mtype__GIN_etype__emodel",
                "emodel_9d332c": "ACAd1__L1_DAC__bNAC__emodel",
                "emodel_0ed829": "ACAd1__L1_DAC__cNAC",
            }
        },
        "configuration": {
            "AAA": {
                "GEN_mtype": {
                    "GEN_etype": {"assignmentAlgorithm": "assignOne", "eModel": "emodel_8f840b"}
                },
                "GIN_mtype": {
                    "GIN_etype": {"assignmentAlgorithm": "assignOne", "eModel": "emodel_23da5a"}
                },
            },
            "ACAd1": {
                "L1_DAC": {
                    "bNAC": {"assignmentAlgorithm": "assignOne", "eModel": "emodel_9d332c"},
                    "cNAC": {"assignmentAlgorithm": "assignOne", "eModel": "emodel_0ed829"},
                }
            },
        },
    }
