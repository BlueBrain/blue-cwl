"""MEModel recipe generation."""
from copy import deepcopy


def build_me_model_recipe(me_model_config):
    """Build me-model configuration."""
    defaults = me_model_config["defaults"]["neurons_me_model"]
    overrides = me_model_config["overrides"]["neurons_me_model"]
    result = _convert_to_labels(defaults, leaf_func=_default_strategy)
    result = _apply_overrides(defaults, overrides, result)
    return result


def _default_strategy(data):
    emodel = list(data["hasPart"].values())[0]
    return {
        "assignmentAlgorithm": "assignOne",
        "eModel": emodel,
    }


def _apply_overrides(defaults, overrides, contracted):
    result = deepcopy(contracted)
    for region_id, region_data in overrides.items():
        default_region_data = defaults["hasPart"][region_id]
        region_notation = default_region_data["notation"]
        for mtype_id, mtype_data in region_data.items():
            default_mtype_data = default_region_data["hasPart"][mtype_id]
            mtype_label = default_mtype_data["label"]
            for etype_id, etype_data in mtype_data.items():
                default_etype_data = default_mtype_data["hasPart"][etype_id]
                etype_label = default_etype_data["label"]
                result[region_notation][mtype_label][etype_label] = etype_data
    return result


def _convert_to_labels(nested_data: dict, leaf_func) -> dict:
    return {
        region_data["notation"]: {
            mtype_data["label"]: {
                etype_data["label"]: leaf_func(etype_data)
                for etype_data in mtype_data["hasPart"].values()
            }
            for mtype_data in region_data["hasPart"].values()
        }
        for region_data in nested_data["hasPart"].values()
    }
