"""Staging utils for mmodel."""
from functools import partial

from cwl_registry import utils
from cwl_registry.nexus import get_resource_json_ld
from cwl_registry.staging import (
    get_distribution_path_from_resource,
    get_entry_id,
    get_morphology_paths,
    transform_cached,
    transform_nested_dataset,
)


@transform_cached
def _get_parameters_distributions(entry_id: str, entry_data: dict, forge, model_class):
    """Extract parameters and distributions data from resources."""
    json_data = get_resource_json_ld(resource_id=entry_id, forge=forge)

    params_id = get_entry_id(json_data["morphologyModelParameter"])
    distrs_id = get_entry_id(json_data["morphologyModelDistribution"])

    return model_class(
        parameters=get_distribution_path_from_resource(forge, params_id)["path"],
        distributions=get_distribution_path_from_resource(forge, distrs_id)["path"],
        overrides=entry_data.get("overrides", None),
    )


def _get_existing_region_notation(_, entry_data):
    return {"label": entry_data["notation"]}


def _get_existing_label(_, entry_data):
    return {"label": entry_data["label"]}


def materialize_canonical_config(
    dataset: dict, forge, model_class, output_file=None, labels_only=False
) -> dict:
    """Materialize canonical morphology model config."""
    result = _materialize_canonical_config(dataset, forge, model_class)

    if labels_only:
        result = _convert_to_labels(result, leaf_func=lambda e: list(e.values())[0])

    if output_file:
        utils.write_json(filepath=output_file, data=result)

    return result


def _materialize_canonical_config(dataset, forge, model_class):
    levels = (
        _get_existing_region_notation,
        _get_existing_label,
        partial(_get_parameters_distributions, forge=forge, model_class=model_class),
    )

    result = transform_nested_dataset(dataset, levels)

    return result


def _convert_to_labels(nested_data: dict, leaf_func) -> dict:
    return {
        region_data["label"]: {
            mtype_data["label"]: leaf_func(mtype_data["hasPart"])
            for mtype_data in region_data["hasPart"].values()
        }
        for region_data in nested_data["hasPart"].values()
    }


def materialize_placeholders_config(
    dataset: dict,
    forge,
    output_file=None,
    labels_only=False,
) -> dict:
    """Materialize placeholders config."""
    result = _materialize_placeholders_config(dataset, forge)

    if labels_only:
        result = _convert_to_labels(result, leaf_func=lambda e: [v["path"] for v in e.values()])

    if output_file:
        utils.write_json(filepath=output_file, data=result)

    return result


def _materialize_placeholders_config(dataset, forge):
    """Materialize v2 placeholder config.

    In v2 it is guaranteed that notation and label are present in the config.
    """
    levels = (
        _get_existing_region_notation,
        _get_existing_label,
        partial(get_morphology_paths, forge=forge),
    )

    result = transform_nested_dataset(dataset, levels)

    return result
