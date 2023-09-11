"""Staging utils for mmodel."""
from functools import partial

from cwl_registry import utils
from cwl_registry.nexus import get_resource
from cwl_registry.staging import (
    get_distribution_path_from_resource,
    get_label,
    get_morphology_paths,
    get_region_label,
    transform_cached,
    transform_nested_dataset,
)


@transform_cached
def _get_parameters_distributions(entry_id: str, entry_data: dict, forge, model_class):
    """Extract parameters and distributions data from resources."""
    resource = get_resource(resource_id=entry_id, forge=forge)

    params_id = resource.morphologyModelParameter.id
    distrs_id = resource.morphologyModelDistribution.id

    return model_class(
        parameters=get_distribution_path_from_resource(forge, params_id)["path"],
        distributions=get_distribution_path_from_resource(forge, distrs_id)["path"],
        overrides=entry_data.get("overrides", None),
    )


def materialize_canonical_config(
    dataset: dict, forge, model_class, output_file=None, labels_only=False
) -> dict:
    """Materialize canonical morphology model config."""
    levels = (
        partial(get_region_label, forge=forge),
        partial(get_label, forge=forge),
        partial(_get_parameters_distributions, forge=forge, model_class=model_class),
    )

    result = transform_nested_dataset(dataset, levels)

    if labels_only:
        result = _convert_to_labels(result, leaf_func=lambda e: list(e.values())[0])

    if output_file:
        utils.write_json(filepath=output_file, data=result)

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
    dataset: dict, forge, output_file=None, labels_only=False
) -> dict:
    """Materialize placeholders config."""
    levels = (
        partial(get_region_label, forge=forge),
        partial(get_label, forge=forge),
        partial(get_morphology_paths, forge=forge),
    )

    result = transform_nested_dataset(dataset, levels)

    if labels_only:
        result = _convert_to_labels(result, leaf_func=lambda e: [v["path"] for v in e.values()])

    if output_file:
        utils.write_json(filepath=output_file, data=result)

    return result
