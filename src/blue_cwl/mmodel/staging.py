"""Staging utils for mmodel."""

from functools import partial

from entity_management.nexus import load_by_id

from blue_cwl import utils
from blue_cwl.nexus import get_distribution_location_path
from blue_cwl.staging import (
    get_distribution_path_entry,
    get_entry_id,
    get_entry_property,
    transform_cached,
    transform_nested_dataset,
)


@transform_cached
def _get_parameters_distributions(
    entry_id: str, entry_data: dict, *, model_class, base=None, org=None, proj=None, token=None
):
    """Extract parameters and distributions data from resources."""
    json_data = load_by_id(
        entry_id,
        cross_bucket=True,
        base=base,
        org=org,
        proj=proj,
        token=token,
    )
    params_id = get_entry_id(json_data["morphologyModelParameter"])
    distrs_id = get_entry_id(json_data["morphologyModelDistribution"])

    return model_class(
        parameters=get_distribution_location_path(
            params_id, base=base, org=org, proj=proj, token=token
        ),
        distributions=get_distribution_location_path(
            distrs_id, base=base, org=org, proj=proj, token=token
        ),
        overrides=entry_data.get("overrides", None),
    )


def materialize_canonical_config(
    dataset: dict,
    model_class,
    *,
    output_file=None,
    labels_only=False,
    base=None,
    org=None,
    proj=None,
    token=None,
) -> dict:
    """Materialize canonical morphology model config."""
    result = _materialize_canonical_config(
        dataset, model_class, base=base, org=org, proj=proj, token=token
    )

    if labels_only:
        result = _convert_to_labels(result, leaf_func=lambda e: list(e.values())[0])

    if output_file:
        utils.write_json(filepath=output_file, data=result)

    return result


def _materialize_canonical_config(
    dataset, model_class, *, base=None, org=None, proj=None, token=None
):
    levels = (
        partial(
            get_entry_property, property_name="notation", base=base, org=org, proj=proj, token=token
        ),
        partial(
            get_entry_property, property_name="label", base=base, org=org, proj=proj, token=token
        ),
        partial(
            _get_parameters_distributions,
            model_class=model_class,
            base=base,
            org=org,
            proj=proj,
            token=token,
        ),
    )

    result = transform_nested_dataset(dataset, levels)

    return result


def _convert_to_labels(nested_data: dict, leaf_func) -> dict:
    return {
        region_data["notation"]: {
            mtype_data["label"]: leaf_func(mtype_data["hasPart"])
            for mtype_data in region_data["hasPart"].values()
        }
        for region_data in nested_data["hasPart"].values()
    }


def materialize_placeholders_config(
    dataset: dict,
    *,
    output_file=None,
    labels_only=False,
    base=None,
    org=None,
    proj=None,
    token=None,
) -> dict:
    """Materialize placeholders config."""
    result = _materialize_placeholders_config(dataset, base=base, org=org, proj=proj, token=token)

    if labels_only:
        result = _convert_to_labels(result, leaf_func=lambda e: [v["path"] for v in e.values()])

    if output_file:
        utils.write_json(filepath=output_file, data=result)

    return result


def _materialize_placeholders_config(dataset, *, base=None, org=None, proj=None, token=None):
    """Materialize v2 placeholder config.

    In v2 it is guaranteed that notation and label are present in the config.
    """
    levels = (
        partial(
            get_entry_property, property_name="notation", base=base, org=org, proj=proj, token=token
        ),
        partial(
            get_entry_property, property_name="label", base=base, org=org, proj=proj, token=token
        ),
        partial(get_distribution_path_entry, base=base, org=org, proj=proj, token=token),
    )

    result = transform_nested_dataset(dataset, levels)

    return result
