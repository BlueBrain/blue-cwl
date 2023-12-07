"""Staging module."""
from copy import deepcopy
from functools import partial
from pathlib import Path

from entity_management.core import Entity

from cwl_registry.exceptions import CWLWorkflowError
from cwl_registry.me_model.entity import EModel
from cwl_registry.nexus import read_json_file_from_resource_id
from cwl_registry.staging import get_entry_id, transform_cached, transform_nested_dataset
from cwl_registry.utils import write_json

_MATERIALIZED_KEYS = {
    "EModel",
    "ExtractionTargetsConfiguration",
    "EModelPipelineSettings",
    "EModelConfiguration",
    "FitnessCalculatorConfiguration",
}


def materialize_me_model_config(
    dataset: dict, staging_dir: Path, forge, output_file: Path | None = None
):
    """Materialize an MEModelConfig."""
    res = deepcopy(dataset)
    res["defaults"] = _materialize_defaults(
        dataset=res["defaults"],
        staging_dir=staging_dir,
        forge=forge,
        output_dir=staging_dir if output_file else None,
    )
    res["overrides"] = _materialize_overrides(
        dataset=res.get("overrides", {}),
        staging_dir=staging_dir,
    )
    if output_file:
        write_json(filepath=output_file, data=res)

    return res


def _materialize_defaults(dataset: dict, staging_dir: Path, forge, output_dir: Path | None = None):
    variant_to_materializer = {
        "neurons_me_model": materialize_placeholder_emodel_config,
    }
    output_files = {
        variant: (
            output_dir / f"materialized_placeholder_emodel_config__{variant}.json"
            if output_dir
            else None
        )
        for variant in variant_to_materializer
    }
    return {
        variant: variant_to_materializer[variant](
            dataset=read_json_file_from_resource_id(forge, get_entry_id(entry)),
            staging_dir=staging_dir,
            labels_only=False,
            output_file=output_files[variant],
        )
        for variant, entry in dataset.items()
    }


def _materialize_overrides(dataset, staging_dir):
    strategies = {"neurons_me_model": {"assignOne": _materialize_one}}

    res = deepcopy(dataset)
    for variant, variant_data in dataset.items():
        for region_id, region_data in variant_data.items():
            for mtype_id, mtype_data in region_data.items():
                for etype_id, etype_data in mtype_data.items():
                    algorithm = etype_data["assignmentAlgorithm"]
                    result = strategies[variant][algorithm](
                        dataset=etype_data,
                        staging_dir=staging_dir,
                    )
                    res[variant][region_id][mtype_id][etype_id] = result
    return res


def materialize_placeholder_emodel_config(
    dataset: dict, staging_dir: Path, labels_only=False, output_file: Path | None = None
):
    """Materialize a PlaceholderEModelConfig."""
    levels = (
        _get_existing_region_notation,
        _get_existing_label,
        _get_existing_label,
        partial(_materialize_emodel, output_dir=staging_dir),
    )
    result = transform_nested_dataset(dataset, levels)

    if labels_only:
        result = _convert_to_labels(result, leaf_func=lambda e: list(e["hasPart"].values())[0])

    if output_file:
        write_json(filepath=output_file, data=result)

    return result


def _get_existing_region_notation(_, entry_data):
    return {"notation": entry_data["notation"]}


def _get_existing_label(_, entry_data):
    return {"label": entry_data["label"]}


@transform_cached
def _materialize_emodel(entry_id, _, output_dir):
    """Materialize an EModel entity."""
    emodel = EModel.from_id(entry_id, cross_bucket=True)
    emodel_path = _get_json_distribution(emodel.distribution).get_location_path()
    emodel_dict = {"EModel": str(emodel_path)}

    # we also need the configurations from the attached EModelWofklow
    try:
        workflow_dict = _materialize_emodel_workflow(
            dataset=emodel.generation.activity.followedWorkflow.distribution.as_dict(),
            staging_dir=Path(output_dir),
        )
    except Exception as e:
        raise CWLWorkflowError(f"EModel {entry_id} is incomplete.") from e

    result = {**emodel_dict, **workflow_dict}

    if not _MATERIALIZED_KEYS.issubset(result):
        raise CWLWorkflowError(
            f"Expected materialized EModel keys: {sorted(_MATERIALIZED_KEYS)}. "
            f"Got: {sorted(result)}.\n"
            f"EModel id: {entry_id}"
        )

    return result


def _materialize_emodel_workflow(dataset, staging_dir):
    targets_configuration_id = dataset["targets_configuration_id"]
    assert targets_configuration_id is not None, (dataset, targets_configuration_id)

    pipeline_settings_id = dataset["pipeline_settings_id"]
    assert pipeline_settings_id is not None, (dataset, pipeline_settings_id)

    fitness_configuration_id = dataset["fitness_configuration_id"]
    assert fitness_configuration_id is not None, (dataset, fitness_configuration_id)

    emodel_configuration_id = dataset["emodel_configuration_id"]
    assert emodel_configuration_id is not None, (dataset, emodel_configuration_id)

    return {
        "ExtractionTargetsConfiguration": _get_location_path(targets_configuration_id),
        "EModelPipelineSettings": _get_location_path(pipeline_settings_id),
        "FitnessCalculatorConfiguration": _get_location_path(fitness_configuration_id),
        "EModelConfiguration": _materialize_emodel_configuration(
            emodel_configuration_id, staging_dir
        ),
    }


def _get_location_path(entity_id):
    entity = Entity.from_id(entity_id, cross_bucket=True)
    assert entity is not None, entity_id
    return entity.distribution.get_location_path()


def _materialize_emodel_configuration(entity_id, output_dir):
    """Materialize the morphology id in the EModelConfiguration."""
    entity = Entity.from_id(entity_id, cross_bucket=True)
    dataset = entity.distribution.as_dict()

    # materialize morphology path
    dataset["morphology"]["path"] = _get_location_path(dataset["morphology"]["id"])
    del dataset["morphology"]["id"]

    # materialize the mod files
    for i, mechanism_dict in enumerate(dataset["mechanisms"]):
        mechanism_id = mechanism_dict["id"]
        if mechanism_id is None:
            path = None
        else:
            path = _get_location_path(dataset["mechanisms"][i]["id"])
        dataset["mechanisms"][i]["path"] = path
        del dataset["mechanisms"][i]["id"]

    out_path = Path(output_dir, Path(entity.distribution.get_location_path()).name)
    write_json(data=dataset, filepath=out_path)
    return out_path


def _get_json_distribution(distributions):
    """Get the json content from the list of distributions."""
    for d in distributions:
        if d.encodingFormat == "application/json":
            return d
    raise TypeError("No json distribution.")


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


def _materialize_one(dataset, staging_dir):
    res = deepcopy(dataset)
    res["eModel"] = _materialize_emodel(
        get_entry_id(dataset["eModel"]), None, output_dir=staging_dir
    )
    return res
