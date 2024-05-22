# SPDX-License-Identifier: Apache-2.0

"""Staging module."""

from copy import deepcopy
from functools import partial
from pathlib import Path
from typing import Any

from entity_management.emodel import EModel

from blue_cwl.exceptions import CWLWorkflowError
from blue_cwl.nexus import download_distribution, get_distribution, get_distribution_as_dict
from blue_cwl.staging import get_entry_id, transform_cached, transform_nested_dataset
from blue_cwl.typing import StrOrPath
from blue_cwl.utils import create_dir, get_obj, load_json, url_without_revision, write_json

OPTIONAL_WORKFLOW_DATASETS = {
    "ExtractionTargetsConfiguration": {
        "id": "targets_configuration_id",
        "path": "targets_configuration_path",
    },
    "EModelPipelineSettings": {
        "id": "pipeline_settings_id",
        "path": "pipeline_settings_path",
    },
    "FitnessCalculatorConfiguration": {
        "id": "fitness_configuration_id",
        "path": "fitness_configuration_path",
    },
}

MANDATORY_WORKFLOW_DATASETS = {
    "EModelConfiguration": {
        "id": "emodel_configuration_id",
        "path": "emodel_configuration_path",
    },
    "EModelScript": {
        "id": "emodel_scripts_id",
        "path": "emodel_scripts_path",
    },
}


def stage_me_model_config(
    dataset: dict,
    staging_dir: Path,
    *,
    output_file: Path | None = None,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
):
    """Materialize an MEModelConfig."""
    res = deepcopy(dataset)
    res["defaults"] = _materialize_defaults(
        dataset=res["defaults"],
        staging_dir=staging_dir,
        output_dir=staging_dir if output_file else None,
        base=base,
        org=org,
        proj=proj,
        token=token,
    )
    res["overrides"] = _materialize_overrides(
        dataset=res.get("overrides", {}),
        staging_dir=staging_dir,
        base=base,
        org=org,
        proj=proj,
        token=token,
    )
    if output_file:
        write_json(filepath=output_file, data=res)

    return res


def _materialize_defaults(
    dataset: dict,
    staging_dir: Path,
    *,
    output_dir: Path | None = None,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
):
    variant_to_materializer = {
        "neurons_me_model": stage_placeholder_emodel_config,
    }

    result = {}
    for variant, entry in dataset.items():
        entry_id = get_entry_id(entry)
        stage_func = variant_to_materializer[variant]
        dataset = get_distribution_as_dict(entry_id, base=base, org=org, proj=proj, token=token)

        if output_dir:
            output_file = output_dir / f"materialized_placeholder_emodel_config__{variant}.json"
        else:
            output_file = None

        result[variant] = stage_func(
            dataset=dataset,
            staging_dir=staging_dir,
            output_file=output_file,
            base=base,
            org=org,
            proj=proj,
            token=token,
        )

    return result


def _materialize_overrides(dataset, staging_dir, base, org, proj, token):
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
                        base=base,
                        org=org,
                        proj=proj,
                        token=token,
                    )
                    res[variant][region_id][mtype_id][etype_id] = result
    return res


def stage_placeholder_emodel_config(
    dataset: dict,
    *,
    staging_dir: Path,
    output_file: Path | None = None,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
):
    """Materialize a PlaceholderEModelConfig."""
    levels = (
        _get_existing_region_notation,
        _get_existing_label,
        _get_existing_label,
        partial(
            _stage_emodel_entry,
            staging_dir=staging_dir,
            base=base,
            org=org,
            proj=proj,
            token=token,
        ),
    )
    result = transform_nested_dataset(dataset, levels)

    if output_file:
        write_json(filepath=output_file, data=result)

    return result


def _get_existing_region_notation(_, entry_data):
    return {"notation": entry_data["notation"]}


def _get_existing_label(_, entry_data):
    return {"label": entry_data["label"]}


@transform_cached
def _stage_emodel_entry(entry_id, _, staging_dir, base, org, proj, token):
    # use the id from the url as a unique identifier for the directory
    output_dir = create_dir(staging_dir / _emodel_identifier(entry_id))

    dataset = stage_emodel(
        entry_id,
        staging_dir=output_dir,
        base=base,
        org=org,
        proj=proj,
        token=token,
    )
    return dataset


def stage_emodel(
    obj,
    *,
    staging_dir: StrOrPath,
    output_file: StrOrPath | None = None,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
):
    """Stage an EModel entity."""
    staging_dir = Path(staging_dir)

    emodel = get_obj(obj, cls=EModel, base=base, org=org, proj=proj, token=token)

    # stage EModel distribution with parameters and scores. There are no ids inside.
    emodel_path = download_distribution(
        emodel,
        output_dir=staging_dir,
        filename="EModel.json",
        encoding_format="application/json",
        base=base,
        org=org,
        proj=proj,
        token=token,
    )

    # stage all available configurations from the attached EModelWofklow
    try:
        workflow_dict = stage_emodel_workflow(
            emodel.generation.activity.followedWorkflow,
            staging_dir=staging_dir,
            base=base,
            org=org,
            proj=proj,
            token=token,
        )
    except Exception as e:
        raise CWLWorkflowError(f"EModel {emodel.get_id()} is incomplete.") from e

    morph_path = load_json(workflow_dict["emodel_configuration_path"])["morphology"]["path"]

    dataset = {
        "morphology": morph_path,
        "params": {
            "values": emodel_path,
            "bounds": workflow_dict["emodel_configuration_path"],
        },
        "features": workflow_dict.get("fitness_configuration_path", None),
        "pipeline_settings": workflow_dict.get("pipeline_settings_path", None),
    }

    if output_file:
        write_json(data=dataset, filepath=output_file)

    return dataset


def stage_emodel_workflow(
    obj,
    *,
    staging_dir: StrOrPath,
    output_file: str | None = None,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
):
    """Stage an emodel workflow."""
    staging_dir = Path(staging_dir)

    emodel_workflow = get_obj(obj, base=base, org=org, proj=proj, token=token)
    distribution = get_distribution(emodel_workflow, encoding_format="application/json")
    dataset = distribution.as_dict(use_auth=token)

    staged_dataset = {}

    staged_optional_datasets = _stage_optional_datasets(
        dataset=dataset,
        staging_dir=staging_dir,
        base=base,
        org=org,
        proj=proj,
        token=token,
    )
    staged_dataset.update(staged_optional_datasets)

    staged_mandatory_datasets = _stage_mandatory_datasets(
        dataset=dataset,
        staging_dir=staging_dir,
        base=base,
        org=org,
        proj=proj,
        token=token,
    )
    staged_dataset.update(staged_mandatory_datasets)

    if output_file:
        write_json(data=staged_dataset, filepath=output_file)

    return staged_dataset


def _stage_optional_datasets(dataset: dict, staging_dir, base, org, proj, token):
    """Stage optional EModelWorkflow datasets."""
    result = {}
    for name, entry in OPTIONAL_WORKFLOW_DATASETS.items():
        source_key, target_key = entry["id"], entry["path"]

        if dataset_id := dataset[source_key]:
            filename = f"{name}.json"
            result[target_key] = download_distribution(
                dataset_id,
                output_dir=staging_dir,
                filename=filename,
                encoding_format="application/json",
                base=base,
                org=org,
                proj=proj,
                token=token,
            )
    return result


def _stage_mandatory_datasets(dataset: dict, staging_dir, base, org, proj, token):
    result: dict[str, Any] = {}

    # stage mandatory hoc file
    emodel_script_ids = dataset["emodel_scripts_id"]

    if len(emodel_script_ids) != 1:
        raise CWLWorkflowError("More than one EModelScript entities found.")

    emodel_script_path = staging_dir / "model.hoc"
    result["emodel_scripts_path"] = [
        download_distribution(
            emodel_script_ids[0],
            output_dir=staging_dir,
            filename=emodel_script_path.name,
            encoding_format="application/hoc",
            base=base,
            org=org,
            proj=proj,
            token=token,
        )
    ]

    # stage mandatory EModelConfiguration
    emodel_configuration_id = dataset["emodel_configuration_id"]

    if emodel_configuration_id is None:
        raise CWLWorkflowError(f"No emodel_configuration_id in dataset: {dataset}")

    emodel_configuration_path = staging_dir / "EModelConfiguration.json"
    stage_emodel_configuration(
        emodel_configuration_id,
        staging_dir=staging_dir,
        output_file=emodel_configuration_path,
        base=base,
        org=org,
        proj=proj,
        token=token,
    )
    result["emodel_configuration_path"] = str(emodel_configuration_path)

    return result


def stage_emodel_configuration(
    entity_id,
    *,
    staging_dir: StrOrPath,
    output_file: StrOrPath | None = None,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
):
    """Materialize the morphology id in the EModelConfiguration."""
    entity = get_obj(entity_id, base=base, org=org, proj=proj, token=token)
    distribution = get_distribution(
        entity, encoding_format="application/json", base=base, org=org, proj=proj, token=token
    )

    dataset = distribution.as_dict(use_auth=token)
    staged_dataset = deepcopy(dataset)

    mechanisms_dir = create_dir(Path(staging_dir, "mechanisms"))
    morphologies_dir = create_dir(Path(staging_dir, "morphology"))

    # stage swc morphology path
    staged_dataset["morphology"]["path"] = download_distribution(
        dataset["morphology"]["id"],
        output_dir=morphologies_dir,
        encoding_format="application/swc",
        base=base,
        org=org,
        proj=proj,
        token=token,
    )
    del staged_dataset["morphology"]["id"]

    # stage mod file path
    staged_dataset["mechanisms"] = _stage_emodel_mechanisms(
        dataset["mechanisms"],
        staging_dir=mechanisms_dir,
        base=base,
        org=org,
        proj=proj,
        token=token,
    )

    if output_file:
        write_json(data=staged_dataset, filepath=output_file)

    return staged_dataset


def _stage_emodel_mechanisms(
    dataset: dict,
    staging_dir: Path,
    base: str | None,
    org: str | None,
    proj: str | None,
    token: str | None,
):
    """Stage mod files from the dataset to the local staging_dir."""
    result = deepcopy(dataset)
    for i, mechanism_dict in enumerate(dataset):
        if mechanism_id := mechanism_dict["id"]:
            path = download_distribution(
                mechanism_id,
                output_dir=staging_dir,
                base=base,
                org=org,
                proj=proj,
                token=token,
            )
            del result[i]["id"]
        else:
            path = None
        result[i]["path"] = path
    return result


def _emodel_identifier(emodel_id: str) -> str:
    """Convert the emodel_id to an identifier.

    The following actions are performed:
        - Revision is removed
        - The uuid part of the url is extracted
        - Dashes are removed
        - emodel_ is prefixed

    Example:
        Before: https://foo/bar/1234-5678?rev=1
        After : emodel_12345678
    """
    uuid = url_without_revision(emodel_id).split("/")[-1]
    uuid_without_dashes = uuid.replace("-", "")
    return f"emodel_{uuid_without_dashes}"


def _materialize_one(dataset: dict, staging_dir: Path, base, org, proj, token) -> dict:
    res = deepcopy(dataset)
    emodel_id = get_entry_id(dataset["eModel"])
    res["eModel"] = _stage_emodel_entry(
        emodel_id, None, staging_dir=staging_dir, base=base, org=org, proj=proj, token=token
    )
    return res
