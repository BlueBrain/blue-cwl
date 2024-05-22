# SPDX-License-Identifier: Apache-2.0

"""Parsing module for cwl files."""

import logging
from copy import deepcopy
from pathlib import Path
from typing import Any

from blue_cwl.core import config, cwl
from blue_cwl.core.exceptions import CWLError
from blue_cwl.core.executor import LocalExecutor, SallocExecutor
from blue_cwl.core.types import PathLike
from blue_cwl.core.utils import load_yaml, resolve_path
from blue_cwl.core.validate import validate_workflow

L = logging.getLogger(__name__)


def parse_cwl_file(cwl_file: PathLike):
    """Parse a cwl file and return a CommandLinetTool or Workflow object.

    Returns:
        The cwl object corresponding to the file.
    """
    filepath = Path(cwl_file).resolve()

    raw = load_yaml(cwl_file)

    return parse_cwl_data(data=raw, base_dir=filepath.parent)


def parse_cwl_data(data: dict, base_dir: Path):
    """Parse a cwl dict and return a CommandLineTool or Workflow object."""
    if "class" not in data:
        raise CWLError("Missing 'class' in cwl data.")

    class_definition = data["class"]

    L.debug("Class definition found in cwl data: %s", class_definition)

    if class_definition == "Workflow":
        return _build_workflow(data, base_dir=base_dir)

    if class_definition == "CommandLineTool":
        return _build_command_line_tool(data, base_dir=base_dir)

    raise TypeError(
        f"Unknown class {class_definition}.\n" "Supported classes: [Workflow, CommandLineTool]"
    )


def parse_config_file(cwl_file: PathLike) -> cwl.Config:
    """Parse a cwl config file into a dictionary.

    Returns:
        The loaded dictionary with default values where allowed.
    """
    filepath = Path(cwl_file).resolve()

    raw = load_yaml(cwl_file)

    data = _resolve_paths(raw=raw, base_dir=filepath.parent)

    inputs = {
        input_name: _build_config_input(input_data)
        for input_name, input_data in data["inputs"].items()
    }
    return cwl.Config(inputs=inputs)


def _preprocess_types(document):
    """Type transformations.

    list of transformations:
        1. Type <T> ending with ? should be transformed to [<T>, "null"].
        2. Type <T> ending with [] should be transformed to {"type": "array", "items": <T>}

    See: https://www.commonwl.org/v1.2/Workflow.html#Document_preprocessing
    """
    if isinstance(document, list):
        return [_preprocess_types(d) for d in document]

    if isinstance(document, dict):
        res = {}
        for k, v in document.items():
            if k == "type":
                res[k] = _preprocess_type(v)
            else:
                res[k] = _preprocess_types(v)
        return res

    return document


def _preprocess_type(type_string: str | dict) -> str | list | dict:
    if isinstance(type_string, dict):
        return type_string

    if type_string.endswith("[]"):
        return {"type": "array", "items": type_string[:-2]}

    if type_string.endswith("?"):
        return [type_string[:-1], None]

    return type_string


def _build_config_input(input_data: dict):
    if isinstance(input_data, str):
        input_type = "string"
        input_value = input_data
    else:
        input_type = input_data["class"]
        if input_type == "NexusType" and "resource-id" in input_data:
            input_value = input_data["resource-id"]
        else:
            input_value = input_data["path"]

    return cwl.ConfigInput(type=input_type, value=input_value)


def _build_workflow(data: dict, base_dir: Path) -> cwl.Workflow:
    document = _preprocess_workflow_document(data, base_dir)
    workflow = cwl.Workflow.from_dict(document)
    validate_workflow(workflow)
    return workflow


def _preprocess_workflow_document(document, base_dir):
    document = deepcopy(document)
    document.pop("class")

    # transform types
    document = _preprocess_types(document)

    # resolve relative paths
    document = _resolve_paths(document, base_dir=base_dir)

    # transform list to map
    document["inputs"] = _preprocess_io(document["inputs"])

    # transform list to map
    document["outputs"] = _preprocess_io(document["outputs"])

    # transform map to list
    document["steps"] = _preprocess_workflow_steps(document["steps"], base_dir)

    return document


def _preprocess_workflow_steps(steps, base_dir):
    steps = deepcopy(steps)

    step_list = _dict_to_list_entries(steps)

    for step in step_list:
        # in -> inputs
        if "in" in step:
            step["inputs"] = _preprocess_step_inputs(step.pop("in"))

        # out -> outputs
        if "out" in step:
            step["outputs"] = step.pop("out")

        # build CommandLineTool
        if "run" in step:
            step["run"] = _build_workflow_run(step["run"], base_dir)

    return step_list


def _preprocess_step_inputs(data):
    processed = {}
    for k, v in data.items():
        if isinstance(v, str):
            processed[k] = {"id": k, "source": v}
        else:
            processed[k] = {"id": k} | v

    return processed


def _dict_to_list_entries(dataset):
    if isinstance(dataset, dict):
        return [{"id": name} | entry for name, entry in dataset.items()]

    return dataset


def _build_workflow_run(data_or_file, base_dir):
    if isinstance(data_or_file, dict):
        return parse_cwl_data(data_or_file, base_dir)

    return parse_cwl_file(data_or_file)


def _build_command_line_tool(data: dict, base_dir: Path) -> cwl.CommandLineTool:
    """Parse a cwl command line tool file into a dictionary.

    Returns:
        The loaded dictionary with default values where allowed.
    """
    data = _preprocess_command_line_tool_document(data, base_dir)
    return cwl.CommandLineTool.from_dict(data)


def _preprocess_command_line_tool_document(data, base_dir):
    data["baseCommand"] = _parse_baseCommand(data["baseCommand"], base_dir=base_dir)

    data = _resolve_paths(raw=data, base_dir=base_dir)
    data = _preprocess_types(data)
    data["inputs"] = _preprocess_io(data["inputs"])
    data["outputs"] = _preprocess_io(data["outputs"])
    data["executor"] = _build_executor(data)
    data.pop("class")

    return data


def _preprocess_io(data):
    if isinstance(data, list):
        return {entry["id"]: entry for entry in data}
    return {k: {"id": k} | v for k, v in data.items()}


def _resolve_paths(raw: dict[str, Any], base_dir: Path) -> dict[str, Any]:
    """Return a copy of raw data, with paths resolves wrt base_dir."""

    def recursive_resolve(entry):
        if isinstance(entry, list):
            for v in entry:
                recursive_resolve(v)
        elif isinstance(entry, dict):
            for k, v in entry.items():
                if k in {"run", "path"}:
                    if not isinstance(v, dict):
                        entry[k] = str(resolve_path(v, base_dir))
                else:
                    recursive_resolve(v)

    data = deepcopy(raw)
    recursive_resolve(data)
    return data


def _parse_io_parameters(data: list[dict[str, Any]] | dict[str, Any]) -> dict[str, Any]:
    """Return inputs or outputs in dictionary format."""
    if isinstance(data, list):
        return {entry["id"]: {k: v for k, v in entry.items() if k != "id"} for entry in data}

    return data


def _parse_baseCommand(raw: str | list[str], base_dir: Path) -> list[str]:
    base_command = raw

    if isinstance(base_command, str):
        base_command = base_command.split()

    if not isinstance(base_command, list):
        raise CWLError(
            f"Unknown format type '{type(base_command).__name__}' for baseCommand. "
            "Expected either str or list."
        )

    # resolve local executables wrt cwl_path directory
    for i, token in enumerate(base_command):
        if token.startswith("./"):
            base_command[i] = str(resolve_path(token, base_dir))

    return base_command


def _build_executor(data: dict) -> LocalExecutor | SallocExecutor:
    data = data.get("executor", None)

    if data is None:
        return LocalExecutor()

    executor_type = data["type"]
    env_vars = data.get("env_vars", None)

    if executor_type == "local":
        return LocalExecutor(env_vars=env_vars)

    if executor_type == "slurm":
        remote_config = config.RemoteConfig.from_dict(data["remote_config"])
        slurm_config = config.SlurmConfig.from_dict(data["slurm_config"])

        return SallocExecutor(
            remote_config=remote_config,
            slurm_config=slurm_config,
            env_vars=env_vars,
        )

    raise TypeError(f"Unknown executor type {executor_type}")
