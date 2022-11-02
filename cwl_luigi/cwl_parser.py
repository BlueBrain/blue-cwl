"""Parsing module for cwl files."""
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Union

from cwl_luigi.exceptions import CWLError
from cwl_luigi.types import PathLike
from cwl_luigi.utils import load_yaml, resolve_path, validate_schema


def parse_config_file(cwl_file: PathLike) -> Dict[str, Any]:
    """Parse a cwl config file into a dictionary.

    Returns:
        The loaded dictionary with default values where allowed.
    """
    filepath = Path(cwl_file).resolve()

    raw = load_yaml(cwl_file)

    return _resolve_paths(raw=raw, base_dir=filepath.parent)


def parse_workflow_file(cwl_file: PathLike) -> Dict[str, Any]:
    """Parse a cwl workflow file into a dictionary.

    Returns:
        The loaded dictionary with default values where allowed.
    """
    filepath = Path(cwl_file).resolve()

    data = load_yaml(cwl_file)

    validate_schema(data, "workflow.yml")

    data["inputs"] = _parse_io_parameters(data["inputs"])
    data["outputs"] = _parse_io_parameters(data["outputs"])

    return _resolve_paths(raw=data, base_dir=filepath.parent)


def parse_command_line_tool_file(cwl_file: PathLike) -> Dict[str, Any]:
    """Parse a cwl command line tool file into a dictionary.

    Returns:
        The loaded dictionary with default values where allowed.
    """
    filepath = Path(cwl_file).resolve()

    data = load_yaml(cwl_file)

    validate_schema(data, "commandlinetool.yml")

    data["inputs"] = _parse_io_parameters(data["inputs"])
    data["outputs"] = _parse_io_parameters(data["outputs"])
    data["baseCommand"] = _parse_baseCommand(data["baseCommand"], base_dir=filepath.parent)

    if "environment" not in data:
        data["environment"] = None

    return _resolve_paths(raw=data, base_dir=filepath.parent)


def _resolve_paths(raw: Dict[str, Any], base_dir: Path) -> Dict[str, Any]:
    """Return a copy of raw data, with paths resolves wrt base_dir."""

    def recursive_resolve(entry):
        if isinstance(entry, list):
            for v in entry:
                recursive_resolve(v)
        elif isinstance(entry, dict):
            for k, v in entry.items():
                if k in {"run", "path"}:
                    entry[k] = str(resolve_path(v, base_dir))
                else:
                    recursive_resolve(v)

    data = deepcopy(raw)
    recursive_resolve(data)
    return data


def _parse_io_parameters(data: Union[List[Dict[str, Any]], Dict[str, Any]]) -> Dict[str, Any]:
    """Return inputs or outputs in dictionary format."""
    if isinstance(data, list):
        return {entry["id"]: {k: v for k, v in entry.items() if k != "id"} for entry in data}

    return data


def _parse_baseCommand(raw: Union[str, List[str]], base_dir: Path) -> List[str]:

    base_command = raw

    if isinstance(base_command, str):
        base_command = base_command.split()

    if not isinstance(base_command, list):
        raise CWLError(
            f"Unknown format type '{type(base_command).__name__}' for baseCommand. "
            "Expected either str or list."
        )

    # resolve local executables wrt cwl_path directory
    if base_command[0].startswith("./"):
        base_command[0] = str((base_dir / base_command[0]).resolve())

    return base_command
