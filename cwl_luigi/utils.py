"""Utilities."""
import functools
import importlib.resources
import inspect
import json
import logging
import operator
import os
import pathlib
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import jsonschema
import yaml

from cwl_luigi.exceptions import CWLError
from cwl_luigi.types import PathLike

L = logging.getLogger(__name__)


def log(function, logger=L):
    """Log the signature of a function.

    Note: Do not use for functions that receive large inputs as it may slow down runtime.
    """

    @functools.wraps(function)
    def wrapper(*args, **kwargs):

        signature = inspect.signature(function)

        params = [
            (k, v.default if v.default is not inspect.Parameter.empty else None)
            for k, v in signature.parameters.items()
        ]

        # create argument pairs
        arg_pairs = [(name, v) for (name, _), v in zip(params[: len(args)], args)]

        # use kwargs or defaults for the rest of the parameters
        arg_pairs.extend(
            (name, kwargs[name] if name in kwargs else default_value)
            for name, default_value in params[len(args) :]
        )

        str_v = "  " + "\n  ".join([f"{k} = {v!r}" for k, v in arg_pairs])

        str_function_repr = f" Name: {function.__name__}\n" f" Args: \n\n{str_v}\n\n"
        logger.debug("Executed function:\n%s\n", str_function_repr)

        return function(*args, **kwargs)

    return wrapper


@log
def create_dir(path: Path) -> Path:
    """Create directory and parents if it doesn't already exist."""
    path.mkdir(parents=True, exist_ok=True)
    return path


@contextmanager
def cwd(path):
    """Context manager to temporarily change the working directory."""
    original_cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(original_cwd)


@log
def load_yaml(filepath: PathLike) -> Dict[Any, Any]:
    """Load from YAML file."""
    with open(filepath, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@log
def write_yaml(filepath: PathLike, data: dict) -> None:
    """Writes dict data to yaml."""

    class Dumper(yaml.SafeDumper):
        """Custom dumper that adds an empty line between root level entries."""

        def write_line_break(self, data=None):
            super().write_line_break(data)

            if len(self.indents) == 1:
                super().write_line_break()

    def path_representer(dumper, path):
        return dumper.represent_scalar("tag:yaml.org,2002:str", str(path))

    Dumper.add_multi_representer(pathlib.PurePath, path_representer)

    with open(filepath, mode="w", encoding="utf-8") as out_file:
        yaml.dump(data, out_file, Dumper=Dumper, sort_keys=False, default_flow_style=False)


@log
def load_json(filepath: PathLike) -> Dict[Any, Any]:
    """Load from JSON file."""
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


@log
def write_json(filepath: PathLike, data: Dict[Any, Any]) -> None:
    """Write json file."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


@log
def resolve_path(path: PathLike, base_dir: Optional[PathLike] = None):
    """Resolve path if it's relative wrt base_dir if given."""
    if base_dir is not None:
        return Path(base_dir, path).resolve()

    return Path(path).resolve()


def validate_schema(data: Dict[str, Any], schema_name: str) -> None:
    """Validata data against the schema with 'schema_name'."""
    schema = _read_schema(schema_name)

    cls = jsonschema.validators.validator_for(schema)
    cls.check_schema(schema)
    validator = cls(schema)
    errors = validator.iter_errors(data)

    messages: List[str] = []
    for error in errors:
        if error.context:
            messages.extend(map(_format_error, error.context))
        else:
            messages.append(_format_error(error))

    if messages:
        raise CWLError("\n".join(messages))


def _read_schema(schema_name: str) -> Dict[str, Any]:
    """Load a schema and return the result as a dictionary."""
    resource = importlib.resources.files("cwl_luigi") / "schemas" / schema_name
    content = resource.read_text()
    return yaml.safe_load(content)


def _format_error(error):
    paths = " -> ".join(map(str, error.absolute_path))
    return f"[{paths}]: {error.message}"


def sorted_dict(unsorted_dict):
    """Return a copy with sorted keys."""
    return dict(sorted(unsorted_dict.items()))


def zip_mappings(*mappings: Mapping[str, Any]):
    """Zip mappings together."""
    for key in mappings[0].keys():
        yield key, tuple(map(operator.itemgetter(key), mappings))
