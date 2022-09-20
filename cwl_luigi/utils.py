"""Utilities."""
import importlib.resources
import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

import jsonschema
import yaml

from cwl_luigi.exceptions import CWLError
from cwl_luigi.types import PathLike


@contextmanager
def cwd(path):
    """Context manager to temporarily change the working directory."""
    original_cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(original_cwd)


def load_yaml(filepath: PathLike) -> Dict[Any, Any]:
    """Load from YAML file."""
    with open(filepath, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_json(filepath: PathLike) -> Dict[Any, Any]:
    """Load from JSON file."""
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


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
