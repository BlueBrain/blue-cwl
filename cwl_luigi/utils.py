"""Utilities."""
import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml


@contextmanager
def cwd(path):
    """Context manager to temporarily change the working directory."""
    original_cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(original_cwd)


def load_yaml(filepath: Union[str, Path]) -> Dict[Any, Any]:
    """Load from YAML file."""
    with open(filepath, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_json(filepath: Union[str, Path]) -> Dict[Any, Any]:
    """Load from JSON file."""
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def resolve_path(path: Path, base_dir: Optional[Path] = None):
    """Resolve path if it's relative wrt base_dir if given."""
    if base_dir is not None:
        return Path(base_dir, path).resolve()

    return path.resolve()
