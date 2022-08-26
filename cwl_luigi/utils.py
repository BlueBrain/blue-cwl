"""Utilities."""
import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Union

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


def rename_dict_inplace(data: Dict[str, str], old: str, new: str):
    """Rename a key in data in-place."""
    data[new] = data[old]
    del data[old]
