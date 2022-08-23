"""Utilities."""
from pathlib import Path
from typing import Any, Dict, Union

import yaml


def load_yaml(filepath: Union[str, Path]) -> Dict[Any, Any]:
    """Load from YAML file."""
    with open(filepath, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def rename_dict_inplace(data: Dict[str, str], old: str, new: str):
    """Rename a key in data in-place."""
    data[new] = data[old]
    del data[old]
