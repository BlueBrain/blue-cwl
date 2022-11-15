"""Utilities."""
from pathlib import Path
from typing import Dict


def get_directory_contents(directory_path: Path) -> Dict[str, Path]:
    """Return the file in a dictionary if it exists, an empty dict otherwise."""
    if directory_path.is_dir():
        return {path.name: path for path in directory_path.iterdir()}
    return {}
