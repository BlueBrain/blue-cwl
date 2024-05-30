"""Wrapper common utils."""

from pathlib import Path

from blue_cwl.typing import StrOrPath
from blue_cwl.utils import create_dir

_SUB_DIRECTORIES = ("build", "stage", "transform")


def setup_directories(output_dir: StrOrPath, sub_directories=_SUB_DIRECTORIES) -> dict[str, Path]:
    """Setup directory hierarchy for wrapper output."""
    create_dir(output_dir)
    return {dirname: create_dir(Path(output_dir, dirname)) for dirname in sub_directories}
