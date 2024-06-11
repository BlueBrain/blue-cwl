"""Wrapper common utils."""

from pathlib import Path

import click

from blue_cwl.typing import StrOrPath
from blue_cwl.utils import create_dir

_SUB_DIRECTORIES = ("build", "stage", "transform")


@click.group(name="common")
def app():
    """Morphology synthesis of neurons."""


@app.command(name="setup-directories")
@click.option("--output-dir", required=True, help="Output directory.")
def setup_directories_cli(**kwargs):
    """Setup directory hierarchy for wrapper output."""
    setup_directories(**kwargs)


def setup_directories(output_dir: StrOrPath, sub_directories=_SUB_DIRECTORIES) -> dict[str, Path]:
    """Setup directory hierarchy for wrapper output."""
    create_dir(output_dir)
    return {dirname: create_dir(Path(output_dir, dirname)) for dirname in sub_directories}
