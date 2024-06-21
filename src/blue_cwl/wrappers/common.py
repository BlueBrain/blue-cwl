"""Wrapper common utils."""

import logging
from pathlib import Path

import click
from entity_management.core import Entity

from blue_cwl import utils
from blue_cwl.exceptions import CWLWorkflowError
from blue_cwl.typing import StrOrPath

L = logging.getLogger(__name__)


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
    utils.create_dir(output_dir)
    return {dirname: utils.create_dir(Path(output_dir, dirname)) for dirname in sub_directories}


def write_entity_id_to_file(entity: Entity, output_file: StrOrPath) -> None:
    """Write entity id to json file."""
    entity_id = entity.get_id()
    entity_type = entity.__name__

    if entity_id is None:
        raise CWLWorkflowError(f"Entity '{entity_type}' has no id.")

    contents = {"@id": entity_id, "@type": entity_type}
    utils.write_json(data=contents, filepath=output_file)

    L.debug("Written registered entity:\nContents: %s\nLocation: %s", contents, output_file)
