"""Workflow wrappers."""

import logging
import os

import click

from blue_cwl import __version__ as VERSION
from blue_cwl.wrappers import (
    cell_composition_manipulation,
    cell_composition_summary,
    connectome_filtering_synapses,
    connectome_generation_placeholder,
    density_calculation,
    memodel,
    mmodel,
    neurons_cell_position,
)


@click.group("blue-cwl", help=__doc__.format(esc="\b"))
@click.version_option(version=VERSION)
@click.option("-v", "--verbose", count=True, default=1, help="-v for INFO, -vv for DEBUG")
def main(verbose):
    """CWL Registry execution tools."""
    existing_handlers = logging.getLogger().handlers

    if existing_handlers:
        logging.warning(
            "A basicConfig has been set at import time. This is an antipattern and needs to be "
            "addressed by the respective package as it overrides this cli's configuration."
        )

    # Allow overriding the logging with the DEBUG env var.
    # This is particularly useful for bbp-workflow because it allows changing the logging level
    # without changing the generator definition of the wrapper.
    if os.getenv("DEBUG", "False").lower() == "true":
        level = logging.DEBUG
    else:
        level = (logging.WARNING, logging.INFO, logging.DEBUG)[min(verbose, 2)]

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@main.group()
def execute():
    """Subcommand grouping together all execution wrappers."""


execute.add_command(name="neurons-cell-position", cmd=neurons_cell_position.app)
execute.add_command(name="density-calculation", cmd=density_calculation.app)
execute.add_command(name="cell-composition-summary", cmd=cell_composition_summary.app)
execute.add_command(name="cell-composition-manipulation", cmd=cell_composition_manipulation.app)
execute.add_command(name="mmodel-neurons", cmd=mmodel.app)
execute.add_command(
    name="connectome-generation-placeholder",
    cmd=connectome_generation_placeholder.app,
)
execute.add_command(name="connectome-filtering-synapses", cmd=connectome_filtering_synapses.app)
execute.add_command(cmd=memodel.app)

if __name__ == "__main__":
    main(verbose=1)
