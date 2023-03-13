"""Workflow wrappers."""
import logging
import os

import click

from cwl_registry.version import VERSION
from cwl_registry.wrappers import (
    cell_composition_manipulation,
    cell_composition_summary,
    density_calculation,
    density_manipulation,
    neurons_cell_position,
    placeholder_emodel_assignment,
    placeholder_morphology_assignment,
)


@click.group("cwl-registry", help=__doc__.format(esc="\b"))
@click.version_option(version=VERSION)
@click.option("-v", "--verbose", count=True, default=0, help="-v for INFO, -vv for DEBUG")
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
execute.add_command(name="density-manipulation", cmd=density_manipulation.app)
execute.add_command(name="cell-composition-summary", cmd=cell_composition_summary.app)
execute.add_command(name="cell-composition-manipulation", cmd=cell_composition_manipulation.app)
execute.add_command(
    name="placeholder-morphology-assignment", cmd=placeholder_morphology_assignment.app
)
execute.add_command(name="placeholder-emodel-assignment", cmd=placeholder_emodel_assignment.app)


if __name__ == "__main__":
    main(verbose=1)
