"""Workflow wrappers."""
import logging

import click

from cwl_registry.app import (
    cell_composition_summary,
    density_calculation,
    density_manipulation,
    me_type_property,
    placeholder_emodel_assignment,
    placeholder_morphology_assignment,
)
from cwl_registry.version import VERSION


@click.group("cwl-registry", help=__doc__.format(esc="\b"))
@click.version_option(version=VERSION)
@click.option("-v", "--verbose", count=True, default=0, help="-v for INFO, -vv for DEBUG")
def main(verbose):
    """CWL Registry execution tools."""
    level = (logging.WARNING, logging.INFO, logging.DEBUG)[min(verbose, 2)]
    logging.basicConfig(level=level)


@main.group()
def execute():
    """Subcommand grouping together all execution wrappers."""


execute.add_command(name="neurons-me-type-property", cmd=me_type_property.app)
execute.add_command(name="density-calculation", cmd=density_calculation.app)
execute.add_command(name="density-manipulation", cmd=density_manipulation.app)
execute.add_command(name="cell-composition-summary", cmd=cell_composition_summary.app)
execute.add_command(
    name="placeholder-morphology-assignment", cmd=placeholder_morphology_assignment.app
)
execute.add_command(name="placeholder-emodel-assignment", cmd=placeholder_emodel_assignment.app)


if __name__ == "__main__":
    main(verbose=2)
