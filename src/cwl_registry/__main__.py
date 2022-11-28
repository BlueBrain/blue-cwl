"""Workflow wrappers."""
import logging

import click

from cwl_registry.app import (
    me_type_property,
    placeholder_emodel_assignment,
    placeholder_morphology_assignment,
)
from cwl_registry.version import VERSION


@click.group("cwl-workflow", help=__doc__.format(esc="\b"))
@click.version_option(version=VERSION)
@click.option("-v", "--verbose", count=True, default=0, help="-v for INFO, -vv for DEBUG")
def main(verbose):
    """Workflow wrappers."""
    level = (logging.WARNING, logging.INFO, logging.DEBUG)[min(verbose, 2)]
    logging.basicConfig(level=level)


main.add_command(name="me-type-property", cmd=me_type_property.app)
main.add_command(
    name="placeholder-morphology-assignment", cmd=placeholder_morphology_assignment.app
)
main.add_command(name="placeholder-emodel-assignment", cmd=placeholder_emodel_assignment.app)


if __name__ == "__main__":
    main(verbose=2)
