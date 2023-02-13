"""Validation functions."""
import libsonata

from cwl_registry import utils
from cwl_registry.exceptions import CWLRegistryError


def check_population_name_consistent_with_region(population_name, region_acronym):
    """Raise if the region name is not part of the population name."""
    if not population_name.startswith(region_acronym):
        raise CWLRegistryError(
            f"Population name '{population_name}' is not consistent with region '{region_acronym}'."
        )


def check_population_name_in_config(population_name, config_file):
    """Raise if the population name is not present in the sonata config file."""
    config = utils.load_json(config_file)

    nodes = config["networks"]["nodes"]

    found = False
    for node in nodes:
        populations = node["populations"]

        if population_name in populations:
            found = True
            break

    if not found:
        raise CWLRegistryError(
            f"Population name '{population_name}' not found in config {config_file}.\n"
        )


def check_population_name_in_nodes(population_name, nodes_file):
    """Raise if population name not in nodes file."""
    nodes = libsonata.NodeStorage(nodes_file)

    available_names = nodes.population_names

    if population_name not in available_names:
        raise CWLRegistryError(
            f"Population name '{population_name}' not found in nodes file {nodes_file}"
        )
