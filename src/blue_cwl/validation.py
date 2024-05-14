"""Validation functions."""

from importlib import resources
from typing import Any

import jsonschema
import libsonata
import yaml

from blue_cwl.exceptions import CWLRegistryError, CWLWorkflowError, SchemaValidationError
from blue_cwl.typing import StrOrPath
from blue_cwl.utils import load_json


def check_population_name_consistent_with_region(population_name: str, region_acronym: str) -> None:
    """Raise if the region name is not part of the population name."""
    if not population_name.startswith(region_acronym):
        raise CWLRegistryError(
            f"Population name '{population_name}' is not consistent with region '{region_acronym}'."
        )


def check_population_name_in_config(population_name: str, config_file: StrOrPath) -> None:
    """Raise if the population name is not present in the sonata config file."""
    config = load_json(config_file)

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


def check_population_name_in_nodes(population_name: str, nodes_file: StrOrPath) -> None:
    """Raise if population name not in nodes file."""
    nodes = libsonata.NodeStorage(nodes_file)

    available_names = nodes.population_names

    if population_name not in available_names:
        raise CWLRegistryError(
            f"Population name '{population_name}' not found in nodes file {nodes_file}"
        )


def check_properties_in_population(
    population_name: str, nodes_file: StrOrPath, property_names: list[str]
) -> None:
    """Raise if properties not in population."""
    pop = libsonata.NodeStorage(nodes_file).open_population(population_name)

    pop_attributes = pop.attribute_names
    pop_dynamics_attributes = pop.dynamics_attribute_names

    dynamics_prefix = "dynamics_params/"

    def _property_exists(property_name):
        if property_name.startswith(dynamics_prefix):
            return property_name.removeprefix(dynamics_prefix) in pop_dynamics_attributes
        return property_name in pop_attributes

    not_existing = [name for name in property_names if not _property_exists(name)]

    if not_existing:
        raise CWLWorkflowError(
            f"{not_existing} are not contained in {population_name} in {nodes_file}."
        )


def validate_schema(data: dict[str, Any], schema_name: str) -> None:
    """Validata data against the schema with 'schema_name'."""
    schema = _read_schema(schema_name)

    cls = jsonschema.validators.validator_for(schema)
    cls.check_schema(schema)
    validator = cls(schema)
    errors = validator.iter_errors(data)

    messages: list[str] = []
    for error in errors:
        if error.context:
            messages.extend(map(_format_error, error.context))
        else:
            messages.append(_format_error(error))

    if messages:
        raise SchemaValidationError("\n".join(messages))


def _read_schema(schema_name: str) -> dict[str, Any]:
    """Load a schema and return the result as a dictionary."""
    resource = resources.files("blue_cwl") / "schemas" / schema_name
    content = resource.read_text()
    return yaml.safe_load(content)


def _format_error(error) -> str:
    paths = " -> ".join(map(str, error.absolute_path))
    return f"[{paths}]: {error.message}"
