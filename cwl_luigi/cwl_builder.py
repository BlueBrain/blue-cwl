"""CWL files builder from UI recipe."""
import logging
from typing import Any, Dict, Tuple

import cwl_registry

from cwl_luigi.utils import load_yaml

L = logging.getLogger(__name__)


def build_workflow_cwl_datasets(
    forge,
    recipe_dict: dict,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """Convert the recipe dict into a cwl workflow and a cwl config."""
    config: Dict[str, Any] = {"inputs": {}}

    workflow: Dict[str, Any] = {
        "cwlVersion": "v1.2",
        "class": "Workflow",
        "id": "Workflow",
        "inputs": {},
        "outputs": dict(recipe_dict["outputs"]),
        "steps": [],
    }

    for name, data in recipe_dict["inputs"].items():

        # we assume everything is a nexus entity
        assert data["type"] == "NexusType"
        config["inputs"][name] = {"class": "NexusType", "resource-id": data["resource-id"]}
        workflow["inputs"][name] = {"type": "NexusType"}

    allocation_configs = {}

    for recipe_step in recipe_dict["steps"]:

        # TODO: Accessing the registry here can be out of sync with the resource of the variant
        # that is passed if the latter is updated directly on KG.
        variant = _get_variant(forge, recipe_step["run"], config["inputs"])

        step_id = recipe_step["id"]
        allocation_configs[step_id] = _get_allocation_config(variant)

        # nexus configuration parameters are passed to each step from the global inputs
        # and resolved by the workflow task building.
        step_entry = {
            "id": step_id,
            "in": recipe_step["in"],
            "out": recipe_step["out"],
            "run": variant.execute_definition_file,
        }
        workflow["steps"].append(step_entry)

    return workflow, config, allocation_configs


def _get_variant(forge, variant_entry, inputs):

    if isinstance(variant_entry, dict):
        variant = cwl_registry.Variant.from_registry(
            generator_name=variant_entry["generator_name"],
            variant_name=variant_entry["variant_name"],
            version=variant_entry["version"],
        )
        L.info(
            "Variant retrieved from registry (%s, %s, %s)",
            variant.generator_name,
            variant.name,
            variant.version,
        )
    else:
        variant_resource_id = inputs[variant_entry]["resource-id"]
        variant = cwl_registry.Variant.from_resource_id(forge, variant_resource_id)
        L.info(
            "Variant retrieved from resource (%s, %s, %s)",
            variant.generator_name,
            variant.name,
            variant.version,
        )

    return variant


def _get_allocation_config(variant):
    try:
        return load_yaml(variant.get_resources_file("variant_config.yml"))
    except KeyError:
        return None
