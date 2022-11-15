"""CWL files builder from UI recipe."""
from typing import Any, Dict, Optional, Tuple

import cwl_registry

from cwl_luigi.allocation import AllocationBuilder


def build_workflow_cwl_datasets(
    recipe_dict: dict,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Optional[AllocationBuilder]]]:
    """Convert the recipe dict into a cwl workflow and a cwl config."""
    config: Dict[str, Any] = {"inputs": {}}
    workflow: Dict[str, Any] = {
        "cwlVersion": "v1.2",
        "class": "Workflow",
        "id": "Workflow",
        "inputs": {
            "kg_base": {"type": "string"},
            "kg_org": {"type": "string"},
            "nexus_token": {"type": "string"},
            "kg_proj": {"type": "string"},
        },
        "outputs": dict(recipe_dict["outputs"]),
        "steps": [],
    }

    for name, data in recipe_dict["inputs"].items():

        # we assume everything is a nexus entity
        assert data["type"] == "NexusType"
        config["inputs"][name] = {"class": "NexusType", "resource-id": data["resource-id"]}
        workflow["inputs"][name] = {"type": "NexusType"}

    allocation_builders: Dict[str, Optional[AllocationBuilder]] = {}

    for recipe_step in recipe_dict["steps"]:

        # TODO: Accessing the registry here can be out of sync with the resource of the variant
        # that is passed if the latter is updated directly on KG.
        variant = cwl_registry.Variant.from_registry(
            generator_name=recipe_step["run"]["generator_name"],
            variant_name=recipe_step["run"]["variant_name"],
            version=recipe_step["run"]["version"],
        )

        step_id = recipe_step["id"]
        allocation_builders[step_id] = _get_allocation_config(variant)

        # nexus configuration parameters are passed to each step from the global inputs
        # and resolved by the workflow task building.
        step_entry = {
            "id": step_id,
            "in": {
                **recipe_step["in"],
                "nexus_base": "kg_base",
                "nexus_org": "kg_org",
                "nexus_project": "kg_proj",
                "nexus_token": "nexus_token",
            },
            "out": recipe_step["out"],
            "run": variant.execute_definition_file,
        }
        workflow["steps"].append(step_entry)

    return workflow, config, allocation_builders


def _get_allocation_config(variant):

    try:
        path = variant.get_resources_file("variant_config.yml")
        return AllocationBuilder.from_config_path(path)
    except KeyError:
        return None
