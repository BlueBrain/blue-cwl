"""CWL files builder from UI recipe."""
from typing import Any, Dict, Tuple

import cwl_registry


def build_workflow_cwl_datasets(
    recipe_dict: dict, nexus_token: str, nexus_base: str, nexus_org: str, nexus_project: str
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Convert the recipe dict into a cwl workflow and a cwl config."""
    config: Dict[str, Any] = {
        "inputs": {
            "nexus_base": nexus_base,
            "nexus_org": nexus_org,
            "nexus_project": nexus_project,
            "nexus_token": nexus_token,
        }
    }
    workflow: Dict[str, Any] = {
        "cwlVersion": "v1.2",
        "class": "Workflow",
        "id": "Workflow",
        "inputs": {
            "nexus_base": {"type": "string"},
            "nexus_org": {"type": "string"},
            "nexus_token": {"type": "string"},
            "nexus_project": {"type": "string"},
        },
        "outputs": dict(recipe_dict["outputs"]),
        "steps": [],
    }

    for name, data in recipe_dict["inputs"].items():

        # we assume everything is a nexus entity
        assert data["type"] == "NexusType"
        config["inputs"][name] = {"class": "NexusType", "resource-id": data["resource-id"]}
        workflow["inputs"][name] = {"type": "NexusType"}

    for recipe_step in recipe_dict["steps"]:

        variant = cwl_registry.get_variant(
            generator_name=recipe_step["run"]["generator_name"],
            variant_name=recipe_step["run"]["variant_name"],
            version=recipe_step["run"]["version"],
        )

        # nexus configuration parameters are passed to each step from the global inputs
        # and resolved by the workflow task building.
        step_entry = {
            "id": recipe_step["id"],
            "in": {
                **recipe_step["in"],
                "nexus_base": "nexus_base",
                "nexus_org": "nexus_org",
                "nexus_project": "nexus_project",
                "nexus_token": "nexus_token",
            },
            "out": recipe_step["out"],
            "run": variant.execute_definition_file,
        }
        workflow["steps"].append(step_entry)

    return workflow, config
