"""Nexus target."""
import logging

import luigi

from cwl_luigi import hashing
from cwl_luigi.nexus import get_kg_forge

L = logging.getLogger(__name__)


class NexusTarget(luigi.Target):
    """A target for nexus entities."""

    def __init__(self, name, task):
        """Target instance.

        Args:
            name: Name of the target.
            task: The task that generated this target.
        """
        self.name = name
        self.task = task

    @property
    def resource(self):
        """Get a resource by searching the knowledge graph about its type and unique hash."""
        target_hexdigest = hashing.get_target_hexdigest(self.task.digest(), self.name)

        forge = get_kg_forge(
            nexus_base=self.task.kg_config["kg_base"],
            nexus_org=self.task.kg_config["kg_org"],
            nexus_project=self.task.kg_config["kg_proj"],
        )

        resources = forge.search(
            {
                "wasGeneratedBy": {"type": "BMOTask", "targetDigest": target_hexdigest},
            }
        )

        if resources:
            assert len(resources) == 1
            return resources[0]

        return None

    def exists(self):
        """Return True if the resource exists in kg."""
        return self.resource is not None
