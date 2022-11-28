"""Variant entry."""
import importlib.resources
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from cwl_registry.exceptions import CWLRegistryError
from cwl_registry.nexus import retrieve_variant_data
from cwl_registry.utils import get_directory_contents

L = logging.getLogger(__name__)


CONFIGS_DIR_NAME = "configs"
RESOURCES_DIR_NAME = "resources"
DEFINITIONS_DIR_NAME = "definitions"


@dataclass(frozen=True)
class Variant:
    """Variant dataclass."""

    name: str
    generator_name: str
    version: str

    configs: Dict[str, Path]
    resources: Dict[str, Path]
    definitions: Dict[str, Path]

    @classmethod
    def from_resource_id(
        cls, forge, resource_id: str, staging_dir: Optional[Path] = None
    ) -> "Variant":
        """Create a variant instance from a KG resource.

        Args:
            forge: Instance of kg forge.
            resource_id: KG resource id
            staging_dir: Optional dir to stage the data. If None the nexus gpfs paths will be used,
                otherwise symlinks will be created.
        """
        return cls(**retrieve_variant_data(forge, resource_id, staging_dir))

    @classmethod
    def from_registry(cls, generator_name: str, variant_name: str, version: str) -> "Variant":
        """Create a variant intance from the package's registry."""
        return _get_variant(generator_name, variant_name, version)

    @property
    def execute_definition_file(self):
        """Return the tool definition of the variant."""
        return self.get_definition_file("execute.cwl")

    def get_config_file(self, filename: str) -> Path:
        """Get config file path.

        Example: get_config_file('config.json')

        Returns:
            The path to the file if it exists, throws otherwise.
        """
        return self.configs[filename]

    def get_definition_file(self, filename: str) -> Path:
        """Get definition file path.

        Example: get_definition_file('execute.cwl')

        Returns:
            The path to the file if it exists, throws otherwise.
        """
        return self.definitions[filename]

    def get_resources_file(self, filename: str) -> Path:
        """Get resource configuration file path.

        Example: get_definition_file('cluster_config.yml')

        Returns:
            The path to the file if it exists, throws otherwise.
        """
        return self.resources[filename]


def _get_variant(generator_name: str, variant_name: str, version: str = None):
    """Get variant's paths."""
    if version is None:
        version = "latest"

    package_path = importlib.resources.files("cwl_registry")
    L.debug("Package path: %s", package_path)

    variant_spec = _get_variant_spec(generator_name, variant_name, version)
    variant_dir = _check_directory_exists(package_path / variant_spec)

    configs = get_directory_contents(variant_dir / CONFIGS_DIR_NAME)
    resources = get_directory_contents(variant_dir / RESOURCES_DIR_NAME)
    definitions = get_directory_contents(variant_dir / DEFINITIONS_DIR_NAME)

    return Variant(
        name=variant_name,
        generator_name=generator_name,
        version=str(version),
        configs=configs,
        resources=resources,
        definitions=definitions,
    )


def _check_directory_exists(directory: Path) -> Path:
    if not directory.exists():
        raise CWLRegistryError(f"Directory '{directory}' does not exist.")
    return directory


def _get_variant_spec(generator_name: str, variant_name: str, version: str) -> str:
    return f"generators/{generator_name}/{variant_name}/{version}"
