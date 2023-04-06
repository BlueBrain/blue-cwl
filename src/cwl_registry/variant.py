"""Variant entry."""
import importlib.resources
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from cwl_luigi import cwl

from cwl_registry.constants import (
    CONFIGS_DIR_NAME,
    DEFINITION_FILENAME,
    DEFINITIONS_DIR_NAME,
    RESOURCES_DIR_NAME,
)
from cwl_registry.exceptions import CWLRegistryError
from cwl_registry.nexus import retrieve_variant_data
from cwl_registry.utils import get_directory_contents

L = logging.getLogger(__name__)


@dataclass(frozen=True)
class Variant:
    """Variant dataclass."""

    name: str
    generator_name: str
    version: str

    configs: Dict[str, Path]
    resources: Dict[str, Path]
    definitions: Dict[str, Path]

    def __repr__(self):
        """Return repr string."""
        return f"Variant({self.generator_name}, {self.name}, {self.version})"

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
    def from_registry(
        cls, generator_name: str, variant_name: str, version: Optional[str] = None
    ) -> "Variant":
        """Create a variant intance from the package's registry.

        Args:
            generator_name: The name of the generator group. Example: cell_composition
            variant_name: The name of the variant definition. Example: cell_composition_manipulation
            version: The variant's version. If None the latest release will be used. Example: v0.3.1
        """
        return _get_variant(generator_name, variant_name, version)

    @property
    def execute_definition_file(self):
        """Return the tool definition of the variant."""
        return self.get_definition_file(DEFINITION_FILENAME)

    @property
    def tool_definition(self):
        """Return the cwl definition for this variant."""
        return cwl.CommandLineTool.from_cwl(self.execute_definition_file)

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


def _get_variant(generator_name: str, variant_name: str, version: str):
    """Get variant's paths."""
    package_path = importlib.resources.files("cwl_registry")
    L.debug("Package path: %s", package_path)

    variant_dir = _get_variant_dir(package_path, generator_name, variant_name, version)

    configs = get_directory_contents(variant_dir / CONFIGS_DIR_NAME)
    resources = get_directory_contents(variant_dir / RESOURCES_DIR_NAME)
    definitions = get_directory_contents(variant_dir / DEFINITIONS_DIR_NAME)

    return Variant(
        name=variant_name,
        generator_name=generator_name,
        version=variant_dir.name,
        configs=configs,
        resources=resources,
        definitions=definitions,
    )


def _check_directory_exists(directory: Path) -> Path:
    if not directory.exists():
        raise CWLRegistryError(f"Directory '{directory}' does not exist.")
    return directory


def _get_variant_dir(package_path, generator_name: str, variant_name: str, version: str) -> str:
    root_dir = _check_directory_exists(package_path / "generators")
    generator_dir = _check_directory_names(root_dir / generator_name)
    variant_dir = _check_directory_names(generator_dir / variant_name)

    if version is None:
        version_dir = _get_latest_release_dir(variant_dir)
    else:
        version_dir = _check_directory_names(variant_dir / version)

    return version_dir


def _get_latest_release_dir(variant_dir: Path):
    release_dirs = _sorted_versions(
        v for v in variant_dir.iterdir() if v.is_dir() and _is_release(v.name)
    )
    if not release_dirs:
        raise CWLRegistryError(f"No versions found in {variant_dir}")
    return release_dirs[-1]


def _sorted_versions(version_paths):
    def as_int_tuple(version_dir: Path):
        tup = version_dir.name[1:].split(".")
        res = [0, 0, 0]
        for i, t in enumerate(tup):
            res[i] = int(t)
        return res

    return sorted(version_paths, key=as_int_tuple)


def _is_release(version_string):
    """Return True for a release, e.g. v0.3.1 but not v.0.3.1.dev0"""
    return re.match(r"^v(0|[1-9]\d*)?\.?(0|[1-9]\d*)?\.?(0|[1-9]\d*)$", version_string) is not None


def _check_directory_names(directory: Path):
    if not directory.is_dir():
        names = sorted(x.name for x in directory.parent.iterdir() if x.is_dir())
        raise CWLRegistryError(
            f"Directory '{directory.name}' does not exist. " f"Available names: {names}"
        )
    return directory
