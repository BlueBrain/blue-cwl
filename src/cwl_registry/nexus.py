"""Nexus stuff."""
import json
import logging
import os
from pathlib import Path
from typing import Dict, Optional

from kgforge.core import KnowledgeGraphForge, Resource

ext_to_format = {
    ".json": "application/json",
    ".yaml": "application/yaml",
    ".yml": "application/yaml",
    ".cwl": "application/yaml",
}


L = logging.getLogger(__name__)


def get_forge(
    nexus_base: str = None,
    nexus_org: str = None,
    nexus_project: str = None,
    nexus_token: str = None,
):
    """Get KG forge."""
    nexus_base = nexus_base or os.getenv("NEXUS_BASE")
    nexus_org = nexus_org or os.getenv("NEXUS_ORG")
    nexus_project = nexus_project or os.getenv("NEXUS_PROJ")
    nexus_token = nexus_token or os.getenv("NEXUS_TOKEN")

    return KnowledgeGraphForge(
        configuration="https://raw.githubusercontent.com/BlueBrain/nexus-forge/master/examples/notebooks/use-cases/prod-forge-nexus.yml",
        bucket=f"{nexus_org}/{nexus_project}",
        endpoint=nexus_base,
        searchendpoints={
            "sparql": {
                "endpoint": "https://bbp.epfl.ch/neurosciencegraph/data/views/aggreg-sp/dataset"
            },
            "elastic": {
                "endpoint": "https://bbp.epfl.ch/neurosciencegraph/data/views/aggreg-es/dataset",
                "mapping": "https://bbp.epfl.ch/neurosciencegraph/data/views/es/dataset",
                "default_str_keyword_field": "keyword",
            },
        },
        token=nexus_token,
    )


def get_resource(forge, resource_id):
    """Get resource from knowledge graph."""
    resource = forge.retrieve(resource_id, cross_bucket=True)

    assert resource is not None

    return resource


def _remove_prefix(prefix, path):
    if path.startswith(prefix):
        return path[len(prefix) :]
    return path


def read_json_file_from_resource(resource):
    """Read json file from kg resource."""
    if isinstance(resource.distribution, list):
        assert len(resource.distribution) == 1
        distribution = resource.distribution[0]
    else:
        distribution = resource.distribution

    filepath = distribution.atLocation.location

    with open(_remove_prefix("file://", filepath), mode="r", encoding="utf-8") as fd:
        return json.load(fd)


def register_variant(forge: KnowledgeGraphForge, variant):
    """Create a kg resource out of the variant files."""
    variant_spec = f"{variant.generator_name}|{variant.name}|{variant.version}"

    configs = Resource(
        name=f"Task Variant Parameters: {variant_spec}",
        type="VariantParameters",
        generator_name=variant.generator_name,
        variant_name=variant.name,
        version=variant.version,
        hasPart=_create_parts(forge, variant.configs),
    )
    forge.register(configs)

    definitions = Resource(
        name=f"Task Variant Definitions: {variant_spec}",
        type="VariantDefinitions",
        generator_name=variant.generator_name,
        variant_name=variant.name,
        version=variant.version,
        hasPart=_create_parts(forge, variant.definitions),
    )
    forge.register(definitions)

    resources = Resource(
        name=f"Task Variant Allocation Resources: {variant_spec}",
        type="VariantResources",
        generator_name=variant.generator_name,
        variant_name=variant.name,
        version=variant.version,
        hasPart=_create_parts(forge, variant.resources),
    )
    forge.register(resources)

    resource = Resource(
        name=f"Task Variant Configuration: {variant_spec}",
        variant_name=variant.name,
        generator_name=variant.generator_name,
        version=variant.version,
        type="VariantConfig",
        configs=forge.reshape(configs, ["id", "type"]),
        definitions=forge.reshape(definitions, ["id", "type"]),
        allocation_resources=forge.reshape(resources, ["id", "type"]),
    )
    forge.register(resource)
    return resource


def _create_parts(forge: KnowledgeGraphForge, dictionary):
    return [
        Resource(distribution=forge.attach(path=path, content_type=ext_to_format[path.suffix]))
        for path in dictionary.values()
    ]


def retrieve_variant_data(
    forge: KnowledgeGraphForge, resource_id: str, staging_dir: Optional[Path] = None
):
    """Retrieve variant data from KG resource."""
    variant_resource = forge.retrieve(resource_id, cross_bucket=True)
    L.debug("Variant resource: %s", variant_resource)

    configs_resource = forge.retrieve(variant_resource.configs.id, cross_bucket=True)
    L.debug("Variant configs resource: %s", configs_resource)

    resources_resource = forge.retrieve(variant_resource.allocation_resources.id, cross_bucket=True)
    L.debug("Variant allocation resource: %s", resources_resource)

    definitions_resource = forge.retrieve(variant_resource.definitions.id, cross_bucket=True)
    L.debug("Variant definitions resource: %s", definitions_resource)

    configs = _get_files(configs_resource)
    resources = _get_files(resources_resource)
    definitions = _get_files(definitions_resource)

    if staging_dir:
        staging_dir = Path(staging_dir).resolve()
        configs = _stage_into_subdir(staging_dir / "configs", configs)
        resources = _stage_into_subdir(staging_dir / "resources", resources)
        definitions = _stage_into_subdir(staging_dir / "definitions", definitions)

    return {
        "name": variant_resource.variant_name,
        "generator_name": variant_resource.variant_name,
        "version": variant_resource.version,
        "configs": configs,
        "resources": resources,
        "definitions": definitions,
    }


def _stage_into_subdir(subdir_path: Path, paths_dict: Dict[str, Path]):
    new_paths_dict = {}
    subdir_path.mkdir(parents=True, exist_ok=True)
    for filename, path in paths_dict.items():

        new_path = subdir_path / filename
        L.debug("%s - > %s", path, new_path)

        new_path.unlink(missing_ok=True)

        os.symlink(path, new_path)
        new_paths_dict[filename] = new_path
    return new_paths_dict


def _get_files(resource):

    files = {}
    for part in resource.hasPart:

        if hasattr(part, "distribution"):

            distribution = part.distribution

            filename = distribution.name
            path = distribution.atLocation.location

            if path.startswith("file://"):
                path = path[7:]

            files[filename] = Path(path)

    return files
