"""Nexus stuff."""
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Dict, Optional

import jwt
import requests
from entity_management import state
from entity_management.nexus import (
    _print_nexus_error,
    file_as_dict,
    get_unquoted_uri_path,
    load_by_id,
)
from entity_management.util import unquote_uri_path
from kgforge.core import KnowledgeGraphForge, Resource

from cwl_registry.exceptions import CWLRegistryError
from cwl_registry.utils import load_arrow

ext_to_format = {
    ".json": "application/json",
    ".yaml": "application/yaml",
    ".yml": "application/yaml",
    ".cwl": "application/yaml",
}


L = logging.getLogger(__name__)


@dataclass(frozen=True)
class NexusConfig:
    """Nexus configuration dataclass."""

    base: str
    org: str
    proj: str

    @property
    def bucket(self):
        """Get nexus bucket."""
        return f"{self.org}/{self.proj}"


DEFAULT_NEXUS_CONFIG = NexusConfig(
    base=state.get_base(), org=state.get_org(), proj=state.get_proj()
)


# Renew the token if it expires in 5 minutes from now
SECONDS_TO_EXPIRATION = 5 * 60


def _decode(token):
    """Decode the token, and return its contents."""
    return jwt.decode(token, options={"verify_signature": False})


def _has_expired(token):
    """Check if the token has expired or is going to expire in 'SECONDS_TO_EXPIRATION'."""
    expiration_time = _decode(token)["exp"]
    return datetime.timestamp(datetime.now()) + SECONDS_TO_EXPIRATION > expiration_time


def _refresh_token_on_failure(func):
    """Refresh access token on failure and try again."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        """Decorator function"""
        try:
            return func(*args, **kwargs)
        except requests.exceptions.HTTPError as e1:
            if e1.response.status_code == 401 and state.has_offline_token():
                kwargs["token"] = state.refresh_token()
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.HTTPError as e2:
                    _print_nexus_error(e2)
                    raise
            _print_nexus_error(e1)
            raise

    return wrapper


def _get_valid_token(token: Optional[str] = None, force_refresh: bool = False) -> str:
    """Return a valid token if possible."""
    if token is None:
        token = state.get_token()
    else:
        state.set_token(token)

    # the access token can only be refreshed if an offline/refresh token is available
    if (force_refresh or _has_expired(token)) and state.has_offline_token():
        return state.refresh_token()

    return token


def get_forge(
    nexus_base: str = None,
    nexus_org: str = None,
    nexus_project: str = None,
    nexus_token: str = None,
    force_refresh: bool = False,
):  # pragma: no cover
    """Get KG forge."""
    nexus_base = nexus_base or os.getenv("NEXUS_BASE")
    nexus_org = nexus_org or state.get_org()
    nexus_project = nexus_project or state.get_proj()
    nexus_token = _get_valid_token(nexus_token, force_refresh)

    return _refresh_token_on_failure(KnowledgeGraphForge)(
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


def forge_to_config(forge):
    """Get nexus configuration from forge instance."""
    store = forge._store  # pylint: disable=protected-access
    return store.endpoint, store.bucket, store.token


def find_variants(forge, generator_name, variant_name, version):
    """Return variants from KG."""
    return forge.search(
        {
            "type": "Variant",
            "generator_name": generator_name,
            "variant_name": variant_name,
            "version": version,
        }
    )


def get_resource(forge, resource_id):
    """Get resource from knowledge graph."""
    resource = forge.retrieve(resource_id, cross_bucket=True)

    if resource is None:
        # pylint: disable=protected-access
        raise CWLRegistryError(
            f"Resource id {resource_id} could not be retrieved.\n"
            f"endpoint: {forge._store.endpoint}\n"
            f"bucket  : {forge._store.bucket}"
        )
    return resource


def get_resource_json_ld(resource_id: str, forge, cross_bucket=True) -> dict:
    """Get json-ld dictionary from resource id."""
    endpoint, bucket, token = forge_to_config(forge)
    org, proj = bucket.split("/")
    return load_by_id(
        resource_id=resource_id,
        cross_bucket=cross_bucket,
        base=endpoint,
        org=org,
        proj=proj,
        token=token,
    )


def _remove_prefix(prefix, path):
    if path.startswith(prefix):
        return path[len(prefix) :]
    return path


def _without_file_prefix(path):
    return _remove_prefix("file://", path)


def _get_distribution(resource):
    if isinstance(resource.distribution, list):
        assert len(resource.distribution) == 1
        distribution = resource.distribution[0]
    else:
        distribution = resource.distribution

    return distribution


def read_json_file_from_resource_id(forge, resource_id: str) -> dict:
    """Read json file from kg resource id."""
    return read_json_file_from_resource(get_resource(forge, resource_id))


def read_json_file_from_resource(resource) -> dict:
    """Read json file from kg resource."""
    distribution = _get_distribution(resource)
    return file_as_dict(distribution.contentUrl)


def read_arrow_file_from_resource(resource):
    """Read arrow file from kg resource."""
    distribution = _get_distribution(resource)
    gpfs_location = get_unquoted_uri_path(distribution.contentUrl)
    return load_arrow(gpfs_location)


def register_variant(forge: KnowledgeGraphForge, variant, update=False):
    """Create a kg resource out of the variant files."""
    existing = find_variants(forge, variant.generator_name, variant.name, variant.version)

    if existing and not update:
        raise CWLRegistryError(f"Variant {variant} already registered in KG.")

    variant_spec = f"{variant.generator_name}|{variant.name}|{variant.version}"

    configs = Resource(
        name=f"Variant Parameters: {variant_spec}",
        type="VariantParameters",
        generator_name=variant.generator_name,
        variant_name=variant.name,
        version=variant.version,
        hasPart=_create_parts(forge, variant.configs),
    )
    forge.register(configs)

    definitions = Resource(
        name=f"Variant Definitions: {variant_spec}",
        type="VariantDefinitions",
        generator_name=variant.generator_name,
        variant_name=variant.name,
        version=variant.version,
        hasPart=_create_parts(forge, variant.definitions),
    )
    forge.register(definitions)

    resources = Resource(
        name=f"Variant Allocation Resources: {variant_spec}",
        type="VariantResources",
        generator_name=variant.generator_name,
        variant_name=variant.name,
        version=variant.version,
        hasPart=_create_parts(forge, variant.resources),
    )
    forge.register(resources)

    configs = forge.reshape(configs, ["id", "type"])
    definitions = forge.reshape(definitions, ["id", "type"])
    resources = forge.reshape(resources, ["id", "type"])

    if existing:
        assert len(existing) == 1
        resource = get_resource(forge, existing[0].id)
        resource.configs = configs
        resource.definitions = definitions
        resource.allocation_resources = resources
        forge.update(resource)
    else:
        resource = Resource(
            name=f"Variant: {variant_spec}",
            variant_name=variant.name,
            generator_name=variant.generator_name,
            version=variant.version,
            type="Variant",
            configs=configs,
            definitions=definitions,
            allocation_resources=resources,
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
    variant_resource = get_resource(forge, resource_id)
    assert variant_resource is not None
    L.debug("Variant resource: %s", variant_resource)

    try:
        configs_resource = get_resource(forge, variant_resource.configs.id)
        configs = _get_files(configs_resource)
    except AttributeError:
        configs = {}
    L.debug("Variant configs: %s", configs)

    resources_resource = get_resource(forge, variant_resource.allocation_resources.id)
    L.debug("Variant allocation resource: %s", resources_resource)

    definitions_resource = get_resource(forge, variant_resource.definitions.id)
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
            files[distribution.name] = Path(unquote_uri_path(distribution.atLocation.location))

    return files


def get_config_path_from_circuit_resource(forge, resource_id: str) -> Path:
    """Get config path from resource.

    Note:
        It supports the following representations of circuitConfigPath:
            - A single string with or without a file prefix.
            - A DataDownload resource with the config path as a url with or without file prefix.
    """
    partial_circuit_resource = get_resource(forge, resource_id)

    config_path = partial_circuit_resource.circuitConfigPath

    # DataDownload resource with a url
    try:
        path = config_path.url
    # A single string
    except AttributeError:
        path = config_path

    return Path(unquote_uri_path(path))


def get_region_resource_acronym(forge, resource_id: str) -> str:
    """Retrieve the hierarchy acronym from a KG registered region."""
    endpoint, _, token = forge_to_config(forge)
    return load_by_id(
        resource_id=resource_id,
        cross_bucket=False,
        base=endpoint,
        org="neurosciencegraph",
        proj="datamodels",
        token=token,
    )["notation"]
