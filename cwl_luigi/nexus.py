"""Nexus utils."""
import json
import os
from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, List, Optional

from kgforge.core import KnowledgeGraphForge, Resource

from cwl_luigi.utils import load_json

INTERLEX_IDENTIFIER_BASE = "http://uri.interlex.org/base"

ENVIRONMENTS = {
    "prod": "https://raw.githubusercontent.com/BlueBrain/nexus-forge/master/examples/notebooks/use-cases/prod-forge-nexus.yml"
}


@dataclass
class ResourceGroupMember:
    """Group's member."""

    _rev: Optional[str]
    type: List[str]
    resource: Resource

    @classmethod
    def from_json(cls, forge, data):
        """Construct from json data."""
        return cls(
            _rev=data.get("_rev", None),
            type=data.get("type", ["unknown", "unknown"]),
            resource=get_resource(forge, data["id"]),
        )


@dataclass
class ResourceGroup:
    """Resource group."""

    label: str
    members: List[ResourceGroupMember]

    @classmethod
    def from_json(cls, forge, data):
        """Construct from json data."""
        members = [ResourceGroupMember.from_json(forge, entry) for entry in data["hasPart"]]

        return cls(label=data["label"], members=members)


@dataclass
class GroupedDataset:
    """Grouped dataset representation for nexus resources."""

    context: str
    groups: Dict[str, ResourceGroup]

    @classmethod
    def from_json(cls, forge, data):
        """Construct from json data."""
        data = deepcopy(data)

        _jsonld_to_json(data)

        context = data.pop("context", None)

        groups = {}
        for k, v in data.items():
            if k.startswith(INTERLEX_IDENTIFIER_BASE):
                groups[k] = ResourceGroup.from_json(forge, v)

        return cls(context=context, groups=groups)

    @classmethod
    def from_resource(cls, forge, resource):
        """Construct from resource."""
        return cls.from_json(forge, read_json_file_from_resource(resource))

    @classmethod
    def from_file(cls, forge, path):
        """Construct from path."""
        return cls.from_json(forge, load_json(path))


def _jsonld_to_json(data):

    if isinstance(data, dict):
        keys_to_change = set()
        for k, v in data.items():
            if k.startswith("@"):
                keys_to_change.add(k)
            _jsonld_to_json(v)
        for key in keys_to_change:
            data[key[1:]] = data.pop(key)
    elif isinstance(data, list):
        for v in data:
            _jsonld_to_json(v)


def _get_var_from_environment(var_name):
    try:
        return os.environ[var_name]
    except KeyError as e:
        raise ValueError(f"'{var_name}' env var not found.") from e


def get_kg_forge(nexus_base=None, nexus_org=None, nexus_project=None, nexus_token=None):
    """Get kg forge."""
    nexus_base = nexus_base or _get_var_from_environment("NEXUS_BASE")

    nexus_org = nexus_org or _get_var_from_environment("NEXUS_ORG")

    nexus_project = nexus_project or _get_var_from_environment("NEXUS_PROJ")

    nexus_token = nexus_token or _get_var_from_environment("NEXUS_TOKEN")

    return KnowledgeGraphForge(
        ENVIRONMENTS["prod"],
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
