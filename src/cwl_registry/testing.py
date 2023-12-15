"""Testing resources."""
import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from kgforge.core import Resource

from cwl_registry.nexus import get_forge


@contextmanager
def temporary_nexus_resource(
    properties: dict,
    distribution_contents: dict | None = None,
    nexus_base: str = None,
    nexus_org: str = None,
    nexus_proj: str = None,
):
    """Make a temporary nexus resource.

    Args:
        properties: A dict with the entity's properties.
        distribution: The contents of the json file to be attached.
    """
    forge = get_forge(nexus_base=nexus_base, nexus_org=nexus_org, nexus_project=nexus_proj)

    assert "type" in properties

    resource = Resource.from_json(properties)

    if distribution_contents:
        filepath = _data_to_json_file(data=distribution_contents)
        resource.distribution = forge.attach(path=filepath, content_type="application/json")

    forge.register(resource)

    assert resource.id

    try:
        yield resource
    finally:
        forge.deprecate(resource)


def _data_to_json_file(data):
    serialized_data = json.dumps(data, indent=2)

    with tempfile.NamedTemporaryFile(suffix=".json") as tfile:
        filepath = Path(tfile.name)
        filepath.write_text(serialized_data, encoding="utf-8")
        yield filepath


@contextmanager
def cwd(path):
    """Context manager to temporarily change the working directory."""
    original_cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(original_cwd)


def patchenv(**envvars):
    """Patch function environment."""
    return patch.dict(os.environ, envvars, clear=True)
