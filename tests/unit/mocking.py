import uuid
import yaml
import shutil
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch, Mock

import pytest

from kgforge.core import Resource
from kgforge.core.conversions.json import as_json, from_json
from kgforge.core.forge import KnowledgeGraphForge
from kgforge.core import Resource
from kgforge.core.commons.files import load_file_as_byte
from kgforge.core.reshaping import Reshaper


class LocalForge:
    storage = {}

    def __init__(self, configuration=None, output_dir=None):
        if configuration is None:
            configuration = "https://raw.githubusercontent.com/BlueBrain/nexus-forge/master/examples/notebooks/use-cases/prod-forge-nexus.yml"

        if isinstance(configuration, str):
            config_data = load_file_as_byte(configuration)
            config_data = config_data.decode("utf-8")
            self._config = yaml.safe_load(config_data)
        else:
            self._config = deepcopy(configuration)

        if output_dir is None:
            self._output_dir = None
        else:
            self._output_dir = Path(output_dir).resolve()

    def as_json(self, resource):
        return {}

    def _get_id(self):
        new_id = str(uuid.uuid1())
        while new_id in self.storage:
            new_id = str(uuid.uuid1())
        return new_id

    def reshape(self, data, keep, versioned=False):
        """Use the original reshaper."""
        template = self._config["Store"].get("versioned_id_template", None)
        return Reshaper(template).reshape(data, keep, versioned)

    def attach(self, path, content_type):
        resource_id = self._get_id()

        assert self._output_dir

        filename = Path(path).name

        destination_dir = self._output_dir / resource_id
        destination_dir.mkdir()

        destination = destination_dir / filename
        shutil.copyfile(path, destination)

        resource = Resource.from_json(
            {
                "id": resource_id,
                "type": "DataDownload",
                "name": filename,
                "atLocation": Resource.from_json(
                    {"type": "Location", "location": f"file://{str(destination)}"}
                ),
                "encodingFormat": content_type,
            }
        )
        self.register(resource)
        return resource

    def register(self, resource, *args, **kwargs):
        if not hasattr(resource, "id"):
            resource.id = self._get_id()

        self.storage[resource.id] = resource

    def retrieve(self, resource_id, *args, **kwargs):
        return self.storage[resource_id]
