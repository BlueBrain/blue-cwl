# SPDX-License-Identifier: Apache-2.0

"""Types module."""

import enum
import hashlib
import os
from pathlib import Path
from typing import Literal

from entity_management.util import unquote_uri_path

from blue_cwl.core.common import CustomBaseModel
from blue_cwl.core.exceptions import CWLError

# according to the CWL spec only the first 64KB are loaded to contents
# https://www.commonwl.org/v1.0/CommandLineTool.html#File
FILE_BUFFER_SIZE = 64 * 1024

CWLType = Literal[
    "null",
    "boolean",
    "int",
    "long",
    "float",
    "double",
    "string",
    "File",
    "Directory",
    "stdin",
    "stdout",
    "stderr",
    "Any",
    "NexusType",
]


class CWLWorkflowType(enum.Enum):
    """Workflow node types."""

    INPUT = enum.auto()
    OUTPUT = enum.auto()
    STEP = enum.auto()


class _Path(CustomBaseModel):
    """Path class."""

    path: str | None = None
    location: str | None = None

    def __init__(self, **data):
        """Initialize a FileLike object."""
        path = data.get("path")
        location = data.get("location")

        if path is None and location is None:
            raise CWLError("Either 'path' or 'location' can be None, not both.")

        if path and not location:
            data["path"] = str(path)
            data["location"] = f"file://{os.path.abspath(path)}"

        if location and not path:
            path = data["path"] = unquote_uri_path(location)

        super().__init__(**data)

    @property
    def basename(self):
        """Return the base name of the file path."""
        return Path(self.path).name


class File(_Path):
    """File class."""

    @property
    def dirname(self):
        """Return the path to the directory containing the file."""
        return str(Path(self.path).parent)

    @property
    def nameroot(self):
        """Return the base name without extension."""
        return Path(self.path).stem

    @property
    def nameext(self):
        """Return the extension of the base name."""
        return Path(self.path).suffix

    @property
    def contents(self):
        """Return first 64KB from file."""
        return Path(self.path).open(buffering=FILE_BUFFER_SIZE, encoding="utf-8").read()

    @property
    def size(self):
        """Return the size of the file."""
        return Path(self.path).stat().st_size

    @property
    def checksum(self):
        """Return the sha1 checksum of the file."""
        with open(self.path, "rb") as f:
            sha1 = hashlib.sha1()  # noqa: S324
            while chunk := f.read(FILE_BUFFER_SIZE):
                sha1.update(chunk)
        return sha1.hexdigest()


class Directory(_Path):
    """Directory class."""


class NexusResource(CustomBaseModel):
    """NexusResource class."""

    id: str | None = None
    path: str | None = None

    def __init__(self, **data):
        """Initialize NexusResource."""
        resource_id = data.get("id")
        resource_path = data.get("path")

        if resource_id is None and resource_path is None:
            raise CWLError("Either 'resource_id' or 'resource_path' can be None, not both.")

        super().__init__(**data)
