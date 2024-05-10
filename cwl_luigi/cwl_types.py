"""Types module."""

import enum
import os
from typing import Literal

from cwl_luigi.common import CustomBaseModel

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


class _FileLike(CustomBaseModel):

    path: str | None = None
    location: str | None = None
    basename: str | None = None

    def __init__(self, **data):
        """Initialize a FileLike object."""
        path = data.get("path")
        location = data.get("location")

        assert path or location

        if path and not location:
            data["path"] = str(path)
            data["location"] = f"file://{os.path.abspath(path)}"

        if location and not path:
            data["path"] = str(location)[7:]

        if data.get("basename", None) is None:
            data["basename"] = os.path.basename(data["path"])

        super().__init__(**data)


class File(_FileLike):
    """File class."""


class Directory(_FileLike):
    """Directory class."""


class NexusResource(CustomBaseModel):
    """NexusResource class."""

    id: str | None = None
    path: str | None = None

    def __init__(self, **data):
        """Initialize NexusResource."""
        resource_id = data.get("id")
        resource_path = data.get("path")

        assert resource_id or resource_path

        super().__init__(**data)
