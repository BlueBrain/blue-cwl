"""Typing related definitions."""

import os

from entity_management.core import Entity

StrOrPath = str | os.PathLike[str]

IdOrEntity = str | Entity
