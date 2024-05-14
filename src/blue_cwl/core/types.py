"""Common types."""

import os

from blue_cwl.core.cwl_types import Directory, File, NexusResource

PathLike = str | os.PathLike[str]

InputValue = str | bool | int | float | list[str] | list[int] | list[float] | list[str]

InputValueObject = (
    str
    | int
    | float
    | list[str]
    | list[int]
    | list[float]
    | File
    | list[File]
    | Directory
    | list[Directory]
    | NexusResource
    | list[NexusResource]
)

OutputValueObject = File | Directory | NexusResource


EnvVarDict = dict[str, int | float | str]
