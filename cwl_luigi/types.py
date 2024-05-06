"""Common types."""

import os

PathLike = str | os.PathLike[str]

InputValueType = str | bool | int | float | list[str] | list[int] | list[float] | list[str]

EnvVarDict = dict[str, int | float | str]
