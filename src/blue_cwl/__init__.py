"""blue_cwl."""

from importlib.metadata import version

__version__ = version(__package__)

from blue_cwl.variant import Variant

__all__ = ["Variant", "__version__"]
