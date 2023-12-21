"""Definition of nexus entities."""

from entity_management.base import AttrOf, attributes
from entity_management.core import DataDownload, Entity


@attributes(
    {
        "generator_name": AttrOf(str),
        "variant_name": AttrOf(str),
        "version": AttrOf(str),
        "distribution": AttrOf(DataDownload),
    },
)
class Variant(Entity):
    """Variant entity."""
