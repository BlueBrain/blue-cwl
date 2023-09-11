"""Entities."""
from entity_management.core import Entity

from cwl_registry.mmodel import schemas


def _distribution_to_model(distribution, schema_class):
    path = distribution.get_location()[7:]
    return schema_class.parse_file(path)


class MorphologyAssignmentConfig(Entity):
    """Morphology assignment config entity."""

    def to_model(self) -> schemas.MModelConfigRaw:
        """Return the config from the json distribution."""
        # pylint: disable=no-member
        return _distribution_to_model(self.distribution, schemas.MModelConfigRaw)


class CanonicalMorphologyModelConfig(Entity):
    """Canonical morphology model config"""

    def to_model(self) -> schemas.CanonicalDistributionConfig:
        """Return the config from the json distribution."""
        # pylint: disable=no-member
        return _distribution_to_model(self.distribution, schemas.CanonicalDistributionConfig)


class PlaceholderMorphologyConfig(Entity):
    """Placeholder morphologies config"""

    def to_model(self) -> schemas.PlaceholderDistributionConfig:
        """Return the config from the json distribution."""
        # pylint: disable=no-member
        return _distribution_to_model(self.distribution, schemas.PlaceholderDistributionConfig)
