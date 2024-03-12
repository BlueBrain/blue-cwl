"""Entities."""
from entity_management.config import MEModelConfig as _MEModelConfig

from cwl_registry.validation import validate_schema


class MEModelConfig(_MEModelConfig):
    """Morpho-electric assignment config."""

    def get_validated_content(self) -> dict:
        """Return the config from the json distribution."""
        # pylint: disable=no-member
        dataset = self.distribution.as_dict()
        validate_schema(data=dataset, schema_name="me_model_config_distribution.yml")
        return dataset
