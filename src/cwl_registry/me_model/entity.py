"""Entities."""
from entity_management.base import AttrOf, Frozen, attributes
from entity_management.core import DataDownload, Entity, Identifiable

from cwl_registry.validation import validate_schema


class FitnessCalculatorConfiguration(Identifiable):
    """FitnessCalculatorConfiguration."""


class ExtractionTargetsConfiguration(Identifiable):
    """ExtractionTargetsConfiguration."""


class EModelPipelineSettings(Identifiable):
    """EModelPipelineSettings."""


class EModelConfiguration(Identifiable):
    """EModelConfiguration."""


class EModelWorkflow(Entity):
    """EModelWorkflow."""


@attributes({"followedWorkflow": AttrOf(EModelWorkflow)})
class FollowedWorkflowActivity(Frozen):
    """FollowedWorkflowActivity."""


@attributes({"activity": AttrOf(FollowedWorkflowActivity)})
class _EModelGeneration(Frozen):
    """EModelGeneration."""


@attributes(
    {
        "distribution": AttrOf(list[DataDownload]),
        "generation": AttrOf(_EModelGeneration, default=None),
    }
)
class EModel(Identifiable):
    """EModel definition."""


class MEModelConfig(Entity):
    """Morpho-electric assignment config."""

    def get_validated_content(self) -> dict:
        """Return the config from the json distribution."""
        # pylint: disable=no-member
        dataset = self.distribution.as_dict()
        validate_schema(data=dataset, schema_name="me_model_config_distribution.yml")
        return dataset
