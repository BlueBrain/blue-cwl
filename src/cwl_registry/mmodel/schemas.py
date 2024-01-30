"""Schemas."""
import hashlib
import json
from enum import Enum
from pathlib import Path

from cwl_registry.mmodel.config import split_config
from cwl_registry.mmodel.staging import (
    materialize_canonical_config,
    materialize_placeholders_config,
)
from cwl_registry.model import CustomBaseModel
from cwl_registry.nexus import get_distribution_as_dict
from cwl_registry.staging import get_entry_id


class VariantEntry(str, Enum):
    """Morphology assignment variants."""

    topological = "topological_synthesis"
    placeholder = "placeholder_assignment"


class VariantInfo(CustomBaseModel):
    """Variant entry information."""

    algorithm: str
    version: str


class SynthesisOverrides(CustomBaseModel):
    """Synthesis inputs overrides."""

    total_extent: float | None = None
    randomness: float | None = None
    orientation: list | None = None
    step_size: dict[str, dict[str, float]] | None = None
    radius: float | None = None


class CanonicalMorphologyModel(CustomBaseModel):
    """Synthesis datasets."""

    parameters: Path
    distributions: Path

    overrides: dict[str, SynthesisOverrides] | None = None

    def checksum(self) -> str:
        """Return the checksum of the data structure."""
        filehash = hashlib.blake2b()
        filehash.update(Path(self.parameters).read_bytes())
        filehash.update(Path(self.distributions).read_bytes())

        if self.overrides:
            overrides = {}
            for neurite_type, neurite_overrides in self.overrides.items():
                neurite_overrides = {
                    k: v for k, v in neurite_overrides.to_dict().items() if v is not None
                }
                if neurite_overrides:
                    overrides[neurite_type] = neurite_overrides
            if overrides:
                filehash.update(json.dumps(overrides, sort_keys=True).encode())

        return filehash.hexdigest()

    def __eq__(self, other):
        """Return true if the two objects have the same checksum."""
        return self.checksum() == other.checksum()


class CanonicalDistributionConfig(CustomBaseModel):
    """Canonical distribution config."""

    data: dict

    def materialize(
        self,
        *,
        output_file=None,
        labels_only=False,
        base=None,
        org=None,
        proj=None,
        token=None,
    ) -> dict:
        """Materialize distribution config."""
        return materialize_canonical_config(
            dataset=self.data,
            model_class=CanonicalMorphologyModel,
            output_file=output_file,
            labels_only=labels_only,
            base=base,
            org=org,
            proj=proj,
            token=token,
        )


class PlaceholderDistributionConfig(CustomBaseModel):
    """Placeholder distribution config."""

    data: dict

    def materialize(
        self,
        *,
        output_file=None,
        labels_only=False,
        base=None,
        org=None,
        proj=None,
        token=None,
    ) -> dict:
        """Materialize distribution config."""
        return materialize_placeholders_config(
            dataset=self.data,
            output_file=output_file,
            labels_only=labels_only,
            base=base,
            org=org,
            proj=proj,
            token=token,
        )


class MModelConfigExpanded(CustomBaseModel):
    """Expanded config with json data instead of entity info."""

    variantDefinition: dict[VariantEntry, VariantInfo]
    defaults: dict[VariantEntry, dict]
    configuration: dict[VariantEntry, dict]

    def split(self) -> tuple[PlaceholderDistributionConfig, CanonicalDistributionConfig]:
        """Split the canonical and placeholder defaults based on the configuration."""
        placeholders_dict, canonicals_dict = split_config(
            defaults=self.defaults,
            configuration=self.configuration,
            canonical_key=VariantEntry.topological,
            placeholder_key=VariantEntry.placeholder,
        )
        return (
            PlaceholderDistributionConfig(data=placeholders_dict),
            CanonicalDistributionConfig(data=canonicals_dict),
        )


class MModelConfigRaw(CustomBaseModel):
    """Morphology assignment config schema."""

    variantDefinition: dict[VariantEntry, VariantInfo]
    defaults: dict[VariantEntry, dict]
    configuration: dict[VariantEntry, dict[str, dict[str, dict]]]

    def expand(self, *, base=None, org=None, proj=None, token=None) -> MModelConfigExpanded:
        """Expand the resources in the defaults with their json contents."""
        defaults = {
            k: get_distribution_as_dict(get_entry_id(v), base=base, org=org, proj=proj, token=token)
            for k, v in self.defaults.items()
        }

        return MModelConfigExpanded(
            variantDefinition=self.variantDefinition,
            defaults=defaults,
            configuration=self.configuration,
        )
