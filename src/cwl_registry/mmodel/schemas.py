"""Schemas."""
import hashlib
import json
from enum import Enum
from pathlib import Path

from pydantic import BaseModel

from cwl_registry import nexus
from cwl_registry.mmodel.config import split_config
from cwl_registry.mmodel.staging import (
    materialize_canonical_config,
    materialize_placeholders_config,
)
from cwl_registry.staging import get_entry_id


class VariantEntry(str, Enum):
    """Morphology assignment variants."""

    topological = "topological_synthesis"
    placeholder = "placeholder_assignment"


class VariantInfo(BaseModel):
    """Variant entry information."""

    algorithm: str
    version: str


class SynthesisOverrides(BaseModel):
    """Synthesis inputs overrides."""

    total_extent: float | None = None
    randomness: float | None = None
    orientation: list | None = None
    step_size: dict[str, dict[str, float]] | None = None
    radius: float | None = None


class CanonicalMorphologyModel(BaseModel):
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
                    k: v for k, v in neurite_overrides.dict().items() if v is not None
                }
                if neurite_overrides:
                    overrides[neurite_type] = neurite_overrides
            if overrides:
                filehash.update(json.dumps(overrides, sort_keys=True).encode())

        return filehash.hexdigest()

    def __eq__(self, other):
        """Return true if the two objects have the same checksum."""
        return self.checksum() == other.checksum()


class CanonicalDistributionConfig(BaseModel):
    """Canonical distribution config."""

    data: dict

    def materialize(self, forge, output_file=None, labels_only=False) -> dict:
        """Materialize distribution config."""
        return materialize_canonical_config(
            dataset=self.data,
            forge=forge,
            model_class=CanonicalMorphologyModel,
            output_file=output_file,
            labels_only=labels_only,
        )


class PlaceholderDistributionConfig(BaseModel):
    """Placeholder distribution config."""

    data: dict

    def materialize(self, forge, output_file=None, labels_only=False) -> dict:
        """Materialize distribution config."""
        return materialize_placeholders_config(
            dataset=self.data,
            forge=forge,
            output_file=output_file,
            labels_only=labels_only,
        )


class MModelConfigExpanded(BaseModel):
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


class MModelConfigRaw(BaseModel):
    """Morphology assignment config schema."""

    variantDefinition: dict[VariantEntry, VariantInfo]
    defaults: dict[VariantEntry, dict]
    configuration: dict[VariantEntry, dict[str, dict[str, dict]]]

    def expand(self, forge) -> MModelConfigExpanded:
        """Expand the resources in the defaults with their json contents."""
        # TODO: Switch forge with direct requests
        defaults = {
            k: nexus.read_json_file_from_resource_id(forge, get_entry_id(v))
            for k, v in self.defaults.items()
        }

        return MModelConfigExpanded(
            variantDefinition=self.variantDefinition,
            defaults=defaults,
            configuration=self.configuration,
        )
