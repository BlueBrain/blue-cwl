"""Common."""

import json

from pydantic import BaseModel, ConfigDict, ValidationError

from cwl_luigi.exceptions import CWLError


class CustomBaseModel(BaseModel):
    """Custom Model Config."""

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        validate_assignment=True,
        arbitrary_types_allowed=True,
    )

    @classmethod
    def from_dict(cls, data: dict, **kwargs):
        """Instantiate a model from a dict."""
        try:
            return cls.model_validate(data, **kwargs)
        except ValidationError as e:
            raise CWLError(json.dumps(e.errors(), indent=2)) from e

    def to_dict(self, **kwargs) -> dict:
        """Convert the object into a dict."""
        return self.model_dump(**kwargs)

    def to_string(self, **kwargs) -> str:
        """Serialize the object to JSON."""
        return self.model_dump_json(**kwargs)

    @classmethod
    def from_string(cls, serialized_data: str, **kwargs):
        """Deserialize object from JSON."""
        try:
            return cls.model_validate_json(serialized_data, **kwargs)
        except ValidationError as e:
            raise CWLError(json.dumps(e.errors(), indent=2)) from e
