"""Custom Base Model for pydantic classes."""
import pydantic

_PYDANTIC_MAJOR = int(pydantic.__version__.split(".", maxsplit=1)[0])


class CustomBaseModel(pydantic.BaseModel):
    """Custom Model Config."""

    class Config:
        """Custom Model Config."""

        frozen = True
        smart_union = True
        extra = pydantic.Extra.forbid
        validate_assignment = True
        arbitrary_types_allowed = True

    def to_dict(self) -> dict:
        """Convert the object into a dict."""
        return self.model_dump() if _PYDANTIC_MAJOR > 1 else self.dict()

    def to_json(self) -> str:
        """Serialize the object to JSON."""
        return self.model_dump_json() if _PYDANTIC_MAJOR > 1 else self.json()
