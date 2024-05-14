import pytest

from blue_cwl.core.common import CustomBaseModel
from blue_cwl.core.exceptions import CWLError


class TestModel(CustomBaseModel):

    a: str
    b: str | None = None
    c: list[str]


def test_custom_base_model():

    model = TestModel(a="foo", c=["foo", "bar"])

    model_dict = model.to_dict()
    assert model_dict == {"a": "foo", "b": None, "c": ["foo", "bar"]}

    with pytest.raises(CWLError):
        TestModel.from_dict({"a": "b"})

    model1 = TestModel.from_dict(model_dict)
    assert model == model1

    model_str = model.to_string()
    assert model_str == '{"a":"foo","b":null,"c":["foo","bar"]}'

    with pytest.raises(CWLError):
        TestModel.from_string('{"a": "b"}')

    model2 = model.from_string(model_str)
    assert model == model2
