from unittest.mock import Mock
import pytest
from blue_cwl.core import process as test_module
from blue_cwl.core.cwl_types import File, Directory, NexusResource, CWLType
from blue_cwl.core import cwl
from blue_cwl.core.exceptions import CWLError


def test_concretize_inputs__defaults():

    Par = cwl.WorkflowInputParameter

    inputs = {
        "0": Par(id="i", type="string", default="default"),
        "1": Par(id="i", type="File", default="default"),
        "2": Par(id="i", type="File", default=File(path="default")),
        "3": Par(id="i", type="Directory", default="default"),
        "4": Par(id="i", type="Directory", default=Directory(path="default")),
        "5": Par(id="i", type="NexusType", default="default"),
        "6": Par(id="i", type="NexusType", default=File(path="default")),
        "7": Par(id="i", type="NexusType", default=NexusResource(id="default")),
        "8": Par(id="i", type="NexusType", default=NexusResource(path="default")),
        "9": Par(id="i", type="int", default=2),
        "10": Par(id="i", type="int", default="2"),
        "10": Par(id="i", type="int", default=2.0),
        "11": Par(id="i", type="int", default=2),
        "12": Par(id="i", type="float", default=3),
        "13": Par(id="i", type="float", default=3.0),
        "14": Par(id="i", type="float", default="3"),
        "15": Par(id="i", type="boolean", default=True),
    }

    res = test_module._concretize_inputs(inputs, {})

    assert res == {
        "0": "default",
        "1": File(path="default"),
        "2": File(path="default"),
        "3": Directory(path="default"),
        "4": Directory(path="default"),
        "5": NexusResource(id="default"),
        "6": NexusResource(path="default"),
        "7": NexusResource(id="default"),
        "8": NexusResource(path="default"),
        "9": 2,
        "10": 2,
        "11": 2,
        "12": 3.0,
        "13": 3.0,
        "14": 3.0,
        "15": True,
    }


def test_concretize_inputs__nondefaults():

    cases = [
        ("int", 0, 0),
        ("int", 0.0, 0),
        ("int", "0", 0),
        ("string", "foo", "foo"),
        ("string", 1, "1"),
        ("string", 1.0, "1.0"),
        ("File", "foo", File(path="foo")),
        ("Directory", "bar", Directory(path="bar")),
        ("NexusType", "foo", NexusResource(id="foo")),
        ("NexusType", File(path="foo"), NexusResource(path="foo")),
    ]

    inputs = {}
    input_values = {}
    expected_values = {}
    for n, (inp_type, inp_value, expected) in enumerate(cases):
        name = str(n)
        inputs[name] = cwl.WorkflowInputParameter(id=name, type=inp_type)
        input_values[name] = inp_value
        expected_values[name] = expected

    res = test_module._concretize_inputs(inputs, input_values)
    assert res == expected_values


def test_concretize_inputs__raises():

    inputs = {"a": cwl.WorkflowInputParameter(id="a", type="File")}

    with pytest.raises(CWLError):
        test_module._concretize_inputs(inputs, {})


def test_concretize_tool_outputs__raises():

    with pytest.raises(NotImplementedError):
        test_module._concretize_tool_outputs({"a": Mock(type="int")}, {})


def _array_type(type_):
    return {"type": "array", "items": type_}


@pytest.mark.parametrize(
    "input_type,input_value,expected",
    [
        ("int", 2, 2),
        (_array_type("int"), [1, 2], [1, 2]),
        ("long", 2, 2),
        (_array_type("long"), [1, 2], [1, 2]),
        ("float", 2.0, 2.0),
        (_array_type("float"), [1.0, 2.0], [1.0, 2.0]),
        ("double", 2.0, 2.0),
        (_array_type("double"), [1.0, 2.0], [1.0, 2.0]),
        ("string", "foo", "foo"),
        (_array_type("string"), ["foo", "bar"], ["foo", "bar"]),
        ("boolean", True, True),
        ("File", File(path="foo.txt"), File(path="foo.txt")),
        (
            _array_type("File"),
            [File(path="foo.txt"), File(path="bar.txt")],
            [File(path="foo.txt"), File(path="bar.txt")],
        ),
        ("Directory", Directory(path="foo"), Directory(path="foo")),
        (
            _array_type("Directory"),
            [Directory(path="foo"), Directory(path="bar")],
            [Directory(path="foo"), Directory(path="bar")],
        ),
        ("string", File(path="foo.txt"), "foo.txt"),
        ("string", Directory(path="foo"), "foo"),
        ("File", "foo.txt", File(path="foo.txt")),
        ("File", {"path": "foo.txt"}, File(path="foo.txt")),
        ("Directory", "foo", Directory(path="foo")),
        ("Directory", {"path": "foo"}, Directory(path="foo")),
        (
            _array_type("string"),
            [File(path="foo.txt"), File(path="bar.txt")],
            ["foo.txt", "bar.txt"],
        ),
        (_array_type("File"), ["foo.txt", "bar.txt"], [File(path="foo.txt"), File(path="bar.txt")]),
        (_array_type("Directory"), ["foo", "bar"], [Directory(path="foo"), Directory(path="bar")]),
        (
            _array_type("string"),
            ["foo", File(path="bar.txt"), Directory(path="baz")],
            ["foo", "bar.txt", "baz"],
        ),
        ("NexusType", File(path="foo.txt"), NexusResource(path="foo.txt")),
        ("NexusType", "foo", NexusResource(id="foo")),
        ("NexusType", NexusResource(path="foo.txt"), NexusResource(path="foo.txt")),
    ],
)
def test_input_value_to_object(input_type, input_value, expected):

    res = test_module._input_value_to_object(input_type, input_value)
    assert res == expected


@pytest.mark.parametrize(
    "input_type, input_value",
    [
        ("File", 0),
        ("Directory", 1.0),
        ("string", None),
        ("NexusType", 1),
    ],
)
def test_input_value_to_object__raises(input_type, input_value):

    with pytest.raises(ValueError):
        test_module._input_value_to_object(input_type, input_value)


def test_cmd_elements__raises():

    with pytest.raises(NotImplementedError):
        test_module._cmd_elements("Foo", None, None)
