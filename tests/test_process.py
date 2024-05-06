import pytest
from cwl_luigi import process as test_module
from cwl_luigi.cwl_types import File, Directory, CWLType


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
        ("Directory", "foo", Directory(path="foo")),
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
    ],
)
def test_input_value_to_object(input_type, input_value, expected):

    res = test_module._input_value_to_object(input_type, input_value)
    assert res == expected
