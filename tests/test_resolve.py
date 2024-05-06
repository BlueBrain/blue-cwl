import pytest
from cwl_luigi import resolve as test_module
from cwl_luigi.cwl_types import File, Directory


def test_resolve_parameter_references__no_references():
    string = "foo_bar.json"
    res = test_module.resolve_parameter_references(string, context={}, inputs={}, runtime={})
    assert res == string


@pytest.mark.parametrize(
    "cfg,expected",
    [
        (
            {
                "expression": "foo_$(inputs.bar)",
                "inputs": {"bar": "bar"},
            },
            "foo_bar",
        ),
        (
            {
                "expression": "foo/$(self)",
                "context": "bar",
            },
            "foo/bar",
        ),
        (
            {
                "expression": "foo/$(self[0])/$(self[1])",
                "context": ["bar", "foo"],
            },
            "foo/bar/foo",
        ),
        (
            {
                "expression": "foo/$(self.path)",
                "context": File(path="bar.txt"),
            },
            "foo/bar.txt",
        ),
        (
            {
                "expression": "foo/$(self.path)",
                "context": Directory(path="bar"),
            },
            "foo/bar",
        ),
        (
            {
                "expression": "foo/$(self[0].path)/$(self[1].path)",
                "context": [File(path="bar.txt"), File(path="foo.txt")],
            },
            "foo/bar.txt/foo.txt",
        ),
        (
            {
                "expression": "foo/$(self[0])/$(self[1].path)",
                "context": ["foo", File(path="bar.txt")],
            },
            "foo/foo/bar.txt",
        ),
        (
            {
                "expression": "foo/$(self[0].path)/$(self[1].path)",
                "context": [Directory(path="bar"), Directory(path="foo")],
            },
            "foo/bar/foo",
        ),
        (
            {
                "expression": "foo_$(self.path)/$(inputs.bar)",
                "context": File(path="foo.txt"),
                "inputs": {"bar": "bar.txt"},
            },
            "foo_foo.txt/bar.txt",
        ),
    ],
)
def test_resolve_parameter_references(cfg, expected):
    res = test_module.resolve_parameter_references(
        cfg["expression"],
        inputs=cfg.get("inputs"),
        context=cfg.get("context"),
        runtime=cfg.get("runtime"),
    )
    assert res == expected


def test_resolve_matches_to_keys():
    matches = [
        "inputs.v1",
        "inputs.v2",
        "steps.v1.v2",
        "self",
        "self.path",
        "self[0]",
        "self[0].path",
    ]

    res = test_module._matches_to_keys(matches)

    assert res == {
        "inputs.v1": ["inputs", "v1"],
        "inputs.v2": ["inputs", "v2"],
        "steps.v1.v2": ["steps", "v1", "v2"],
        "self": ["self"],
        "self.path": ["self", "path"],
        "self[0]": ["self", 0],
        "self[0].path": ["self", 0, "path"],
    }


def test_find_reference_values():
    match_keys = {
        "inputs.v1": ["inputs", "v1"],
        "inputs.v2": ["inputs", "v2"],
        "steps.v1.v2": ["steps", "v1", "v2"],
    }

    references = {
        "inputs": {"v1": 1, "v2": 2, "v3": 3},
        "steps": {"v1": {"v2": 4}},
    }

    res = test_module._find_reference_values(match_keys, references)

    assert res == {
        "inputs.v1": 1,
        "inputs.v2": 2,
        "steps.v1.v2": 4,
    }
