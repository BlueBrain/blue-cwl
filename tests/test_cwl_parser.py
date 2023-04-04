import os
import pytest
from pathlib import Path
from cwl_luigi import cwl_parser as tested
from cwl_luigi.exceptions import CWLError

TESTS_DIR = Path(__file__).parent


def test_parse_io_parameters__no_outputs():
    cwl_data = {}
    outputs = tested._parse_io_parameters(cwl_data)
    assert outputs == {}


def test_parse_io_parameters__outputs_as_list():
    cwl_data = [
        {
            "id": "entry1",
            "type": "type1",
        },
        {
            "id": "entry2",
            "type": "type2",
        },
    ]
    outputs = tested._parse_io_parameters(cwl_data)
    assert outputs == {
        "entry1": {"type": "type1"},
        "entry2": {"type": "type2"},
    }


def test_parse_io_parameters__outputs_as_dict():
    cwl_data = {
        "entry1": {"type": "type1"},
        "entry2": {"type": "type2"},
    }
    outputs = tested._parse_io_parameters(cwl_data)
    assert outputs == cwl_data


@pytest.mark.parametrize(
    "cmd, expected",
    [
        ("executable", ["executable"]),
        ("/absolute-executable", ["/absolute-executable"]),
        ("./relative-executable", ["/myabspath/relative-executable"]),
        ("executable with-subcommand", ["executable", "with-subcommand"]),
        ("/absolute-executable with-subcommand", ["/absolute-executable", "with-subcommand"]),
        (
            "./relative-executable with-subcommand",
            ["/myabspath/relative-executable", "with-subcommand"],
        ),
        (["executable"], ["executable"]),
        (["/absolute-executable"], ["/absolute-executable"]),
        (["./relative-executable"], ["/myabspath/relative-executable"]),
        (["executable", "with-subcommand"], ["executable", "with-subcommand"]),
        (["/absolute-executable", "with-subcommand"], ["/absolute-executable", "with-subcommand"]),
        (
            ["./relative-executable", "with-subcommand"],
            ["/myabspath/relative-executable", "with-subcommand"],
        ),
    ],
)
def test_parse_baseCommand(cmd, expected):
    res = tested._parse_baseCommand(cmd, base_dir=Path("/myabspath"))
    assert res == expected


def test_parse_baseCommand__unknown_type():
    with pytest.raises(CWLError, match="Unknown format type 'set' for baseCommand."):
        tested._parse_baseCommand({"cat", "X"}, base_dir=None)


def test_resolve_paths():
    base_dir = "/my/basedir"

    data = {
        "a": {"a": 1},
        "b": {"a": {"path": "bpath"}},
        "c": [
            {"a": {"b": {"path": "cabpath"}}},
            {"d": "e"},
            {"f": {"run": "cfpath"}},
        ],
    }

    res = tested._resolve_paths(data, base_dir=base_dir)

    assert res == {
        "a": {"a": 1},
        "b": {"a": {"path": "/my/basedir/bpath"}},
        "c": [
            {"a": {"b": {"path": "/my/basedir/cabpath"}}},
            {"d": "e"},
            {"f": {"run": "/my/basedir/cfpath"}},
        ],
    }

    res = tested._resolve_paths(data, base_dir=None)

    cwd = os.getcwd()

    assert res == {
        "a": {"a": 1},
        "b": {"a": {"path": f"{cwd}/bpath"}},
        "c": [
            {"a": {"b": {"path": f"{cwd}/cabpath"}}},
            {"d": "e"},
            {"f": {"run": f"{cwd}/cfpath"}},
        ],
    }
