import os
import re
import json
import pytest
from pathlib import Path
from cwl_luigi import parser as tested
from cwl_luigi import cwl
from cwl_luigi.exceptions import CWLError, CWLValidationError
from cwl_luigi.executor import LocalExecutor, SallocExecutor
from cwl_luigi import utils


TESTS_DIR = Path(__file__).parent

DATA_DIR = TESTS_DIR / "data"
WORKFLOW_CAT_ECHO_DIR = DATA_DIR / "cat-echo"


def test_parse_cwl_data__raises_missing_class():
    with pytest.raises(CWLError, match="Missing 'class' in cwl data."):
        tested.parse_cwl_data({}, base_dir=None)


def test_parse_cwl_data__raises_unknown_class():
    with pytest.raises(TypeError, match="Unknown class"):
        tested.parse_cwl_data({"class": "Foo"}, base_dir=None)


def test_parse_io_parameters__no_outputs():
    cwl_data = {}
    outputs = tested._parse_io_parameters(cwl_data)
    assert outputs == {}


@pytest.mark.parametrize(
    "value,expected",
    [
        ("foo", "foo"),
        ("foo[]", {"type": "array", "items": "foo"}),
        ("foo?", ["foo", None]),
    ],
)
def test_preprocess_type(value, expected):
    res = tested._preprocess_type(value)
    assert res == expected


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


def _test_dataclass_instance(obj, expected_attributes):
    assert obj.to_dict() == expected_attributes, (
        f"dataclass: {obj}\n" f"Expected attrs: {expected_attributes}"
    )


def test_parse_config():
    config_file = DATA_DIR / "config.yml"

    obj = tested.parse_config_file(config_file)

    _test_dataclass_instance(
        obj,
        {
            "inputs": {
                "spam": {"type": "File", "value": "/foo"},
                "ham": {"type": "Directory", "value": "/bar"},
                "eggs": {"type": "string", "value": "foo"},
                "monty": {"type": "NexusType", "value": "foo"},
            }
        },
    )


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            "bar",
            {"type": "string", "value": "bar"},
        ),
        (
            {"class": "File", "path": "path1"},
            {"type": "File", "value": "path1"},
        ),
        (
            {"class": "Directory", "path": "path2"},
            {"type": "Directory", "value": "path2"},
        ),
    ],
)
def test_build_config_input(data, expected):
    obj = tested._build_config_input(data)
    _test_dataclass_instance(obj, expected)


def test_build_workflow_run():
    filepath = WORKFLOW_CAT_ECHO_DIR / "cat.cwl"

    res1 = tested._build_workflow_run(filepath, None)

    res2 = tested._build_workflow_run(utils.load_yaml(filepath), filepath.parent)

    assert res1 == res2


@pytest.fixture
def tool_cat():
    filepath = WORKFLOW_CAT_ECHO_DIR / "cat.cwl"
    return tested.parse_cwl_file(filepath)


@pytest.mark.parametrize(
    "attribute, expected_value",
    [
        ("cwlVersion", "v1.2"),
        ("id", "cat-command"),
        ("label", None),
        ("baseCommand", ["cat"]),
        (
            "inputs",
            {
                "f0": cwl.CommandInputParameter(
                    id="f0",
                    type="File",
                    inputBinding=cwl.CommandLineBinding(position=1, prefix=None),
                ),
                "f1": cwl.CommandInputParameter(
                    id="f1",
                    type="File",
                    inputBinding=cwl.CommandLineBinding(position=2, prefix=None),
                ),
            },
        ),
        (
            "outputs",
            {
                "cat_out": cwl.CommandOutputParameter(
                    id="cat_out", type="File", doc=None, outputBinding={"glob": "output.txt"}
                )
            },
        ),
    ],
)
def test_CommandLineTool__attributes(tool_cat, attribute, expected_value):
    value = getattr(tool_cat, attribute)
    assert value == expected_value


def workflow_cat_echo():
    workflow_file = WORKFLOW_CAT_ECHO_DIR / "workflow-cat-echo.cwl"

    with utils.cwd(workflow_file.parent):
        return tested.parse_cwl_file(workflow_file)


def test_workflow__attributes():
    workflow = workflow_cat_echo()

    assert workflow.cwlVersion == "v1.2"
    assert workflow.id == "cat-echo"
    assert workflow.label == "make-some-files"


def test_workflow__methods():
    workflow = workflow_cat_echo()
    assert workflow.step_names() == ["m0", "m1", "m2", "c0", "c1", "d0"]

    step = workflow.get_step_by_name("m2")
    assert step.id == "m2"

    with pytest.raises(ValueError, match="Not found: asdf"):
        workflow.get_step_by_name("asdf")


def test_workflow__inputs():
    workflow = workflow_cat_echo()

    expected_outputs = {
        "msg0": {"id": "msg0", "type": "string", "label": None, "default": None, "doc": None},
        "msg1": {"id": "msg1", "type": "string", "label": None, "default": None, "doc": None},
        "msg2": {"id": "msg2", "type": "string", "label": None, "default": None, "doc": None},
    }

    assert workflow.inputs.keys() == expected_outputs.keys()

    for name, out in expected_outputs.items():
        obj = workflow.inputs[name]
        assert isinstance(obj, cwl.WorkflowInputParameter)
        _test_dataclass_instance(obj, out)


def test_workflow__outputs():
    workflow = workflow_cat_echo()

    expected_outputs = [
        {"id": "output1", "type": "File", "outputSource": "c0/cat_out", "doc": None, "label": None},
        {"id": "output2", "type": "File", "outputSource": "c1/cat_out", "doc": None, "label": None},
        {"id": "output3", "type": "File", "outputSource": "d0/cat_out", "doc": None, "label": None},
    ]

    for out in expected_outputs:
        obj = workflow.outputs[out["id"]]
        assert isinstance(obj, cwl.WorkflowOutputParameter)
        _test_dataclass_instance(obj, out)


def test_workflow__steps():
    def _test_step_inputs(inputs, expected_inputs):
        for name, obj in inputs.items():
            assert isinstance(obj, cwl.CommandInputParameter)
            _test_dataclass_instance(obj, expected_inputs[name])

    def _test_step_outputs(outputs, expected_outputs):
        for name, obj in outputs.items():
            expected_output = expected_outputs[name]

            if name == "stdout":
                assert obj == expected_output
            else:
                assert isinstance(obj, cwl.CommandOutputParameter)
                _test_dataclass_instance(obj, expected_output)

    def _test_CommandLineTool(step, expected_run):
        assert isinstance(step.run, cwl.CommandLineTool)
        assert step.run.cwlVersion == expected_run["cwlVersion"]
        assert step.run.id == expected_run["id"]
        assert step.run.label == expected_run["label"]
        assert step.run.baseCommand == expected_run["baseCommand"]

        _test_step_inputs(step.run.inputs, expected_run["inputs"])
        _test_step_outputs(step.run.outputs, expected_run["outputs"])

    workflow = workflow_cat_echo()

    expected_step_names = ["m0", "m1", "m2", "c0", "c1", "d0"]
    assert [s.id for s in workflow.steps] == expected_step_names

    expected_steps = {
        "m0": {
            "id": "m0",
            "inputs": {
                "message": {
                    "id": "message",
                    "source": "msg0",
                    "label": None,
                    "valueFrom": None,
                    "default": None,
                },
            },
            "outputs": ["example_file"],
            "run": {
                "cwlVersion": "v1.2",
                "id": "echo-command",
                "label": None,
                "baseCommand": [str(WORKFLOW_CAT_ECHO_DIR / "echo-and-write.py")],
                "inputs": {
                    "message": {
                        "id": "message",
                        "type": "string",
                        "inputBinding": {
                            "position": 1,
                            "prefix": None,
                            "itemSeparator": " ",
                            "separate": True,
                        },
                        "default": None,
                        "doc": None,
                        "label": None,
                    },
                },
                "outputs": {
                    "example_file": {
                        "id": "example_file",
                        "type": "File",
                        "doc": None,
                        "outputBinding": {"glob": "file-output.txt"},
                        "label": None,
                    },
                    "stdout": "output.txt",
                },
            },
        },
        "m1": {
            "id": "m1",
            "inputs": {
                "message": {
                    "id": "message",
                    "label": None,
                    "source": "msg1",
                    "valueFrom": None,
                    "default": None,
                },
            },
            "outputs": ["example_file"],
            "run": {
                "cwlVersion": "v1.2",
                "id": "echo-command",
                "label": None,
                "baseCommand": [str(WORKFLOW_CAT_ECHO_DIR / "echo-and-write.py")],
                "inputs": {
                    "message": {
                        "id": "message",
                        "type": "string",
                        "inputBinding": {
                            "position": 1,
                            "prefix": None,
                            "itemSeparator": " ",
                            "separate": True,
                        },
                        "default": None,
                        "doc": None,
                        "label": None,
                    },
                },
                "outputs": {
                    "example_file": {
                        "id": "example_file",
                        "type": "File",
                        "doc": None,
                        "outputBinding": {"glob": "file-output.txt"},
                        "label": None,
                    },
                    "stdout": "output.txt",
                },
            },
        },
        "m2": {
            "id": "m2",
            "inputs": {
                "message": {
                    "id": "message",
                    "label": None,
                    "source": "msg2",
                    "valueFrom": None,
                    "default": None,
                },
            },
            "outputs": ["example_file"],
            "run": {
                "cwlVersion": "v1.2",
                "id": "echo-command",
                "label": None,
                "baseCommand": [str(WORKFLOW_CAT_ECHO_DIR / "echo-and-write.py")],
                "inputs": {
                    "message": {
                        "id": "message",
                        "type": "string",
                        "inputBinding": {
                            "position": 1,
                            "prefix": None,
                            "itemSeparator": " ",
                            "separate": True,
                        },
                        "default": None,
                        "doc": None,
                        "label": None,
                    },
                },
                "outputs": {
                    "example_file": {
                        "id": "example_file",
                        "type": "File",
                        "doc": None,
                        "outputBinding": {"glob": "file-output.txt"},
                        "label": None,
                    },
                    "stdout": "output.txt",
                },
            },
        },
        "c0": {
            "id": "c0",
            "inputs": {
                "f0": {
                    "id": "f0",
                    "label": None,
                    "source": "m0/example_file",
                    "valueFrom": None,
                    "default": None,
                },
                "f1": {
                    "id": "f1",
                    "label": None,
                    "source": "m1/example_file",
                    "valueFrom": None,
                    "default": None,
                },
            },
            "outputs": ["cat_out"],
            "run": {
                "cwlVersion": "v1.2",
                "id": "cat-command",
                "label": None,
                "baseCommand": ["cat"],
                "inputs": {
                    "f0": {
                        "id": "f0",
                        "type": "File",
                        "inputBinding": {
                            "position": 1,
                            "prefix": None,
                            "itemSeparator": " ",
                            "separate": True,
                        },
                        "default": None,
                        "doc": None,
                        "label": None,
                    },
                    "f1": {
                        "id": "f1",
                        "type": "File",
                        "inputBinding": {
                            "position": 2,
                            "prefix": None,
                            "itemSeparator": " ",
                            "separate": True,
                        },
                        "default": None,
                        "doc": None,
                        "label": None,
                    },
                },
                "outputs": {
                    "cat_out": {
                        "id": "cat_out",
                        "type": "File",
                        "doc": None,
                        "outputBinding": {"glob": "output.txt"},
                        "label": None,
                    },
                    "stdout": "output.txt",
                },
            },
        },
        "c1": {
            "id": "c1",
            "inputs": {
                "f0": {
                    "id": "f0",
                    "label": None,
                    "source": "m1/example_file",
                    "valueFrom": None,
                    "default": None,
                },
                "f1": {
                    "id": "f1",
                    "label": None,
                    "source": "m2/example_file",
                    "valueFrom": None,
                    "default": None,
                },
            },
            "outputs": ["cat_out"],
            "run": {
                "cwlVersion": "v1.2",
                "id": "cat-command",
                "label": None,
                "baseCommand": ["cat"],
                "inputs": {
                    "f0": {
                        "id": "f0",
                        "type": "File",
                        "inputBinding": {
                            "position": 1,
                            "prefix": None,
                            "itemSeparator": " ",
                            "separate": True,
                        },
                        "default": None,
                        "doc": None,
                        "label": None,
                    },
                    "f1": {
                        "id": "f1",
                        "type": "File",
                        "inputBinding": {
                            "position": 2,
                            "prefix": None,
                            "itemSeparator": " ",
                            "separate": True,
                        },
                        "default": None,
                        "doc": None,
                        "label": None,
                    },
                },
                "outputs": {
                    "cat_out": {
                        "id": "cat_out",
                        "type": "File",
                        "doc": None,
                        "outputBinding": {"glob": "output.txt"},
                        "label": None,
                    },
                    "stdout": "output.txt",
                },
            },
        },
        "d0": {
            "id": "d0",
            "inputs": {
                "f0": {
                    "id": "f0",
                    "label": None,
                    "source": "c0/cat_out",
                    "valueFrom": None,
                    "default": None,
                },
                "f1": {
                    "id": "f1",
                    "label": None,
                    "source": "c1/cat_out",
                    "valueFrom": None,
                    "default": None,
                },
            },
            "outputs": ["cat_out"],
            "run": {
                "cwlVersion": "v1.2",
                "id": "cat-command",
                "label": None,
                "baseCommand": ["cat"],
                "inputs": {
                    "f0": {
                        "id": "f0",
                        "type": "File",
                        "inputBinding": {
                            "position": 1,
                            "prefix": None,
                            "itemSeparator": " ",
                            "separate": True,
                        },
                        "default": None,
                        "doc": None,
                        "label": None,
                    },
                    "f1": {
                        "id": "f1",
                        "type": "File",
                        "inputBinding": {
                            "position": 2,
                            "prefix": None,
                            "itemSeparator": " ",
                            "separate": True,
                        },
                        "default": None,
                        "doc": None,
                        "label": None,
                    },
                },
                "outputs": {
                    "cat_out": {
                        "id": "cat_out",
                        "type": "File",
                        "doc": None,
                        "label": None,
                        "outputBinding": {"glob": "output.txt"},
                        "label": None,
                    },
                    "stdout": "output.txt",
                },
            },
        },
    }

    for step in workflow.steps:
        expected = expected_steps[step.id]
        assert isinstance(step, cwl.WorkflowStep)
        assert step.id == expected["id"]

        assert {k: v.to_dict() for k, v in step.inputs.items()} == expected["inputs"]
        assert step.outputs == expected["outputs"]

        _test_CommandLineTool(step, expected["run"])


def test_workflow_embedded():
    workflow1 = tested.parse_cwl_file(DATA_DIR / "use_cases/copy_file_chain_embedded.cwl")
    workflow2 = tested.parse_cwl_file(DATA_DIR / "use_cases/copy_file_chain.cwl")
    assert workflow1.to_dict() == workflow2.to_dict()


def test_workflow_broken():

    expected_errors = {
        "s0": [
            {
                "type": "OutputNotInWorkflowInputs",
                "output": "wrong_ref_file",
                "workflow_inputs": ["input_file", "output_dir", "overwrite"],
            }
        ],
        "s1": [
            {
                "type": "InvalidInputSourceOutput",
                "source": "s0",
                "output": "wrong_out_file",
                "source_outputs": ["output_file"],
            }
        ],
        "s2": [
            {
                "type": "InvalidInputSource",
                "source": "wrong_source",
                "output": "output_file",
                "workflow_sources": ["s0", "s1", "s2"],
            }
        ],
    }

    expected_json_str = json.dumps(expected_errors, indent=2)

    with pytest.raises(CWLValidationError, match=re.escape(expected_json_str)):
        tested.parse_cwl_file(DATA_DIR / "use_cases/copy_file_chain_broken.cwl")


def test_build_executor():

    data = {"executor": None}

    res = tested._build_executor(data)
    assert isinstance(res, LocalExecutor)

    data = {"executor": {"type": "local", "env_vars": {"foo": "bar"}}}
    res = tested._build_executor(data)

    assert isinstance(res, LocalExecutor)
    assert res.env_vars == {"foo": "bar"}

    data = {
        "executor": {
            "type": "slurm",
            "env_vars": {"foo": "bar"},
            "remote_config": {"host": "foo"},
            "slurm_config": {"nodes": 1},
        }
    }
    res = tested._build_executor(data)
    assert isinstance(res, SallocExecutor)
    assert res.env_vars == {"foo": "bar"}
    assert res.remote_config.host == "foo"
    assert res.slurm_config.nodes == 1

    data = {
        "executor": {
            "type": "Foo",
        }
    }

    with pytest.raises(TypeError, match="Unknown executor type"):
        tested._build_executor(data)


def test_dict_to_list_entries():

    dataset = {"a": {"foo": "bar"}, "b": {"spam": "zee"}}

    res = tested._dict_to_list_entries(dataset)

    assert res == [{"id": "a", "foo": "bar"}, {"id": "b", "spam": "zee"}]

    assert tested._dict_to_list_entries(res) == res
