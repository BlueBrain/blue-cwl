import yaml
import tempfile
import dataclasses
from pathlib import Path

import pytest
from cwl_luigi import cwl as tested
from cwl_luigi.cwl_types import CWLType
from cwl_luigi.exceptions import CWLError

from cwl_luigi import utils

DATA_DIR = Path(__file__).parent / "data"
WORKFLOW_CAT_ECHO_DIR = DATA_DIR / "cat-echo"


def _test_dataclass_instance(obj, expected_attributes):
    assert dataclasses.asdict(obj) == expected_attributes, (
        f"dataclass: {obj}\n" f"Expected attrs: {expected_attributes}"
    )


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            "bar",
            {"type": CWLType.STRING, "value": "bar"},
        ),
        (
            {"class": "File", "path": "path1"},
            {"type": CWLType.FILE, "value": "path1"},
        ),
        (
            {"class": "Directory", "path": "path2"},
            {"type": CWLType.DIRECTORY, "value": "path2"},
        ),
    ],
)
def test_ConfigInput(data, expected):
    obj = tested.ConfigInput.from_cwl(data)
    _test_dataclass_instance(obj, expected)


def test_Config():
    config_file = DATA_DIR / "config.yml"

    obj = tested.Config.from_cwl(config_file)

    _test_dataclass_instance(
        obj,
        {
            "inputs": {
                "spam": {"type": CWLType.FILE, "value": "/foo"},
                "ham": {"type": CWLType.DIRECTORY, "value": "/bar"},
                "eggs": {"type": CWLType.STRING, "value": "foo"},
            }
        },
    )


def test_CommandLineToolInput():
    data = {"type": "File", "inputBinding": {"prefix": "--region"}}
    obj = tested.CommandLineToolInput.from_cwl(name="John", data=data)

    expected = {
        "id": "John",
        "type": CWLType.FILE,
        "inputBinding": {"position": 0, "prefix": "--region"},
    }

    _test_dataclass_instance(obj, expected)


def test_CommandLineToolOutput():
    data = {
        "type": "Directory",
        "outputBinding": {"glob": "some/path"},
        "doc": "",
    }

    obj = tested.CommandLineToolOutput.from_cwl(name="John", data=data)

    expected = {
        "id": "John",
        "type": CWLType.DIRECTORY,
        "outputBinding": {"glob": "some/path"},
        "doc": "",
    }

    _test_dataclass_instance(obj, expected)


@pytest.fixture
def tool_cat():
    filepath = WORKFLOW_CAT_ECHO_DIR / "cat.cwl"
    return tested.CommandLineTool.from_cwl(filepath)


@pytest.mark.parametrize(
    "attribute, expected_value",
    [
        ("cwlVersion", "v1.2"),
        ("id", str(WORKFLOW_CAT_ECHO_DIR / "cat.cwl")),
        ("label", ""),
        ("baseCommand", ["cat"]),
        (
            "inputs",
            {
                "f0": tested.CommandLineToolInput(
                    id="f0",
                    type=CWLType.FILE,
                    inputBinding=tested.CommandLineBinding(position=1, prefix=None),
                ),
                "f1": tested.CommandLineToolInput(
                    id="f1",
                    type=CWLType.FILE,
                    inputBinding=tested.CommandLineBinding(position=2, prefix=None),
                ),
            },
        ),
        (
            "outputs",
            {
                "cat_out": tested.CommandLineToolOutput(
                    id="cat_out", type=CWLType.STDOUT, doc="", outputBinding={"glob": "output.txt"}
                )
            },
        ),
        ("stdout", "output.txt"),
    ],
)
def test_CommandLineTool__attributes(tool_cat, attribute, expected_value):
    value = getattr(tool_cat, attribute)
    assert value == expected_value


def test_CommandLineTool__assort_inputs():
    tool = tested.CommandLineTool(
        cwlVersion=None,
        id=None,
        label=None,
        baseCommand=None,
        inputs={
            "f0": tested.CommandLineToolInput(
                id="f0",
                type=CWLType.FILE,
                inputBinding=tested.CommandLineBinding(position=1, prefix="--f0"),
            ),
            "f1": tested.CommandLineToolInput(
                id="f1",
                type=CWLType.FILE,
                inputBinding=tested.CommandLineBinding(position=3, prefix=None),
            ),
            "f2": tested.CommandLineToolInput(
                id="f2",
                type=CWLType.FILE,
                inputBinding=tested.CommandLineBinding(position=2, prefix=None),
            ),
            "f4": tested.CommandLineToolInput(
                id="f4",
                type=CWLType.FILE,
                inputBinding=tested.CommandLineBinding(position=1, prefix="--f4"),
            ),
        },
        outputs={},
        stdout=None,
        environment=None,
    )

    positional, named = tool.assort_inputs()

    assert positional == [(3, "f1"), (2, "f2")]

    assert named == {"--f0": "f0", "--f4": "f4"}


def test_WorkflowInput():
    data = {"type": "string", "label": "Some label"}
    obj = tested.WorkflowInput.from_cwl(name="John", data=data)

    expected = {"id": "John", "type": CWLType.STRING, "label": data["label"]}
    _test_dataclass_instance(obj, expected)


def test_WorkflowOutput():
    data = {"type": "File", "outputSource": "A/file"}
    obj = tested.WorkflowOutput.from_cwl(name="John", data=data)

    expected = {"id": "John", "type": CWLType.FILE, "outputSource": data["outputSource"]}
    _test_dataclass_instance(obj, expected)

    assert obj.source_step() == "A"


def test_workflowStep():
    data = {
        "id": "John",
        "in": {
            "i1": "input1",
            "i2": "input2",
        },
        "out": ["o1", "o2"],
        "run": str(WORKFLOW_CAT_ECHO_DIR / "cat.cwl"),
    }

    obj = tested.WorkflowStep.from_cwl(data)

    assert obj.id == data["id"]

    assert obj.inputs["i1"] == "input1"
    assert obj.inputs["i2"] == "input2"

    assert obj.outputs == ["o1", "o2"]

    assert obj.run.cwlVersion == "v1.2"
    assert obj.run.baseCommand == ["cat"]

    assert obj.get_input_name_by_target("input1") == "i1"

    with pytest.raises(ValueError, match="Target does not exist in inputs."):
        assert obj.get_input_name_by_target("asdf")


def workflow_cat_echo():
    workflow_file = WORKFLOW_CAT_ECHO_DIR / "workflow-cat-echo.cwl"

    with utils.cwd(workflow_file.parent):
        return tested.Workflow.from_cwl(workflow_file)


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
        "msg0": {"id": "msg0", "type": CWLType.STRING, "label": ""},
        "msg1": {"id": "msg1", "type": CWLType.STRING, "label": ""},
        "msg2": {"id": "msg2", "type": CWLType.STRING, "label": ""},
    }

    assert workflow.inputs.keys() == expected_outputs.keys()

    for name, out in expected_outputs.items():
        obj = workflow.inputs[name]
        assert isinstance(obj, tested.WorkflowInput)
        _test_dataclass_instance(obj, out)


def test_workflow__outputs():
    workflow = workflow_cat_echo()

    expected_outputs = [
        {"id": "output1", "type": CWLType.FILE, "outputSource": "c0/cat_out"},
        {"id": "output2", "type": CWLType.FILE, "outputSource": "c1/cat_out"},
        {"id": "output3", "type": CWLType.FILE, "outputSource": "d0/cat_out"},
    ]

    for out in expected_outputs:
        obj = workflow.outputs[out["id"]]
        assert isinstance(obj, tested.WorkflowOutput)
        _test_dataclass_instance(obj, out)


def test_workflow__steps():
    def _test_step_inputs(inputs, expected_inputs):
        for name, obj in inputs.items():
            assert isinstance(obj, tested.CommandLineToolInput)
            _test_dataclass_instance(obj, expected_inputs[name])

    def _test_step_outputs(outputs, expected_outputs):
        for name, obj in outputs.items():
            expected_output = expected_outputs[name]

            if name == "stdout":
                assert obj == expected_output
            else:
                assert isinstance(obj, tested.CommandLineToolOutput)
                _test_dataclass_instance(obj, expected_output)

    def _test_CommandLineTool(step, expected_run):
        assert isinstance(step.run, tested.CommandLineTool)
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
            "inputs": {"message": "msg0"},
            "outputs": ["example_stdout"],
            "run": {
                "cwlVersion": "v1.2",
                "id": str(WORKFLOW_CAT_ECHO_DIR / "echo.cwl"),
                "label": "",
                "baseCommand": [str(WORKFLOW_CAT_ECHO_DIR / "echo-and-write.py")],
                "inputs": {
                    "message": {
                        "id": "message",
                        "type": CWLType.STRING,
                        "inputBinding": {"position": 1, "prefix": None},
                    },
                },
                "outputs": {
                    "example_stdout": {
                        "id": "example_stdout",
                        "type": CWLType.STDOUT,
                        "doc": "",
                        "outputBinding": {"glob": "output.txt"},
                    },
                    "example_file": {
                        "id": "example_file",
                        "type": CWLType.FILE,
                        "doc": "",
                        "outputBinding": {"glob": "file-output.txt"},
                    },
                    "stdout": "output.txt",
                },
            },
        },
        "m1": {
            "id": "m1",
            "inputs": {"message": "msg1"},
            "outputs": ["example_stdout"],
            "run": {
                "cwlVersion": "v1.2",
                "id": str(WORKFLOW_CAT_ECHO_DIR / "echo.cwl"),
                "label": "",
                "baseCommand": [str(WORKFLOW_CAT_ECHO_DIR / "echo-and-write.py")],
                "inputs": {
                    "message": {
                        "id": "message",
                        "type": CWLType.STRING,
                        "inputBinding": {"position": 1, "prefix": None},
                    },
                },
                "outputs": {
                    "example_stdout": {
                        "id": "example_stdout",
                        "type": CWLType.STDOUT,
                        "doc": "",
                        "outputBinding": {"glob": "output.txt"},
                    },
                    "example_file": {
                        "id": "example_file",
                        "type": CWLType.FILE,
                        "doc": "",
                        "outputBinding": {"glob": "file-output.txt"},
                    },
                    "stdout": "output.txt",
                },
            },
        },
        "m2": {
            "id": "m2",
            "inputs": {"message": "msg2"},
            "outputs": ["example_stdout"],
            "run": {
                "cwlVersion": "v1.2",
                "id": str(WORKFLOW_CAT_ECHO_DIR / "echo.cwl"),
                "label": "",
                "baseCommand": [str(WORKFLOW_CAT_ECHO_DIR / "echo-and-write.py")],
                "inputs": {
                    "message": {
                        "id": "message",
                        "type": CWLType.STRING,
                        "inputBinding": {"position": 1, "prefix": None},
                    },
                },
                "outputs": {
                    "example_stdout": {
                        "id": "example_stdout",
                        "type": CWLType.STDOUT,
                        "doc": "",
                        "outputBinding": {"glob": "output.txt"},
                    },
                    "example_file": {
                        "id": "example_file",
                        "type": CWLType.FILE,
                        "doc": "",
                        "outputBinding": {"glob": "file-output.txt"},
                    },
                    "stdout": "output.txt",
                },
            },
        },
        "c0": {
            "id": "c0",
            "inputs": {"f0": "m0/example_stdout", "f1": "m1/example_stdout"},
            "outputs": ["cat_out"],
            "run": {
                "cwlVersion": "v1.2",
                "id": str(WORKFLOW_CAT_ECHO_DIR / "cat.cwl"),
                "label": "",
                "baseCommand": ["cat"],
                "inputs": {
                    "f0": {
                        "id": "f0",
                        "type": CWLType.FILE,
                        "inputBinding": {"position": 1, "prefix": None},
                    },
                    "f1": {
                        "id": "f1",
                        "type": CWLType.FILE,
                        "inputBinding": {"position": 2, "prefix": None},
                    },
                },
                "outputs": {
                    "cat_out": {
                        "id": "cat_out",
                        "type": CWLType.STDOUT,
                        "doc": "",
                        "outputBinding": {"glob": "output.txt"},
                    },
                    "stdout": "output.txt",
                },
            },
        },
        "c1": {
            "id": "c1",
            "inputs": {"f0": "m1/example_stdout", "f1": "m2/example_stdout"},
            "outputs": ["cat_out"],
            "run": {
                "cwlVersion": "v1.2",
                "id": str(WORKFLOW_CAT_ECHO_DIR / "cat.cwl"),
                "label": "",
                "baseCommand": ["cat"],
                "inputs": {
                    "f0": {
                        "id": "f0",
                        "type": CWLType.FILE,
                        "inputBinding": {"position": 1, "prefix": None},
                    },
                    "f1": {
                        "id": "f1",
                        "type": CWLType.FILE,
                        "inputBinding": {"position": 2, "prefix": None},
                    },
                },
                "outputs": {
                    "cat_out": {
                        "id": "cat_out",
                        "type": CWLType.STDOUT,
                        "doc": "",
                        "outputBinding": {"glob": "output.txt"},
                    },
                    "stdout": "output.txt",
                },
            },
        },
        "d0": {
            "id": "d0",
            "inputs": {"f0": "c0/cat_out", "f1": "c1/cat_out"},
            "outputs": ["cat_out"],
            "run": {
                "cwlVersion": "v1.2",
                "id": str(WORKFLOW_CAT_ECHO_DIR / "cat.cwl"),
                "label": "",
                "baseCommand": ["cat"],
                "inputs": {
                    "f0": {
                        "id": "f0",
                        "type": CWLType.FILE,
                        "inputBinding": {"position": 1, "prefix": None},
                    },
                    "f1": {
                        "id": "f1",
                        "type": CWLType.FILE,
                        "inputBinding": {"position": 2, "prefix": None},
                    },
                },
                "outputs": {
                    "cat_out": {
                        "id": "cat_out",
                        "type": CWLType.STDOUT,
                        "doc": "",
                        "outputBinding": {"glob": "output.txt"},
                    },
                    "stdout": "output.txt",
                },
            },
        },
    }

    for step in workflow.steps:
        expected = expected_steps[step.id]
        assert isinstance(step, tested.WorkflowStep)
        assert step.id == expected["id"]

        assert step.inputs == expected["inputs"]
        assert step.outputs == expected["outputs"]

        _test_CommandLineTool(step, expected["run"])
