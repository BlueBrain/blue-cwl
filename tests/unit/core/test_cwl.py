import os
import yaml
import json
import tempfile
import subprocess
from pathlib import Path
from contextlib import contextmanager

import luigi
from luigi.freezing import recursively_unfreeze
import pytest
from blue_cwl.core import cwl as tested
from blue_cwl.core.cwl_types import File, Directory, CWLType
from blue_cwl.core.exceptions import CWLError

from blue_cwl.core import utils
from blue_cwl.core import parse_cwl_file

DATA_DIR = Path(__file__).parent / "data"
WORKFLOW_CAT_ECHO_DIR = DATA_DIR / "cat-echo"


def _test_dataclass_instance(obj, expected_attributes):
    assert obj.to_dict() == expected_attributes, (
        f"dataclass: {obj}\n" f"Expected attrs: {expected_attributes}"
    )


def test_workflow_step_output_parameter():

    p = tested.WorkflowOutputParameter(
        id="foo",
        type="File",
        label="foo",
        doc="foo",
        outputSource="bar/out",
    )

    assert p.split_source_output() == ["bar", "out"]


def test_workflow_step_input():

    p = tested.WorkflowStepInput(
        id="foo",
        source="bar/out",
    )

    assert p.split_source_output() == [("bar", "out")]

    p = tested.WorkflowStepInput(
        id="foo",
        source="out",
    )

    assert p.split_source_output() == [(None, "out")]

    p = tested.WorkflowStepInput(
        id="foo",
        source=["bar/out", "out"],
    )

    assert p.split_source_output() == [("bar", "out"), (None, "out")]

    p = tested.WorkflowStepInput(
        id="foo",
        source=None,
    )
    assert p.split_source_output() is None

    p = tested.WorkflowStepInput(
        id="foo",
        source="a/b/c",
    )

    with pytest.raises(ValueError):
        p.split_source_output()


@contextmanager
def cwd(path):
    """Context manager to temporarily change the working directory."""
    original_cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(original_cwd)


@pytest.fixture
def tool():
    return tested.CommandLineTool(
        cwlVersion="v1.2",
        id="my-id",
        label=None,
        baseCommand=["foo"],
        inputs={
            "f0": tested.CommandInputParameter(
                id="f0",
                type="File",
                inputBinding=tested.CommandLineBinding(position=1, prefix="--f0"),
            ),
            "f1": tested.CommandInputParameter(
                id="f1",
                type="File",
                inputBinding=tested.CommandLineBinding(position=3, prefix=None),
            ),
            "f2": tested.CommandInputParameter(
                id="f2",
                type="File",
                inputBinding=tested.CommandLineBinding(position=2, prefix=None),
            ),
            "f4": tested.CommandInputParameter(
                id="f4",
                type="File",
                inputBinding=tested.CommandLineBinding(position=1, prefix="--f4"),
            ),
        },
        outputs={},
        environment=None,
        executor=None,
    )


def test_CommandLineTool__dict(tool):
    data = tool.to_dict()
    new_tool = tested.CommandLineTool(**data)
    assert tool == new_tool


def test_CommandLineTool__json_serialization(tool):
    json_str = tool.to_string()
    data = json.loads(json_str)
    new_tool = tested.CommandLineTool.from_dict(data)
    assert tool == new_tool


def test_CommandLineTool__luigi_DictParameter(tool):
    data = tool.to_dict()

    p = luigi.DictParameter()
    string = p.serialize(data)
    new_data = recursively_unfreeze(p.normalize(p.parse(string)))

    new_tool = tested.CommandLineTool.from_dict(new_data)

    assert tool == new_tool


@pytest.fixture
def workflow():
    return parse_cwl_file(WORKFLOW_CAT_ECHO_DIR / "workflow-cat-echo.cwl")


def test_Workflow__repr(workflow):
    assert repr(workflow) == "Workflow(id=cat-echo)"


def test_Workflow__dict(workflow):
    data = workflow.to_dict()
    new_workflow = tested.Workflow.from_dict(data)
    assert workflow == new_workflow


def test_Workflow__json_serialization(workflow):
    json_str = workflow.to_string()
    data = json.loads(json_str)
    new_workflow = tested.Workflow.from_dict(data)
    assert workflow == new_workflow


def test_Workflow__luigi_DictParameter(workflow):
    data = workflow.to_dict()

    p = luigi.DictParameter()
    string = p.serialize(data)
    new_data = recursively_unfreeze(p.normalize(p.parse(string)))

    new_workflow = tested.Workflow.from_dict(new_data)

    assert workflow == new_workflow


def test_Workflow__make_workflow_step(workflow):

    res = workflow.make_workflow_step("m0", {"msg0": "foo", "msg1": "bar", "msg2": "spam"}, {})

    assert res.base_command is not None
