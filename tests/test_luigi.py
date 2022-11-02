import tempfile
from pathlib import Path

import pytest
import luigi
from cwl_luigi import cwl
from cwl_luigi import cwl_luigi as tested

from cwl_luigi import utils

DATA_DIR = Path(__file__).parent / "data"
WORKFLOW_CAT_ECHO_DIR = DATA_DIR / "cat-echo"


@pytest.fixture
def task_cat_class():
    filepath = cwl.CommandLineTool.from_cwl(WORKFLOW_CAT_ECHO_DIR / "cat.cwl")
    task_class = tested.build_task(filepath, task_name="cat")
    return task_class


def test_build_task__cat_class(task_cat_class):

    assert task_cat_class.__name__ == "cat"
    assert task_cat_class.get_param_names() == ["output_dir", "f0", "f1"]


def test_build_task__cat_instance(task_cat_class):

    with tempfile.TemporaryDirectory() as tdir:

        file1 = Path(tdir, "file1.txt")
        file1.write_text("File1")

        file2 = Path(tdir, "file2.txt")
        file2.write_text("File2")

        task = task_cat_class(f0=file1, f1=file2, output_dir=tdir)

        assert task.get_task_inputs() == {"f0": file1, "f1": file2, "output_dir": Path(tdir)}
        assert task.requires() == {}
        assert task.output().keys() == {"cat_out"}
        assert task.command() == f"cat {file1} {file2}"

        task.run()


@pytest.fixture
def cat_echo_workflow():
    return cwl.Workflow.from_cwl(WORKFLOW_CAT_ECHO_DIR / "workflow-cat-echo.cwl")


def _check_expected_task_class_attributes(task_class):
    assert hasattr(task_class, "_command_template")
    assert hasattr(task_class, "_cwl_outputs")
    assert hasattr(task_class, "_task_parents")
    assert hasattr(task_class, "_internal_mapping")


def test_build_standalone_classes(cat_echo_workflow):
    """Classes should be generated like independent tasks with no upstream tasks."""
    task_classes = tested._build_standalone_classes(cat_echo_workflow)

    assert task_classes.keys() == {"m0", "m1", "m2", "c0", "c1", "d0"}

    for task_name, task_class in task_classes.items():
        assert task_class.__name__ == task_name
        _check_expected_task_class_attributes(task_class)

    for task_class_name in ["m0", "m1", "m2"]:

        task_class = task_classes[task_class_name]
        assert task_class._task_parents is None
        assert task_class.get_param_names() == ["output_dir", "message"]
        assert task_class._cwl_outputs.keys() == {"example_file", "example_stdout"}
        assert task_class._command_template == str(
            WORKFLOW_CAT_ECHO_DIR / "echo-and-write.py {message}"
        )

    for task_class_name in ["c0", "c1", "d0"]:

        task_class = task_classes[task_class_name]
        assert task_class._task_parents is None
        assert task_class.get_param_names() == ["output_dir", "f0", "f1"]
        assert task_class._cwl_outputs.keys() == {"cat_out"}
        assert task_class._command_template == "cat {f0} {f1}"


def test_build_inputs_mapping(cat_echo_workflow):

    internal, external = tested._build_inputs_mapping(cat_echo_workflow)

    # inputs from outside the dag, passed in via parameters
    assert external == {
        "m0": {"message": "msg0"},
        "m1": {"message": "msg1"},
        "m2": {"message": "msg2"},
    }

    # inputs from inside the dag, passed in via the parent outputs
    assert internal == {
        "c0": {
            "f0": ["m0", "example_stdout"],
            "f1": ["m1", "example_stdout"],
        },
        "c1": {
            "f0": ["m1", "example_stdout"],
            "f1": ["m2", "example_stdout"],
        },
        "d0": {
            "f0": ["c0", "cat_out"],
            "f1": ["c1", "cat_out"],
        },
    }


def test_build_dependent_classes(cat_echo_workflow):
    """Only the params form c0, c1, and c2 should be reduced because they are intermediate steps."""
    task_classes = tested._build_standalone_classes(cat_echo_workflow)

    internal, _ = tested._build_inputs_mapping(cat_echo_workflow)

    task_classes = tested._build_dependent_classes(task_classes, internal)

    assert task_classes.keys() == {"m0", "m1", "m2", "c0", "c1", "d0"}

    for task_name, task_class in task_classes.items():
        assert task_class.__name__ == task_name
        _check_expected_task_class_attributes(task_class)

    for task_class_name in ["m0", "m1", "m2"]:

        task_class = task_classes[task_class_name]
        assert task_class._task_parents is None
        assert task_class.get_param_names() == ["output_dir", "message"]
        assert task_class._cwl_outputs.keys() == {"example_file", "example_stdout"}
        assert task_class._command_template == str(
            WORKFLOW_CAT_ECHO_DIR / "echo-and-write.py {message}"
        )

    for task_class_name in ["c0", "c1", "d0"]:

        task_class = task_classes[task_class_name]
        assert task_class._task_parents is None
        assert task_class.get_param_names() == ["output_dir"]
        assert task_class._cwl_outputs.keys() == {"cat_out"}
        assert task_class._command_template == "cat {f0} {f1}"


def test_build_workflow__cat_echo():

    import json

    with tempfile.TemporaryDirectory() as tdir:

        tdir = Path(tdir).resolve()

        workflow = cwl.Workflow.from_cwl(WORKFLOW_CAT_ECHO_DIR / "workflow-cat-echo.cwl")
        config = cwl.Config.from_cwl(WORKFLOW_CAT_ECHO_DIR / "config.yml")

        tasks = tested.build_workflow(workflow, tdir, config)

        # run the tasks manually to avoid the luigi overhead
        for task_name in ["m0", "m1", "m2", "c0", "c1", "d0"]:
            tasks[task_name].run()

        expected_dependencies = {
            "m0": set(),
            "m1": set(),
            "m2": set(),
            "c0": {"m0", "m1"},
            "c1": {"m1", "m2"},
            "d0": {"c0", "c1"},
        }
        expected_outputs = {
            "m0": {
                "example_file": tdir / "m0/file-output.txt",
                "example_stdout": tdir / "m0/output.txt",
            },
            "m1": {
                "example_file": tdir / "m1/file-output.txt",
                "example_stdout": tdir / "m1/output.txt",
            },
            "m2": {
                "example_file": tdir / "m2/file-output.txt",
                "example_stdout": tdir / "m2/output.txt",
            },
            "c0": {
                "cat_out": tdir / "c0/output.txt",
            },
            "c1": {
                "cat_out": tdir / "c1/output.txt",
            },
            "d0": {
                "cat_out": tdir / "d0/output.txt",
            },
        }
        for task_name, task in tasks.items():

            deps = task.requires()
            assert set(deps) == set(expected_dependencies[task_name])

            outs = {name: Path(local_target.path) for name, local_target in task.output().items()}
            assert outs == expected_outputs[task_name]

            for name, path in expected_outputs[task_name].items():
                assert path.exists()
