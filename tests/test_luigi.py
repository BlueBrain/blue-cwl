import tempfile
from pathlib import Path

import pytest
import luigi
from cwl_luigi import cwl
from cwl_luigi import cwl_luigi as tested

from cwl_luigi import utils

DATA_DIR = Path(__file__).parent / "data"
WORKFLOW_CAT_ECHO_DIR = DATA_DIR / "cat-echo"


def _build_workflow(workflow_file, base_dir):

    with utils.cwd(WORKFLOW_CAT_ECHO_DIR):
        workflow = cwl.Workflow.from_cwl(WORKFLOW_CAT_ECHO_DIR / "workflow-cat-echo.cwl")

    nodes, edges = cwl.get_graph(workflow)

    tasks, mapping = tested.build_workflow(workflow, nodes, edges, base_dir=Path(base_dir))

    return tasks, mapping


def test_build_workflow__cat_echo__task_classes():

    with tempfile.TemporaryDirectory() as tdir:

        tdir = Path(tdir)

        tasks, mapping = _build_workflow(WORKFLOW_CAT_ECHO_DIR / "workflow-cat-echo.cwl", tdir)

        assert tasks.keys() == {"m0", "m1", "m2", "c0", "c1", "d0"}

        for task_name, task_class in tasks.items():
            assert task_class.__name__ == task_name
            assert task_class.BASE_DIR == str(tdir)
            assert hasattr(task_class, "mapping")

        m0 = tasks["m0"]
        assert m0.requires_ == []
        assert m0.outputs_.keys() == {"example_file", "STDOUT"}
        assert m0.cmd_ == str(WORKFLOW_CAT_ECHO_DIR / "echo-and-write.py {message}")

        m1 = tasks["m1"]
        assert m1.requires_ == []
        assert m1.outputs_.keys() == {"example_file", "STDOUT"}
        assert m1.cmd_ == str(WORKFLOW_CAT_ECHO_DIR / "echo-and-write.py {message}")

        m2 = tasks["m2"]
        assert m2.requires_ == []
        assert m2.outputs_.keys() == {"example_file", "STDOUT"}
        assert m2.cmd_ == str(WORKFLOW_CAT_ECHO_DIR / "echo-and-write.py {message}")

        c0 = tasks["c0"]
        assert set(c0.requires_) == {m1, m0}
        assert c0.outputs_.keys() == {"STDOUT"}
        assert c0.cmd_ == "cat {f0} {f1}"

        c1 = tasks["c1"]
        assert set(c1.requires_) == {m2, m1}
        assert c1.outputs_.keys() == {"STDOUT"}
        assert c1.cmd_ == "cat {f0} {f1}"

        d0 = tasks["d0"]
        assert set(d0.requires_) == {c0, c1}
        assert d0.outputs_.keys() == {"STDOUT"}
        assert d0.cmd_ == "cat {f0} {f1}"

        assert mapping == {
            "m0": {"message": "msg0"},
            "m1": {"message": "msg1"},
            "m2": {"message": "msg2"},
            "c0": {"f1": str(tdir / "m1/output.txt"), "f0": str(tdir / "m0/output.txt")},
            "c1": {"f0": str(tdir / "m1/output.txt"), "f1": str(tdir / "m2/output.txt")},
            "d0": {"f1": str(tdir / "c1/output.txt"), "f0": str(tdir / "c0/output.txt")},
        }


def test_build_workflow__cat_echo__task_instances():

    import json

    config = json.loads((WORKFLOW_CAT_ECHO_DIR / "config.json").read_bytes())

    with tempfile.TemporaryDirectory() as tdir:

        tdir = Path(tdir)

        tasks, mapping = _build_workflow(WORKFLOW_CAT_ECHO_DIR / "workflow-cat-echo.cwl", tdir)

        config = cwl.Config.from_cwl(WORKFLOW_CAT_ECHO_DIR / "config.json")

        mapping = tested.resolve_mapping_with_config(mapping, config)

        tasks = {name: task(mapping=mapping) for name, task in tasks.items()}

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
                "STDOUT": tdir / "m0/output.txt",
            },
            "m1": {
                "example_file": tdir / "m1/file-output.txt",
                "STDOUT": tdir / "m1/output.txt",
            },
            "m2": {
                "example_file": tdir / "m2/file-output.txt",
                "STDOUT": tdir / "m2/output.txt",
            },
            "c0": {
                "STDOUT": tdir / "c0/output.txt",
            },
            "c1": {
                "STDOUT": tdir / "c1/output.txt",
            },
            "d0": {
                "STDOUT": tdir / "d0/output.txt",
            },
        }
        for task_name, task in tasks.items():

            deps = task.requires()
            assert set(deps) == {tasks[dep_name] for dep_name in expected_dependencies[task_name]}

            outs = {name: Path(local_target.path) for name, local_target in task.output().items()}
            assert outs == expected_outputs[task_name]

            for name, path in expected_outputs[task_name].items():
                assert path.exists()
