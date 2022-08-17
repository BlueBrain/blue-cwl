import collections
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Union

import click
import luigi

from cwl_luigi import cwl

INPUT_MAP = {
    "string": luigi.Parameter,
}

STDOUT = "STDOUT"


def build_luigi_task(
    step: cwl.WorkflowStep, dependencies: List, mapping: Dict[str, str], base_dir: Path
):

    # { generic tasks functions
    def requires(self):
        return [r(**mapping[r.__name__]) for r in self.requires_]

    def output(self):
        return self.outputs_

    def run(self):
        cwd = self.BASE_DIR / type(self).__name__
        cwd.mkdir(parents=True, exist_ok=True)

        format = {}
        for k, v in self.mapping_.items():
            if hasattr(self, v):
                format[k] = getattr(self, v)
            else:
                format[k] = Path("..") / v

        cmd = [atom.format(**format) for atom in self.cmd_]

        # so we can see the command, and pretend it took a while
        click.secho(f"[{cwd}]: {cmd}", fg="red")
        time.sleep(0.5)

        res = subprocess.run(cmd, capture_output=True, cwd=cwd)
        res.check_returncode()

        if STDOUT in self.outputs_:
            with open(self.outputs_[STDOUT].path, "wb") as fd:
                fd.write(res.stdout)

    # }

    # XXX(mgevaert): need to wire up parameters: these are the 'inputs' to the workflow
    # We probably want to keep them as luig.Parameters, so the caching and nexus
    # mechanisms work correctly (as I understand them)
    parameters = {name: luigi.Parameter() for name in step.in_}

    outputs = {}
    for k, v in step.run.outputs.items():
        if v.type == "File":
            outputs[k] = luigi.LocalTarget(str(base_dir / step.id_ / v.outputBinding["glob"]))

    if step.run.stdout:
        assert STDOUT not in outputs
        outputs[STDOUT] = luigi.LocalTarget(str(base_dir / step.id_ / step.run.stdout))

    class_ = type(
        step.id_,
        (luigi.Task,),
        {
            "run": run,
            "requires": requires,
            "output": output,
            "cmd_": step.run.cmd(),
            "requires_": dependencies,
            "outputs_": outputs,
            "mapping_": mapping[step.id_],
            "BASE_DIR": base_dir,
            **parameters,
        },
    )
    return class_


def build_workflow(workflow, nodes, edges, base_dir):
    needed_steps = []
    to_find = collections.deque(name for name, v in nodes.items() if v.type == "output")

    while to_find:
        name = to_find.pop()
        needed_nodes = [e.from_ for e in edges if e.to == name]
        steps = [new for new in needed_nodes if nodes[new].type == "step"]
        to_find.extend(needed_nodes)
        if nodes[name].type == "step":
            needed_steps.append((name, steps))

    mapping = collections.defaultdict(dict)
    for edge in edges:
        src_node = nodes[edge.from_]
        dst_node = nodes[edge.to]

        if src_node.type == "input":
            assert dst_node.type == "step"
            target = workflow.get_step_by_name(edge.to)
            mapping[edge.to][target.get_input_name_by_target(edge.from_)] = edge.from_
        elif src_node.type == "step":
            if dst_node.type == "step":
                src = workflow.get_step_by_name(edge.from_)
                dst = workflow.get_step_by_name(edge.to)

                # XXX assume single input for now
                step_output = src.out[0]

                input_name = f'{src.id_}/{step_output}'
                path = src.run.outputs[step_output].outputBinding["glob"]
                path = f'{src.id_}/{path}'
                mapping[edge.to][dst.get_input_name_by_target(input_name)] = path
            else:
                assert dst_node.type == "output", "step not leading to output"

    mapping = dict(mapping)

    tasks = {}
    for name, depency_names in reversed(needed_steps):
        deps = [tasks[d] for d in depency_names]
        step = workflow.get_step_by_name(name)
        tasks[name] = build_luigi_task(step, deps, mapping, base_dir)

    return tasks, mapping
