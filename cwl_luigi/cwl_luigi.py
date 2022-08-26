"""CWL luigi tasks building module."""
import collections
import subprocess
import time
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Set

import click
import luigi

from cwl_luigi import cwl
from cwl_luigi.cwl import CWLType, CWLWorkflowType

INPUT_MAP = {
    "string": luigi.Parameter,
}

STDOUT = "STDOUT"


def build_luigi_task(step: cwl.WorkflowStep, dependencies: List, base_dir: Path):
    """Generate a luigi task class from a cwl workflow step."""
    # { generic tasks functions
    def requires(self):
        return [r(mapping=self.mapping) for r in self.requires_]

    def output(self):
        return self.outputs_

    def run(self):
        cwd = Path(self.BASE_DIR) / type(self).__name__
        cwd.mkdir(parents=True, exist_ok=True)

        fmt = self.mapping[type(self).__name__]
        cmd = [atom.format(**fmt) for atom in self.cmd_]

        # so we can see the command, and pretend it took a while
        click.secho(f"[{cwd}]: {cmd}", fg="red")
        time.sleep(0.5)

        res = subprocess.run(cmd, capture_output=True, cwd=cwd, check=True)

        if STDOUT in self.outputs_:
            with open(self.outputs_[STDOUT].path, "wb") as fd:
                fd.write(res.stdout)

    # }

    outputs = {}
    for k, v in step.run.outputs.items():
        if v.type == CWLType.FILE:
            path = base_dir / step.id / v.outputBinding["glob"]
            outputs[k] = luigi.LocalTarget(str(path.resolve()))
        elif v.type == CWLType.DIRECTORY:
            raise NotImplementedError

    if step.run.stdout:
        assert STDOUT not in outputs
        outputs[STDOUT] = luigi.LocalTarget(str(base_dir / step.id / step.run.stdout))

    class_ = type(
        step.id,
        (luigi.Task,),
        {
            "run": run,
            "requires": requires,
            "output": output,
            "cmd_": step.run.cmd(),
            "requires_": dependencies,
            "mapping": luigi.DictParameter(),
            "outputs_": outputs,
            "BASE_DIR": str(base_dir),
        },
    )
    return class_


def _find_nondependent_outputs(nodes, edges):
    """Find the nodes that are workflow outputs but are not used as inputs to other steps."""
    output_names = (name for name, node in nodes.items() if node.type == CWLWorkflowType.OUTPUT)

    ret = []

    for out in output_names:

        out_edges = (e for e in edges if e.target == out)

        is_dependent = False

        for e_out in out_edges:
            for e in edges:
                if e.source == e_out.source and nodes[e.target].type == CWLWorkflowType.STEP:
                    is_dependent = True
                    break
            if is_dependent:
                break

        if not is_dependent:
            ret.append(out)

    return ret


def _build_step_inputs(nodes, edges):

    needed_steps = []
    visited_nodes = set()
    to_find = collections.deque(_find_nondependent_outputs(nodes, edges))

    while to_find:

        name = to_find.popleft()

        needed_nodes = [e.source for e in edges if e.target == name]

        steps = [new for new in needed_nodes if nodes[new].type == CWLWorkflowType.STEP]

        for node in needed_nodes:
            if node not in visited_nodes:
                to_find.append(node)
                visited_nodes.add(node)

        if nodes[name].type == CWLWorkflowType.STEP:
            needed_steps.append((name, steps))

    return needed_steps


def _build_inputs_mapping(workflow, nodes, edges, base_dir):

    mapping = collections.defaultdict(dict)
    for edge in edges:
        src_node = nodes[edge.source]
        dst_node = nodes[edge.target]

        if src_node.type == CWLWorkflowType.INPUT:
            assert dst_node.type == CWLWorkflowType.STEP
            target = workflow.get_step_by_name(edge.target)
            mapping[edge.target][target.get_input_name_by_target(edge.source)] = edge.source
        elif src_node.type == CWLWorkflowType.STEP:
            if dst_node.type == CWLWorkflowType.STEP:
                src = workflow.get_step_by_name(edge.source)
                dst = workflow.get_step_by_name(edge.target)

                # XXX assume single input for now
                step_output = src.outputs[0]

                input_name = f"{src.id}/{step_output}"
                path = src.run.outputs[step_output].outputBinding["glob"]
                path = str(Path(base_dir, f"{src.id}/{path}").resolve())
                mapping[edge.target][dst.get_input_name_by_target(input_name)] = path
            else:
                assert dst_node.type == CWLWorkflowType.OUTPUT, "step not leading to output"

    return dict(mapping)


def build_workflow(
    workflow: cwl.Workflow, nodes: Dict[str, cwl.Node], edges: Set[cwl.Edge], base_dir: Path
):
    """TBD."""
    needed_steps = _build_step_inputs(nodes, edges)

    mapping = _build_inputs_mapping(workflow, nodes, edges, base_dir)

    tasks = {}
    for name, depency_names in reversed(needed_steps):

        deps = [tasks[d] for d in depency_names]
        step = workflow.get_step_by_name(name)
        tasks[name] = build_luigi_task(step, deps, base_dir)

    return tasks, mapping


def resolve_mapping_with_config(mapping, config: cwl.Config):
    """Return a copy of mapping, with its values resolved from config.

    Args:
        mapping: Dictionary with task to inputs mapping.
        config: The config with the inputs for instantiating the tasks.
    """
    mapping = deepcopy(mapping)
    inputs = config.inputs

    for dependencies in mapping.values():
        for dep_name, dep_value in dependencies.items():
            if dep_value in inputs:
                dependencies[dep_name] = inputs[dep_value].value

    return mapping
