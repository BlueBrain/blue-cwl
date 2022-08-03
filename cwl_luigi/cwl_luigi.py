import collections
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Union

import click
import luigi

from cwl_luigi import cwl

INPUT_MAP = {
    'string': luigi.Parameter,
    }

STDOUT = 'STDOUT'


def build_luigi_task(step: cwl.WorkflowStep,
                     dependencies: List,
                     mapping,
                     base_dir
                     ):
    #{ generic tasks
    def requires(self):
        return [r(msg0=self.msg0) for r in self.requires_]

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
                format[k] = v

        cmd = [atom.format(**format) for atom in self.cmd_]

        #so we can see the command, and pretend it took a while
        click.secho(f'[{cwd}]: {cmd}', fg='red')
        time.sleep(0.5)

        res = subprocess.run(cmd, capture_output=True, cwd=cwd)
        res.check_returncode()

        if STDOUT in self.outputs_:
            with open(self.outputs_[STDOUT].path, 'wb') as fd:
                fd.write(res.stdout)
    #}

    # XXX: need to wire parameters
    parameters = {}
    parameters['msg0'] = luigi.Parameter()

    outputs = {}
    for k, v in step.run.outputs.items():
        if v.type == 'File':
            outputs[k] = luigi.LocalTarget(str(base_dir / step.id_ / v.outputBinding['glob']))

    if step.run.stdout:
        assert STDOUT not in outputs
        outputs[STDOUT] = luigi.LocalTarget(str(base_dir  / step.id_ / step.run.stdout))


    class_ = type(step.id_, (luigi.Task, object, ),
                  {'run': run,
                   'requires': requires,
                   'output': output,

                   'cmd_': step.run.cmd(),

                   'requires_': dependencies,
                   'outputs_': outputs,
                   'mapping_': mapping,
                   'BASE_DIR': base_dir,

                   **parameters,
                   }
                  )
    return class_


def build_workflow(workflow, nodes, edges, base_dir):
    needed_steps = []
    to_find = collections.deque(name
                                for name, v in nodes.items()
                                if v.type == 'output')

    while to_find:
        name = to_find.pop()
        needed_nodes = [e.from_ for e in edges if e.to == name]
        steps = [new for new in needed_nodes if nodes[new].type == 'step']
        to_find.extend(needed_nodes)
        if nodes[name].type == 'step':
            needed_steps.append((name, steps))

    import ipdb; ipdb.set_trace()  # XXX BREAKPOINT
    #XXX: how to handle base dir handling?????
    mapping = {'message': 'msg0',
               'f0': '../m0/output.txt',
               'f1': '../m1/output.txt',
               }

    tasks = {}
    for name, depency_names in reversed(needed_steps):
        deps = [tasks[d] for d in depency_names]
        tasks[name] = build_luigi_task(workflow.get_step_by_name(name),
                                       deps,
                                       mapping,
                                       base_dir)

    return tasks['c0']
