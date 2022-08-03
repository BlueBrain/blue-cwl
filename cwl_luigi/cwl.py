import collections
import copy
from dataclasses import dataclass
from typing import Dict, List, Union

import yaml


def load_yaml(filepath):
    """Load from YAML file."""
    with open(filepath, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@dataclass
class CommandLineToolInput:
    id: str
    type: str
    inputBinding: Dict[str, str] # prefix: --region

    @classmethod
    def from_cwl(cls, name, data):
        return cls(id=name, **data)


@dataclass
class CommandLineToolOutput:
    id: str
    type: str
    doc: Union[str, None]
    outputBinding: Union[Dict[str, str], None] # glob: "bmo/me-type-property/nodes.h5"

    @classmethod
    def from_cwl(cls, name, data):
        #XXX handles this better; different type (CommandLineToolOutputWithStdout?) for stdout output?
        # different type seems gross
        data = copy.deepcopy(data)
        if 'doc' not in data:
            data['doc'] = None
        if 'outputBinding' not in data:
            data['outputBinding'] = None

        return cls(id=name, **data)


@dataclass
class CommandLineTool:
    cwlVersion: str # v1.2
    id: str
    label: str
    baseCommand: List[str]
    inputs: Dict[str, CommandLineToolInput]

    outputs: Dict[str, CommandLineToolOutput]

    stdout: ... # []

    @classmethod
    def from_cwl(cls, cwl_path):
        data = load_yaml(cwl_path)
        assert 'cwlVersion' in data
        assert data['cwlVersion'] == 'v1.2'
        assert data['class'] ==  'CommandLineTool'
        del data['class']

        data['inputs'] = {k: CommandLineToolInput.from_cwl(k, v)
                          for k,v in data['inputs'].items()}
        data['outputs'] = {k: CommandLineToolOutput.from_cwl(k, v)
                           for k,v in data['outputs'].items()}

        return cls(id=cwl_path,
                   label='',
                   **data
                   )

    def cmd(self):
        prefix = {}
        positional = []
        for id_, i in self.inputs.items():
            if i.inputBinding is None:
                L.warning('No inputBinding: %s', i)

            inputBinding = i.inputBinding

            if 'position' in inputBinding:
                positional.append((int(inputBinding['position']), id_, ))
            elif 'prefix' in inputBinding:
                prefix[inputBinding['prefix']] = id_
            else:
                L.warning('Unknown type: %s', inputBinding)
                assert False

        ret = [self.baseCommand, ]

        for k, v in prefix.items():
            ret.append(k)
            ret.append(f'{{{v}}}')

        for _, v in sorted(positional):
            ret.append(f'{{{v}}}')

        return ret

def rename_dict_inplace(data, old, new):
    data[new] = data[old]
    del data[old]


@dataclass
class WorkflowInput:
    id_: str
    type: str

    @classmethod
    def from_cwl(cls, data):
        rename_dict_inplace(data, 'id', 'id_')
        return cls(**data)


@dataclass
class WorkflowOutput:
    type: str
    outputSource: str

    @classmethod
    def from_cwl(cls, data):
        #rename_dict_inplace(data, 'id', 'id_')
        return cls(**data)

    def source_step(self):
        return self.outputSource.split('/', maxsplit=1)[0]


@dataclass
class WorkflowStep:
    id_: str
    in_: Dict[str, str]
    out: List[str]
    run: CommandLineTool

    @classmethod
    def from_cwl(cls, data):
        rename_dict_inplace(data, 'id', 'id_')
        rename_dict_inplace(data, 'in', 'in_')
        data['run'] = CommandLineTool.from_cwl(data['run'])
        return cls(**data)


@dataclass
class Workflow:
    cwlVersion: str
    id: str
    label: str

    #requirements:
    #  SubworkflowFeatureRequirement: {}

    inputs: List[WorkflowInput]
    outputs: Dict[str, WorkflowOutput]
    steps: List[WorkflowStep]

    def step_names(self):
        return {s.id_ for s in self.steps}

    @classmethod
    def from_cwl(cls, cwl_path):
        data = load_yaml(cwl_path)
        assert 'cwlVersion' in data
        assert data['cwlVersion'] == 'v1.2'
        assert data['class'] ==  'Workflow'

        inputs = [WorkflowInput.from_cwl(s) for s in data['inputs']]

        outputs = {k: WorkflowOutput.from_cwl(v) for k, v in data['outputs'].items()}

        steps = [WorkflowStep.from_cwl(s) for s in data['steps']]

        return cls(cwlVersion=data['cwlVersion'],
                   id=data['id'],
                   label=data['label'] if 'label' in data else '',
                   inputs=inputs,
                   outputs=outputs,
                   steps=steps,
                   )

    def get_step_by_name(self, name):
        for s in self.steps:
            if s.id_ == name:
                return s
        raise ValueError(f'Not found: {name}')


def get_graph(workflow):
    Node = collections.namedtuple('Node', 'type')
    Edge = collections.namedtuple('Edge', 'from_, to')

    nodes = {}
    edges = set()

    for i in workflow.inputs:
        assert i.id_ not in nodes, 'duplicate name, input'
        nodes[i.id_] = Node(type='input')

    for s in workflow.steps:
        assert s.id_ not in nodes, 'duplicate name, step'
        nodes[s.id_] = Node(type='step')
        for v in s.in_.values():
            edges.add(Edge(from_=v.split('/', 1)[0], to=s.id_))

    for name, output in workflow.outputs.items():
        assert name not in nodes, 'duplicate name, outputs'
        nodes[name] = Node(type='output')
        edges.add(Edge(from_=output.source_step(), to=name))

    return nodes, edges
