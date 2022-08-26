"""CWL workflow construction module."""
import copy
import enum
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from cwl_luigi.exceptions import CWLError
from cwl_luigi.utils import load_json, load_yaml, rename_dict_inplace

L = logging.getLogger(__name__)


class CWLType(enum.Enum):
    """Common Workflow Language types.

    See https://www.commonwl.org/v1.2/Workflow.html#CWLType
    """

    NULL = enum.auto()
    BOOLEAN = enum.auto()
    INT = enum.auto()
    LONG = enum.auto()
    FLOAT = enum.auto()
    DOUBLE = enum.auto()
    STRING = enum.auto()
    FILE = enum.auto()
    DIRECTORY = enum.auto()
    STDOUT = enum.auto()

    @staticmethod
    def from_string(string_value: str):
        """Convert a string to its respective CWLType."""
        string_to_enum = {
            "null": CWLType.NULL,
            "boolean": CWLType.BOOLEAN,
            "int": CWLType.INT,
            "long": CWLType.LONG,
            "float": CWLType.FLOAT,
            "double": CWLType.DOUBLE,
            "string": CWLType.STRING,
            "File": CWLType.FILE,
            "Directory": CWLType.DIRECTORY,
            "stdout": CWLType.STDOUT,
        }
        if string_value not in string_to_enum:
            raise CWLError(
                f"Unknown type {string_value}. " f"Expected on of {list(string_to_enum.keys())}."
            )
        return string_to_enum[string_value]


@dataclass(frozen=True)
class ConfigInput:
    """Dataclass for config's input entries."""

    type: CWLType
    value: str

    @classmethod
    def from_cwl(cls, data):
        """Generate a config input from cwl config data."""
        if isinstance(data, str):
            return cls(type=CWLType.STRING, value=data)

        return cls(type=CWLType.from_string(data["class"]), value=data["path"])


@dataclass(frozen=True)
class Config:
    """Dataclass for cwl config."""

    inputs: Dict[str, ConfigInput]

    @classmethod
    def from_cwl(cls, cwl_file: Path):
        """Generate a config instance from a cwl config file."""
        data = load_json(cwl_file)

        inputs = {name: ConfigInput.from_cwl(data) for name, data in data["inputs"].items()}

        return cls(inputs=inputs)


def _parse_io_parameters(cwl_data: dict, io_type: str) -> dict:
    """Return inputs or outputs in dictionary format."""
    if io_type not in cwl_data:
        return {}

    data = cwl_data[io_type]

    if isinstance(data, list):
        return {entry["id"]: {k: v for k, v in entry.items() if k != "id"} for entry in data}

    return data


@dataclass(frozen=True)
class CommandLineToolInput:
    """Dataclass for a command line tool's input.

    Attributes:
        id: The name of the input.
        type: The type of the input.
        inputBinding: The prefix of the input, e.g. --region.
    """

    id: str
    type: CWLType
    inputBinding: Dict[str, Any]

    @classmethod
    def from_cwl(cls, name: str, data: Dict[str, str]):
        """Construct a CommandLineToolInput from cwl data."""
        return cls(
            id=name,
            type=CWLType.from_string(data["type"]),
            inputBinding=data["inputBinding"],
        )


@dataclass(frozen=True)
class CommandLineToolOutput:
    """Dataclass for a command line tool's output.

    Attributes:
        id: The unique identifier of this object.
        type: The type of the output parameter.
        doc: A documentation string for this object.
        outputBinding:
            Describes how to generate this output object based on the files produced by a
            CommandLineTool.
    """

    id: str
    type: CWLType
    doc: Optional[str]
    outputBinding: Dict[str, Any]  # glob: "bmo/me-type-property/nodes.h5"

    @classmethod
    def from_cwl(cls, name, data, stdout=None):
        """Construct a CommandLineToolOutput from cwl data."""
        data = copy.deepcopy(data)

        if "doc" not in data:
            data["doc"] = None

        # make stdout behave like a normal file
        if "outputBinding" not in data:
            assert stdout is not None
            data["outputBinding"] = {"glob": stdout}

        data["type"] = CWLType.from_string(data["type"])

        return cls(id=name, **data)


@dataclass(frozen=True)
class CommandLineTool:
    """Dataclass for a command line tool's output.

    Attributes:
        id: The unique identifier for this object.
        label: A short, human-readable label of this object.
        baseCommand: Specifies the program to execute.
        inputs: Defines the input parameters of the process.
        outputs: Defines the parameters representing the output of the process.
        stdout:
            Capture the command's standard output stream to a file written to the designated
            output directory.
    """

    cwlVersion: str  # v1.2
    id: str
    label: str
    baseCommand: str
    inputs: Dict[str, CommandLineToolInput]
    outputs: Dict[str, CommandLineToolOutput]
    stdout: str

    @classmethod
    def from_cwl(cls, cwl_path):
        """Construct a CommandLineTool from cwl data."""
        data = load_yaml(cwl_path)
        assert "cwlVersion" in data
        assert data["cwlVersion"] == "v1.2"
        assert data["class"] == "CommandLineTool"
        del data["class"]

        stdout = data.get("stdout")
        data["inputs"] = {
            k: CommandLineToolInput.from_cwl(k, v)
            for k, v in _parse_io_parameters(data, "inputs").items()
        }
        data["outputs"] = {
            k: CommandLineToolOutput.from_cwl(k, v, stdout)
            for k, v in _parse_io_parameters(data, "outputs").items()
        }
        # resolve local executables wrt cwl_path directory
        if data["baseCommand"].startswith("./"):
            data["baseCommand"] = str((Path(cwl_path).parent / data["baseCommand"]).resolve())

        return cls(id=str(cwl_path), label=data.get("label", ""), **data)

    def cmd(self):
        """Return the command for executing the command line tool."""
        prefix = {}
        positional = []
        for id_, i in self.inputs.items():
            if i.inputBinding is None:
                L.warning("No inputBinding: %s", i)

            inputBinding = i.inputBinding

            if "position" in inputBinding:
                positional.append(
                    (
                        int(inputBinding["position"]),
                        id_,
                    )
                )
            elif "prefix" in inputBinding:
                prefix[inputBinding["prefix"]] = id_
            else:
                L.warning("Unknown type: %s", inputBinding)
                assert False

        ret = [
            self.baseCommand,
        ]

        for k, v in prefix.items():
            ret.append(k)
            ret.append(f"{{{v}}}")

        for _, v in sorted(positional):
            ret.append(f"{{{v}}}")

        return ret


@dataclass(frozen=True)
class WorkflowInput:
    """Dataclass for a workflow's input.

    Attributes:
        id: The unique identifier for this object.
        type: Specify valid types of data that may be assigned to this parameter.
        label: A short, human-readable label of this object.
    """

    id: str
    type: CWLType
    label: Optional[str]

    @classmethod
    def from_cwl(cls, name: str, data: Dict[str, str]):
        """Construct a WorkflowInput from cwl data."""
        return cls(
            id=name,
            type=CWLType.from_string(data["type"]),
            label=data.get("label", ""),
        )


@dataclass(frozen=True)
class WorkflowOutput:
    """Dataclass for a workflow's output.

    Attributes:
        type: The type of the output.
        outputSource:
    """

    id: str
    type: CWLType
    outputSource: str

    @classmethod
    def from_cwl(cls, name: str, data: Dict[str, str]):
        """Construct a WorkflowOutput from cwl data."""
        return cls(
            id=name,
            type=CWLType.from_string(data["type"]),
            outputSource=data["outputSource"],
        )

    def source_step(self) -> str:
        """Return the step name in the output source."""
        return self.outputSource.split("/", maxsplit=1)[0]


@dataclass(frozen=False)
class WorkflowStep:
    """Dataclass for a workflow's step.

    Attributes:
        id: The unique identifier for this object.
        inputs: The inputs of the step.
        outputs: The outputs of the step.
        run: The comannd line tool executed by the step.
    """

    id: str
    inputs: Dict[str, str]
    outputs: List[str]
    run: CommandLineTool

    @classmethod
    def from_cwl(cls, data: Dict[str, str]):
        """Construct a WorkflowStep from cwl data."""
        data = copy.deepcopy(data)
        rename_dict_inplace(data, "in", "inputs")
        rename_dict_inplace(data, "out", "outputs")
        data["run"] = CommandLineTool.from_cwl(data["run"])
        return cls(**data)

    def get_input_name_by_target(self, target: str) -> str:
        """Return the input name, given its target value."""
        for k, v in self.inputs.items():
            if v == target:
                return k
        raise ValueError("Target does not exist in inputs.")


@dataclass
class Workflow:
    """Dataclass for an entire workflow.

    Attributes:
        cwlVersion: Version of the cwl specication.
        id: Name of the workflow.
        label: Description of the workflow.
    """

    cwlVersion: str
    id: str
    label: str

    # requirements:
    #  SubworkflowFeatureRequirement: {}

    inputs: Dict[str, WorkflowInput]
    outputs: Dict[str, WorkflowOutput]
    steps: List[WorkflowStep]

    def step_names(self) -> Set[str]:
        """Return the names of the workflow steps."""
        return {s.id for s in self.steps}

    @classmethod
    def from_cwl(cls, cwl_path):
        """Construct a Workflow from cwl data."""
        data = load_yaml(cwl_path)
        assert "cwlVersion" in data
        assert data["cwlVersion"] == "v1.2"
        assert data["class"] == "Workflow"

        inputs = {
            k: WorkflowInput.from_cwl(k, v) for k, v in _parse_io_parameters(data, "inputs").items()
        }
        outputs = {
            k: WorkflowOutput.from_cwl(k, v)
            for k, v in _parse_io_parameters(data, "outputs").items()
        }
        steps = [WorkflowStep.from_cwl(s) for s in data["steps"]]

        return cls(
            cwlVersion=data["cwlVersion"],
            id=data["id"],
            label=data["label"] if "label" in data else "",
            inputs=inputs,
            outputs=outputs,
            steps=steps,
        )

    def get_step_by_name(self, name):
        """Return the workflow step with given name."""
        for s in self.steps:
            if s.id == name:
                return s
        raise ValueError(f"Not found: {name}")


class CWLWorkflowType(enum.Enum):
    """Workflow node types."""

    INPUT = enum.auto()
    OUTPUT = enum.auto()
    STEP = enum.auto()


@dataclass(frozen=True)
class Node:
    """CWL graph node dataclass."""

    type: CWLWorkflowType


@dataclass(frozen=True)
class Edge:
    """CWL graph edge dataclass."""

    source: str
    target: str


def get_graph(workflow: Workflow):
    """Return the nodes and edges of the workflow."""
    nodes = {}
    edges = set()

    for name in workflow.inputs:
        assert name not in nodes, "duplicate name, input"
        nodes[name] = Node(type=CWLWorkflowType.INPUT)

    for s in workflow.steps:
        assert s.id not in nodes, "duplicate name, step"
        nodes[s.id] = Node(type=CWLWorkflowType.STEP)
        for v in s.inputs.values():
            edges.add(Edge(source=v.split("/", 1)[0], target=s.id))

    for name, output in workflow.outputs.items():

        if name in nodes:
            raise CWLError(f"Duplicate name '{name}' in outputs")

        nodes[name] = Node(type=CWLWorkflowType.OUTPUT)

        source = output.source_step()
        if source not in nodes:
            raise CWLError(f"'{source}' is not a workflow step.")

        edges.add(Edge(source=source, target=name))

    return nodes, edges
