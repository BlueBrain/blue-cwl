"""CWL workflow construction module."""
import copy
import enum
import logging
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from cwl_luigi.command import build_command
from cwl_luigi.environment import Environment
from cwl_luigi.exceptions import CWLError
from cwl_luigi.utils import load_json, load_yaml, resolve_path

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
    def from_string(string_value: str) -> "CWLType":
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


@dataclass(frozen=True)
class Config:
    """Dataclass for cwl config."""

    inputs: Dict[str, ConfigInput]

    @classmethod
    def from_cwl(cls, cwl_file: Path) -> "Config":
        """Generate a config instance from a cwl config file."""
        data = load_json(cwl_file)

        inputs = {}
        for name, data_input in data["inputs"].items():

            if isinstance(data_input, str):
                inputs[name] = ConfigInput(type=CWLType.STRING, value=data_input)
            else:
                input_type = CWLType.from_string(data_input["class"])
                if input_type in {CWLType.FILE, CWLType.DIRECTORY}:
                    path = str(resolve_path(data_input["path"], Path(cwl_file).parent))
                    inputs[name] = ConfigInput(type=input_type, value=path)
                else:
                    raise NotImplementedError
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
    def from_cwl(cls, name: str, data: Dict[str, Any]) -> "CommandLineToolInput":
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
    def from_cwl(cls, name, data, stdout=None) -> "CommandLineToolOutput":
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


def _parse_base_command(raw: Union[str, List[str]], base_dir: Path) -> List[str]:

    base_command = deepcopy(raw)

    if isinstance(base_command, str):
        base_command = base_command.split()

    if not isinstance(base_command, list):
        raise CWLError(
            f"Unknown format type {type(base_command)} for baseCommand. "
            "Expected either str or list."
        )

    # resolve local executables wrt cwl_path directory
    if base_command[0].startswith("./"):
        base_command[0] = str((base_dir / base_command[0]).resolve())

    return base_command


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
    baseCommand: List[str]
    inputs: Dict[str, CommandLineToolInput]
    outputs: Dict[str, CommandLineToolOutput]
    stdout: str
    environment: Optional[Environment]

    @classmethod
    def from_cwl(cls, cwl_path: Path) -> "CommandLineTool":
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

        data["baseCommand"] = _parse_base_command(
            raw=data["baseCommand"], base_dir=Path(cwl_path).parent
        )

        if "label" not in data:
            data["label"] = ""

        if "id" not in data:
            data["id"] = str(cwl_path)

        if "environment" not in data:
            data["environment"] = None
        else:
            env = data["environment"]
            if env["env_type"] == "VENV":
                env["path"] = str(resolve_path(env["path"], Path(cwl_path).parent))
            data["environment"] = Environment(config=env)

        return cls(**data)

    def cmd(self) -> str:
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

        ret = deepcopy(self.baseCommand)

        assert isinstance(ret, list)

        for k, v in prefix.items():
            ret.append(k)
            ret.append(f"{{{v}}}")

        for _, v in sorted(positional):
            ret.append(f"{{{v}}}")

        return build_command(
            base_command=" ".join(map(str, ret)),
            environment=self.environment,
        )


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
    def from_cwl(cls, name: str, data: Dict[str, str]) -> "WorkflowInput":
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
    def from_cwl(cls, name: str, data: Dict[str, str]) -> "WorkflowOutput":
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
    def from_cwl(cls, data: Dict[str, Any]) -> "WorkflowStep":
        """Construct a WorkflowStep from cwl data."""
        return cls(
            id=data["id"],
            inputs=data["in"],
            outputs=data["out"],
            run=CommandLineTool.from_cwl(data["run"]),
        )

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
    def from_cwl(cls, cwl_path: Path) -> "Workflow":
        """Construct a Workflow from cwl data."""
        data = load_yaml(cwl_path)
        assert "cwlVersion" in data
        assert data["cwlVersion"] == "v1.2"

        class_type = data["class"]

        if class_type != "Workflow":
            raise CWLError(f"Expected class of type 'Workflow' but got '{class_type}'")

        assert data["class"] == "Workflow"

        inputs = {
            k: WorkflowInput.from_cwl(k, v) for k, v in _parse_io_parameters(data, "inputs").items()
        }
        outputs = {
            k: WorkflowOutput.from_cwl(k, v)
            for k, v in _parse_io_parameters(data, "outputs").items()
        }

        for step_data in data["steps"]:

            path = Path(step_data["run"])

            # resolve relative paths wrt workflow directory
            if not path.is_absolute():
                path = (Path(cwl_path).parent / path).resolve()

            step_data["run"] = str(path)

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


def get_graph(workflow: Workflow) -> Tuple[Dict[str, Node], Set[Edge]]:
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
