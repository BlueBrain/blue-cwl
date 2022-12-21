"""CWL workflow construction module."""
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from cwl_luigi import cwl_parser
from cwl_luigi.cwl_types import CWLType

L = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConfigInput:
    """Dataclass for config's input entries."""

    type: CWLType
    value: str

    @classmethod
    def from_cwl(cls, data: Union[str, Dict[str, Any]]) -> "ConfigInput":
        """Generate ConfigInput instance from cwl data."""
        if isinstance(data, str):
            input_type = CWLType.STRING
            input_value = data
        else:
            input_type = CWLType.from_string(data["class"])
            if input_type == CWLType.NEXUS_TYPE:
                input_value = data["resource-id"]
            else:
                input_value = data["path"]

        return cls(type=input_type, value=input_value)


@dataclass(frozen=True)
class Config:
    """Dataclass for cwl config."""

    inputs: Dict[str, ConfigInput]

    @classmethod
    def from_cwl(cls, cwl_file: Path) -> "Config":
        """Generate a config instance from a cwl config file."""
        data = cwl_parser.parse_config_file(cwl_file)

        inputs = {name: ConfigInput.from_cwl(inp) for name, inp in data["inputs"].items()}

        return cls(inputs=inputs)


@dataclass(frozen=True)
class CommandLineBinding:
    """Dataclass for a command line's tool input binding.

    Attributes:
        position: Integer signifying the position in the command line. Default 0.
        prefix: Optional prefix for named argument.
    """

    position: int
    prefix: Optional[str]

    @classmethod
    def from_cwl(cls, data: Dict[str, Any]) -> "CommandLineBinding":
        """Generate a CommandLineBinding from cwl data."""
        return cls(
            position=int(data.get("position", 0)),
            prefix=data.get("prefix", None),
        )


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
    inputBinding: CommandLineBinding

    @classmethod
    def from_cwl(cls, name: str, data: Dict[str, Any]) -> "CommandLineToolInput":
        """Construct a CommandLineToolInput from cwl data."""
        return cls(
            id=name,
            type=CWLType.from_string(data["type"]),
            inputBinding=CommandLineBinding.from_cwl(data=data["inputBinding"]),
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
    doc: str
    outputBinding: Dict[str, Any]  # glob: "bmo/me-type-property/nodes.h5"

    @property
    def path(self):
        """Return output binding's relative path."""
        return Path(self.outputBinding["glob"])

    @classmethod
    def from_cwl(cls, name: str, data: Dict[str, Any], stdout=None) -> "CommandLineToolOutput":
        """Construct a CommandLineToolOutput from cwl data."""
        # make stdout behave like a normal file
        if "outputBinding" not in data:
            assert stdout is not None
            output_binding = {"glob": stdout}
        else:
            output_binding = data["outputBinding"]

        return cls(
            id=name,
            type=CWLType.from_string(data["type"]),
            doc=data.get("doc", ""),
            outputBinding=output_binding,
        )


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
    environment: Optional[dict]

    @classmethod
    def from_cwl(cls, cwl_path: Path) -> "CommandLineTool":
        """Construct a CommandLineTool from cwl data."""
        data = cwl_parser.parse_command_line_tool_file(cwl_path)

        inputs = {
            input_name: CommandLineToolInput.from_cwl(input_name, input_data)
            for input_name, input_data in data["inputs"].items()
        }
        outputs = {
            output_name: CommandLineToolOutput.from_cwl(output_name, output_data, data["stdout"])
            for output_name, output_data in data["outputs"].items()
        }
        environment = data["environment"] if "environment" in data else None

        return cls(
            cwlVersion=data["cwlVersion"],
            id=data.get("id", str(cwl_path)),
            label=data.get("label", ""),
            baseCommand=data["baseCommand"],
            inputs=inputs,
            outputs=outputs,
            stdout=data.get("stdout", "stdout.txt"),
            environment=environment,
        )

    def assort_inputs(self) -> Tuple[List[Tuple[int, str]], Dict[str, str]]:
        """Categorize inputs into positional and named arguments."""
        named_arguments: Dict[str, str] = {}
        positional_arguments: List[Tuple[int, str]] = []

        for cmd_input in self.inputs.values():

            name = cmd_input.id
            binding = cmd_input.inputBinding

            if binding.prefix:
                named_arguments[binding.prefix] = name
            else:
                positional_arguments.append((binding.position, name))

        return positional_arguments, named_arguments


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
        run_entry = data["run"]
        if isinstance(run_entry, CommandLineTool):
            run_obj = run_entry
        else:
            run_obj = CommandLineTool.from_cwl(Path(data["run"]))

        return cls(
            id=data["id"],
            inputs=data.get("in", {}),
            outputs=data["out"],
            run=run_obj,
        )

    def get_input_name_by_target(self, target: str) -> str:
        """Return the input name, given its target value."""
        for k, v in self.inputs.items():
            if v == target:
                return k
        raise ValueError("Target does not exist in inputs.")


@dataclass(frozen=True)
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
    inputs: Dict[str, WorkflowInput]
    outputs: Dict[str, WorkflowOutput]
    steps: List[WorkflowStep]

    def step_names(self) -> List[str]:
        """Return the names of the workflow steps."""
        return [s.id for s in self.steps]

    @classmethod
    def from_cwl(cls, cwl_path: Path) -> "Workflow":
        """Construct a Workflow from cwl data."""
        data = cwl_parser.parse_workflow_file(cwl_path)
        inputs = {k: WorkflowInput.from_cwl(k, v) for k, v in data["inputs"].items()}
        outputs = {k: WorkflowOutput.from_cwl(k, v) for k, v in data["outputs"].items()}
        steps = list(map(WorkflowStep.from_cwl, data["steps"]))
        return cls(
            cwlVersion=data["cwlVersion"],
            id=data.get("id", str(cwl_path)),
            label=data.get("label", ""),
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
