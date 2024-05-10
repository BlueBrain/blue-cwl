"""CWL workflow construction module."""

import logging
from typing import Any, Literal

import cwl_luigi.process
from cwl_luigi.common import CustomBaseModel
from cwl_luigi.cwl_types import CWLType
from cwl_luigi.executor import LocalExecutor, SallocExecutor

L = logging.getLogger(__name__)


class ConfigInput(CustomBaseModel):
    """Dataclass for config's input entries."""

    type: CWLType
    value: str


class Config(CustomBaseModel):
    """Dataclass for cwl config."""

    inputs: dict[str, ConfigInput]


class CommandLineBinding(CustomBaseModel):
    """Dataclass for a command line's tool input binding.

    Attributes:
        position: Integer signifying the position in the command line. Default 0.
        prefix: Optional prefix for named argument.
    """

    position: int = 0
    prefix: str | None = None
    separate: bool = True
    itemSeparator: str = " "


class CommandInputArraySchema(CustomBaseModel):
    """CommandInputArraySchema.

    See: https://www.commonwl.org/v1.2/CommandLineTool.html#CommandInputArraySchema
    """

    items: CWLType
    type: Literal["array"]
    label: str | None = None
    doc: str | None = None
    name: str | None = None
    inputBinding: CommandLineBinding | None = None


class CommandInputParameter(CustomBaseModel):
    """CommandInputParameter shema.

    See: https://www.commonwl.org/v1.2/CommandLineTool.html#CommandInputParameter
    """

    id: str
    label: str | None = None
    doc: str | None = None
    inputBinding: CommandLineBinding | None = None
    default: Any = None
    type: CWLType | CommandInputArraySchema


class CommandOutputBinding(CustomBaseModel):
    """CommandOutputbinding schema.

    See: https://www.commonwl.org/v1.2/CommandLineTool.html#CommandOutputBinding
    """

    glob: str | None = None


class CommandOutputParameter(CustomBaseModel):
    """CommandOutputParameter schema.

    See https://www.commonwl.org/v1.2/CommandLineTool.html#CommandOutputParameter
    """

    id: str | None = None
    type: CWLType
    label: str | None = None
    doc: str | None = None
    outputBinding: CommandOutputBinding | None = None


class CommandLineTool(CustomBaseModel):
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

    cwlVersion: Literal["v1.2"]
    id: str = "CommandLineTool"
    baseCommand: list[str]
    inputs: dict[str, CommandInputParameter]
    outputs: dict[str, CommandOutputParameter]
    environment: dict | None = None
    executor: LocalExecutor | SallocExecutor | None = None
    label: str | None = None
    stdout: str | None = None

    def make(self, input_values: dict):
        """Make an executable process out of the template and the inputs."""
        return cwl_luigi.process.build_command_line_tool_process(self, input_values)


class InputArraySchema(CustomBaseModel):
    """InputArraySchema.

    See: https://www.commonwl.org/v1.2/Workflow.html#InputArraySchema
    """

    items: CWLType
    type: Literal["array"]
    label: str | None = None
    doc: str | None = None
    name: str | None = None


class WorkflowInputParameter(CustomBaseModel):
    """WorkflowInputParameter schema.

    See: https://www.commonwl.org/v1.2/Workflow.html#WorkflowInputParameter
    """

    type: InputArraySchema | CWLType
    id: str | None = None
    doc: str | None = None
    label: str | None = None
    default: Any = None


class WorkflowOutputParameter(CustomBaseModel):
    """Dataclass for a workflow's output.

    See: https://www.commonwl.org/v1.2/Workflow.html#WorkflowOutputParameter

    Attributes:
        type: The type of the output.
        outputSource:
    """

    id: str | None = None
    type: CWLType
    label: str | None = None
    doc: str | None = None
    outputSource: str

    def split_source_output(self) -> list[str]:
        """Return step and output names for source."""
        return self.outputSource.split("/", maxsplit=1)


class WorkflowStepInput(CustomBaseModel):
    """Dataclass for a workflow'steps input.

    See https://www.commonwl.org/v1.2/Workflow.html#WorkflowStepInput
    """

    id: str
    label: str | None = None
    source: list[str] | str | None = None
    default: Any = None
    valueFrom: list[str] | str | None = None

    def split_source_output(self) -> list[tuple[str | None, str]] | None:
        """Split source and return source and outputs names if any."""

        def _split_source(source: str):
            res = source.split("/")

            # source is a workflow input
            if len(res) == 1:
                return (None, res[0])

            if len(res) == 2:
                return tuple(res)

            raise ValueError(res)

        if self.source is None:
            return None

        if isinstance(self.source, list):
            # pylint: disable=not-an-iterable
            return [_split_source(source) for source in self.source]

        return [_split_source(self.source)]


class WorkflowStep(CustomBaseModel):
    """Dataclass for a workflow's step.

    Attributes:
        id: The unique identifier for this object.
        inputs: The inputs of the step.
        outputs: The outputs of the step.
        run: The comannd line tool executed by the step.
    """

    id: str
    inputs: dict[str, WorkflowStepInput]
    outputs: list[str]
    run: CommandLineTool


class Workflow(CustomBaseModel):
    """Dataclass for an entire workflow.

    Attributes:
        cwlVersion: Version of the cwl specication.
        id: Name of the workflow.
        label: Description of the workflow.
    """

    cwlVersion: str
    id: str = "Workflow"
    label: str
    inputs: dict[str, WorkflowInputParameter]
    outputs: dict[str, WorkflowOutputParameter]
    steps: list[WorkflowStep]

    def __repr__(self):
        """Return repr of instance."""
        return f"Workflow(id={self.id})"

    def iter_steps(self):
        """Iterate over steps."""
        return iter(self.steps)

    def step_names(self) -> list[str]:
        """Return the names of the workflow steps."""
        return [s.id for s in self.steps]

    def get_step_by_name(self, name):
        """Return the workflow step with given name."""
        for s in self.steps:
            if s.id == name:
                return s
        raise ValueError(f"Not found: {name}")

    def get_step_source_names(self, name: str) -> list[str]:
        """Get source names for workflow step."""
        step = self.get_step_by_name(name)
        sources = set()
        for inp in step.inputs.values():

            source_outputs = inp.split_source_output()

            if source_outputs is None:
                continue

            for source_name, _ in source_outputs:
                if source_name is not None:
                    sources.add(source_name)

        return sorted(sources)

    def make(self, input_values):
        """Make a concretized workflow process."""
        return cwl_luigi.process.build_workflow_process(self, input_values)

    def make_workflow_step(self, step_name, input_values, sources):
        """Make workflow step."""
        return cwl_luigi.process.build_workflow_step_process(self, step_name, input_values, sources)
