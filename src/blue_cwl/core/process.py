"""Executable Process creation module."""

import logging
from typing import TYPE_CHECKING, Any

import blue_cwl.core.cwl
from blue_cwl.core.common import CustomBaseModel
from blue_cwl.core.cwl_types import Directory, File, NexusResource
from blue_cwl.core.exceptions import CWLError
from blue_cwl.core.executor import Executor, LocalExecutor
from blue_cwl.core.resolve import resolve_parameter_references
from blue_cwl.core.types import EnvVarDict, InputValue, InputValueObject, OutputValueObject

L = logging.getLogger(__name__)

if TYPE_CHECKING:
    from blue_cwl.core.cwl import CommandLineTool, Workflow


class CommandLineToolProcess(CustomBaseModel):
    """Executable CommanLineTool process."""

    inputs: dict[str, Any]
    outputs: dict[str, Any]
    base_command: list[str]
    environment: dict | None = None
    executor: Executor

    def build_command(self, *, env_vars: EnvVarDict | None = None) -> str:
        """Build command from process attributes.

        Args:
            env_vars: Dictionary of env vars to export in the command.

        Returns:
            Command as a string.
        """
        return self.executor.build_command(
            base_command=self.base_command,
            env_config=self.environment,
            env_vars=env_vars,
        )

    def run_command(
        self,
        *,
        command: str,
        redirect_to: str | None = None,
        masked_vars: list[str] | None = None,
        **kwargs,
    ) -> None:
        """Execute input command.

        Args:
            command: Command to execute as a string.
            redirect_to: Output file path to redirect the output.
            masked_vars: List of variables to mask from logging.
            **kwargs: Key arguments to pass to the subprocess.
        """
        self.executor.run(
            command=command, redirect_to=redirect_to, masked_vars=masked_vars, **kwargs
        )

    def run(
        self,
        *,
        env_vars: EnvVarDict | None = None,
        redirect_to: str | None = None,
        masked_vars: list[str] | None = None,
        **kwargs,
    ) -> None:
        """Execute the process.

        Args:
            env_vars: Environment variables dictionary.
            redirect_to: Output file path to redirect the output.
            masked_vars: List of variables to mask from logging.
            **kwargs: Key arguments to pass to the subprocess.
        """
        str_command = self.build_command(
            env_vars=env_vars,
        )
        self.run_command(
            command=str_command,
            redirect_to=redirect_to,
            masked_vars=masked_vars,
            **kwargs,
        )


class WorkflowProcess(CustomBaseModel):
    """Workflow process."""

    inputs: dict[str, Any]
    outputs: dict[str, Any]
    steps: list[CommandLineToolProcess]


def build_command_line_tool_process(
    tool: "CommandLineTool",
    input_values: dict[str, InputValue],
) -> CommandLineToolProcess:
    """Build CommandLineToolProcess.

    Args:
        tool: CommandLineTool template to make executable.
        input_values: Input values for tool.

    Returns:
        Executable CommandLineToolProcess
    """
    L.debug("CommandLineTool input values: %s", input_values)

    concretized_inputs: dict[str, InputValueObject] = _concretize_inputs(tool.inputs, input_values)
    L.debug("Concretized CommandLineTool inputs: %s", concretized_inputs)

    concretized_outputs: dict[str, OutputValueObject] = _concretize_tool_outputs(
        tool.outputs, concretized_inputs
    )
    L.debug("Concretized CommandLineTool outputs: %s", concretized_outputs)

    concretized_command: list = _concretize_tool_command(tool, concretized_inputs)
    L.debug("Concretized CommandLineTool command: %s", concretized_command)

    executor: Executor = tool.executor or LocalExecutor()
    L.debug("CommandLineTool executor: %s", executor)

    process = CommandLineToolProcess.from_dict(
        {
            "inputs": concretized_inputs,
            "outputs": concretized_outputs,
            "base_command": concretized_command,
            "environment": tool.environment,
            "executor": executor,
        }
    )
    return process


def _concretize_inputs(
    inputs: dict, input_values: dict[str, InputValue]
) -> dict[str, InputValueObject]:
    concretized_inputs: dict[str, InputValueObject] = {}

    for name, inp in inputs.items():
        value = input_values.get(name, None)
        if value is None:
            if inp.default is not None:
                concretized_inputs[name] = _input_value_to_object(inp.type, inp.default)
        else:
            concretized_inputs[name] = _input_value_to_object(inp.type, value)

    if not set(inputs).issubset(set(concretized_inputs)):
        raise CWLError(
            "Concretized input values are not consistent with the input template.\n"
            f"Expected: {sorted(inputs.keys())}.\n"
            f"Got     : {sorted(concretized_inputs.keys())}"
        )

    return concretized_inputs


def _concretize_tool_outputs(
    outputs, input_values: dict[str, InputValueObject]
) -> dict[str, OutputValueObject]:
    concretized_outputs: dict[str, OutputValueObject] = {}
    for name, output in outputs.items():
        match output.type:
            case "File":
                out_binding = output.outputBinding
                path = resolve_parameter_references(
                    expression=out_binding.glob,
                    inputs=input_values,
                    context=None,
                    runtime=None,
                )
                concretized_outputs[name] = File(path=path)
            case "Directory":
                out_binding = output.outputBinding
                path = resolve_parameter_references(
                    expression=out_binding.glob,
                    inputs=input_values,
                    context=None,
                    runtime=None,
                )
                concretized_outputs[name] = Directory(path=path)

            case "NexusType":
                out_binding = output.outputBinding
                path = resolve_parameter_references(
                    expression=out_binding.glob,
                    inputs=input_values,
                    context=None,
                    runtime=None,
                )
                concretized_outputs[name] = NexusResource(path=path)
            case _:
                raise NotImplementedError()
    return concretized_outputs


def _concretize_tool_command(tool, input_values: dict[str, InputValueObject]) -> list:
    """Construct tool command with input values."""
    args = []
    for i, inp in enumerate(tool.inputs.values()):
        name, binding = inp.id, inp.inputBinding

        sorting_key = (binding.position, i)

        cmd_elements = _cmd_elements(inp.type, binding, input_values[name])

        if cmd_elements is not None:
            args.append((sorting_key, cmd_elements))

    args_command = [e for _, cmd in sorted(args, key=lambda a: a[0]) for e in cmd]

    return tool.baseCommand + args_command


def _cmd_elements(type_, binding, value) -> tuple | list[tuple] | None:
    def _obj_to_string(obj):
        match obj:
            case File() | Directory() | NexusResource():
                return str(obj.path)

            case _:
                return str(obj)

    def _separate(prefix, value, separate) -> tuple:
        if prefix is None:
            return (value,)
        if separate:
            return (prefix, value)
        return (f"{prefix}{value}",)

    match type_:
        case "File" | "Directory":
            return _separate(binding.prefix, _obj_to_string(value), binding.separate)

        case "boolean":
            if bool(value):
                if binding.prefix is None:
                    raise CWLError("Binding prefix for boolean values cannot be None.")
                return (binding.prefix,)
            return None

        case blue_cwl.core.cwl.CommandInputArraySchema():
            if not isinstance(value, list):
                raise CWLError(f"Value '{value}' is not a list.")

            if item_binding := type_.inputBinding:
                elements: list[tuple] = []
                for v in value:
                    elements.extend(
                        _separate(item_binding.prefix, _obj_to_string(v), item_binding.separate)
                    )
                return elements
            str_join = binding.itemSeparator.join(_obj_to_string(v) for v in value)
            return _separate(binding.prefix, str_join, binding.separate)

        case "string" | "int" | "long" | "float" | "double":
            return _separate(binding.prefix, value, binding.separate)

        case "NexusType":
            if not isinstance(value.id, str):
                raise CWLError(f"NexusType id '{value.id}' is not a string.")
            return _separate(binding.prefix, value.id, binding.separate)

        case _:
            raise NotImplementedError(type(value))


def _get_step_output(source, sources):
    step_name, step_output = source.split("/")
    value = sources[step_name].outputs[step_output]
    return value


def build_workflow_step_process(
    workflow, step_name: str, input_values, sources: dict[str, Any]
) -> CommandLineToolProcess:
    """Build workflow step process."""

    def _get_source_value(source, input_objects, sources):
        """Copy the value from the source."""
        if isinstance(source, list):
            return [_get_source_value(s, input_objects, sources) for s in source]

        try:
            value = _get_step_output(source, sources)
        except ValueError:
            value = input_objects[source]
        return value

    step = workflow.get_step_by_name(step_name)

    input_objects = _concretize_inputs(workflow.inputs, input_values)

    step_input_values = {}

    step_sources = {}

    # loop over inputs to assign sources as input values and defaults
    for name, inp in step.inputs.items():
        if inp.source:
            result = _get_source_value(inp.source, input_objects, sources)
            step_sources[name] = result
        else:
            result = None
            step_sources[name] = None

        # The default value for this parameter to use if either there is no source field,
        # or the value produced by the source is null.
        if result is None:
            result = inp.default

        step_input_values[name] = result

    # The valueFrom fields are evaluated after the the source fields.
    for name, inp in step.inputs.items():
        if inp.valueFrom:
            step_input_values[name] = resolve_parameter_references(
                expression=inp.valueFrom,
                inputs=step_input_values,
                context=step_sources[name],
                runtime={},
            )

    step_process = build_command_line_tool_process(step.run, step_input_values)
    return step_process


def build_workflow_process(
    workflow: "Workflow", input_values: dict[str, InputValue]
) -> "WorkflowProcess":
    """Build WorkflowProcess."""
    concretized_inputs: dict[str, InputValueObject] = _concretize_inputs(
        workflow.inputs, input_values
    )

    step_processes: dict[str, CommandLineToolProcess] = {}
    for step in workflow.steps:
        sources = {name: step_processes[name] for name in workflow.get_step_source_names(step.id)}

        step_processes[step.id] = build_workflow_step_process(
            workflow=workflow,
            step_name=step.id,
            input_values=concretized_inputs,
            sources=sources,
        )

    concretized_outputs = {
        name: _get_step_output(output.outputSource, step_processes)
        for name, output in workflow.outputs.items()
    }

    return WorkflowProcess(
        inputs=concretized_inputs,
        outputs=concretized_outputs,
        steps=list(step_processes.values()),
    )


def _input_value_to_object(input_type, input_value):
    match input_type:
        case dict():
            # e.g. {"type": "array", "items": "string"}

            if input_type["type"] != "array":
                raise CWLError("Input type is not 'array' for array schema.")

            if not isinstance(input_value, list):
                raise CWLError(f"Input value '{input_value}' for array is not a list")

            element_type = input_type["items"]
            value = [_input_value_to_object(element_type, v) for v in input_value]

        case "File":
            match input_value:
                case str():
                    value = File(path=input_value)
                case dict():
                    value = File.from_dict(input_value)
                case File():
                    value = input_value
                case _:
                    raise ValueError(f"type: {input_type}, value: {type(input_value)}")

        case "Directory":
            match input_value:
                case str():
                    value = Directory(path=input_value)
                case dict():
                    value = Directory.from_dict(input_value)
                case Directory():
                    value = input_value
                case _:
                    raise ValueError(f"type: {input_type}, value: {input_value}")

        case "string":
            match input_value:
                case File() | Directory():
                    value = input_value.path
                case str():
                    value = input_value
                case int() | float():
                    value = str(input_value)
                case _:
                    raise ValueError(f"type: {input_type}, value: {input_value}")

        case "NexusType":
            match input_value:
                case File():
                    value = NexusResource(path=input_value.path)
                case str():
                    value = NexusResource(id=input_value)
                case NexusResource():
                    value = input_value
                case _:
                    raise ValueError(f"type: {input_type}, value: {input_value}")

        case "boolean":
            value = bool(input_value)

        case "int" | "long":
            value = int(input_value)

        case "float" | "double":
            value = float(input_value)

        case _:
            value = input_value

    return value
