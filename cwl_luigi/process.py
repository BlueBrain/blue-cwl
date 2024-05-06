"""Executable Process creation module."""

import logging
from copy import deepcopy
from typing import TYPE_CHECKING, Any

import cwl_luigi.cwl
from cwl_luigi.common import CustomBaseModel
from cwl_luigi.cwl_types import Directory, File, NexusResource
from cwl_luigi.exceptions import CWLError
from cwl_luigi.executor import LocalExecutor, SallocExecutor
from cwl_luigi.resolve import resolve_parameter_references
from cwl_luigi.types import EnvVarDict

L = logging.getLogger(__name__)

if TYPE_CHECKING:
    from cwl_luigi.cwl import CommandLineTool, Workflow


InputValueObject = (
    str
    | int
    | float
    | list[str]
    | list[int]
    | list[float]
    | File
    | list[File]
    | Directory
    | list[Directory]
)


class CommandLineToolProcess(CustomBaseModel):
    """Executable CommanLineTool process."""

    inputs: dict[str, Any]
    outputs: dict[str, Any]
    base_command: list[str]
    environment: dict | None = None
    executor: LocalExecutor | SallocExecutor

    def build_command(self, *, env_vars: EnvVarDict | None = None) -> str:
        """Build command."""
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
    ):
        """Run command."""
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
        """Execute the process."""
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
    """Executable Workflow process."""

    inputs: dict[str, Any]
    outputs: dict[str, Any]
    steps: list[CommandLineToolProcess]


def build_command_line_tool_process(
    tool: "CommandLineTool",
    input_values: dict,
) -> CommandLineToolProcess:
    """Build CommandLineToolProcess."""
    input_objects: dict[str, InputValueObject] = _make_input_objects(tool.inputs, input_values)
    L.debug("Inputs objects: %s", input_values)

    concretized_inputs = _concretize_tool_inputs(tool.inputs, input_objects)
    L.debug("Concretized tool inputs: %s", concretized_inputs)

    concretized_outputs = _concretize_tool_outputs(tool.outputs, input_objects)
    L.debug("Concretized tool outputs: %s", concretized_outputs)

    concretized_command = _concretize_tool_command(tool, concretized_inputs)
    L.debug("Concretized tool command: %s", concretized_command)

    executor = tool.executor or LocalExecutor()
    L.debug("Tool executor: %s", executor)

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


def _concretize_tool_inputs(inputs, input_values):

    concretized_inputs = deepcopy(input_values)

    # assign defaults
    for name, inp in inputs.items():
        if inp.default is not None and name not in input_values:
            concretized_inputs[name] = _input_value_to_object(inp.type, inp.default)

    if inputs.keys() != concretized_inputs.keys():
        raise CWLError(
            "Concretized input values are not consistent with the input template.\n"
            f"Expected: {sorted(inputs.keys())}.\n"
            f"Got     : {sorted(concretized_inputs.keys())}"
        )

    return concretized_inputs


def _concretize_tool_outputs(outputs, input_values):
    concretized_outputs = {}
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


def _concretize_tool_command(tool, input_values: dict) -> list:
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
                assert binding.prefix is not None
                return (binding.prefix,)
            return None

        case cwl_luigi.cwl.CommandInputArraySchema():
            assert isinstance(value, list)
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
            assert isinstance(value.id, str), value
            return _separate(binding.prefix, value.id, binding.separate)

        case _:
            raise NotImplementedError(type(value))


def _obj_to_string(obj):

    match obj:
        case File() | Directory():
            return str(obj.path)

        case str():
            return obj

        case _:
            raise NotImplementedError(obj)


def _get_step_output2(source, sources):
    step_name, step_output = source.split("/")
    value = sources[step_name].outputs[step_output]
    return value


def _get_source_value2(source, input_objects, sources):
    """Copy the value from the source."""
    if isinstance(source, list):
        return [_get_source_value2(s, input_objects, sources) for s in source]

    try:
        value = _get_step_output2(source, sources)
    except ValueError:
        value = input_objects[source]
    return value


def build_workflow_step_process(
    workflow, step_name: str, input_values, sources: dict[str, Any]
) -> CommandLineToolProcess:
    """Build workflow step process."""
    step = workflow.get_step_by_name(step_name)

    input_objects: dict[str, InputValueObject] = _make_input_objects(workflow.inputs, input_values)

    step_input_values = {}

    step_sources = {}

    # loop over inputs to assign sources as input values and defaults
    for name, inp in step.inputs.items():
        if inp.source:
            result = _get_source_value2(inp.source, input_objects, sources)
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


def _get_step_output(source, global_outputs):
    step_name, step_output = source.split("/")
    value = global_outputs[step_name][step_output]
    return value


def build_workflow_process(workflow: "Workflow", input_values: dict) -> "WorkflowProcess":
    """Build WorkflowProcess."""
    input_objects: dict[str, InputValueObject] = _make_input_objects(workflow.inputs, input_values)

    outputs: dict[str, Any] = {}

    def _get_source_value(source):
        """Copy the value from the source."""
        if isinstance(source, list):
            return [_get_source_value(s) for s in source]

        try:
            value = _get_step_output(source, outputs)
        except ValueError:
            value = input_objects[source]
        return value

    process_steps = []
    for step in workflow.steps:
        step_input_values = {}
        step_sources = {}

        # loop over inputs to assign sources as input values and defaults
        for name, inp in step.inputs.items():

            if inp.source:
                result = _get_source_value(inp.source)
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
        outputs[step.id] = step_process.outputs

        process_steps.append(step_process)

    concretized_outputs = _concretize_workflow_outputs(workflow.outputs, outputs)

    return WorkflowProcess(
        inputs=input_objects,
        outputs=concretized_outputs,
        steps=process_steps,
    )


def _concretize_workflow_outputs(workflow_outputs, global_outputs):

    concretized_outputs = {}
    for name, output in workflow_outputs.items():
        concretized_outputs[name] = _get_step_output(output.outputSource, global_outputs)
    return concretized_outputs


def _make_input_objects(inputs: dict, input_values: dict) -> dict:

    if inputs.keys() != input_values.keys():
        raise CWLError(
            "Input values are not consistent with the input template.\n"
            f"Expected: {sorted(inputs.keys())}.\n"
            f"Got     : {sorted(input_values.keys())}"
        )

    return {
        name: _input_value_to_object(inputs[name].type, value)
        for name, value in input_values.items()
    }


def _input_value_to_object(input_type, input_value):

    match input_type:

        case dict():
            # e.g. {"type": "array", "items": "string"}
            assert input_type["type"] == "array", input_type
            assert isinstance(input_value, list)
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
                case _:
                    raise ValueError(f"type: {input_type}, value: {input_value}")

        case "NexusType":
            match input_value:
                case File() | Directory():
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
