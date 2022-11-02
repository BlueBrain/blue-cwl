"""CWL luigi tasks building module."""
import collections
import logging
import subprocess
from copy import deepcopy
from functools import partial
from pathlib import Path
from typing import Any, DefaultDict, Dict, Optional, Tuple, Type

import click
import luigi
import luigi.target
from luigi.task_register import Register

from cwl_luigi import cwl, hashing, utils
from cwl_luigi.cwl_types import CWLType
from cwl_luigi.exceptions import CWLError
from cwl_luigi.luigi_outputs import build_output_target

STDOUT = "STDOUT"


L = logging.getLogger(__name__)


ExistingPathParameter = partial(luigi.PathParameter, exists=True)


LUIGI_PARAMETER_TYPE_MAPPING = {
    CWLType.INT: luigi.IntParameter,
    CWLType.LONG: luigi.IntParameter,
    CWLType.FLOAT: luigi.FloatParameter,
    CWLType.DOUBLE: luigi.FloatParameter,
    CWLType.STRING: luigi.Parameter,
    CWLType.FILE: ExistingPathParameter,
    CWLType.DIRECTORY: ExistingPathParameter,
    CWLType.STDOUT: ExistingPathParameter,
    CWLType.NEXUS_TYPE: luigi.Parameter,
}


class _TaskAdapterMixin(luigi.Task):
    """Adapter class used in the conversion of a cwl task into a luigi one.

    Attrs:

        _command_template:
            A string of the command template to execute, e.g. "cat {f0} {f1}".
            The f0, f1 values in the examples are resolved from the tasks's
            parameters.

        _cwl_outputs:
           A dictionary the keys of which are output names and the values CWLCommandLineToolOutput
           dataclass instances, which describe the outputs of the task.

        _task_parents:
            A dictionary the keys of which are the names of the tasks and the values the parent task
            objects. Default None.

        _internal_mapping:
            A dictionary the keys of which are input names and each value a sequence of the task
            name and its respective output name where the value for this input will be mapped to.
            Default None.

        _stdout:
            The standard output. Default None.

    Note:
        _task_parents and _internal_mapping are set together.
    """

    _command_template: str
    _cwl_outputs: Dict[str, cwl.CommandLineToolOutput]
    _task_parents: Optional[Dict[str, Any]] = None
    _internal_mapping: Optional[Dict[str, Any]] = None
    _stdout: Optional[str] = None

    output_dir = luigi.PathParameter()

    param_kwargs: dict

    def requires(self) -> Dict[str, Any]:
        """Return the required task dictionary, the keys of which are sorted.

        The sorting is crucial for the case of including the parents in the task hash.
        """
        return utils.sorted_dict(self._task_parents) if self._task_parents else {}

    def link_required_tasks(self, tasks: Dict[str, Any], mapping: Dict[str, Any]) -> None:
        """Add required upstream tasks to this task.

        The mapping is a dictionary the keys of which are the inputs of the task and its values are
        two element lists with the upstream task name and its output that maps to this input.
        The internal mapping is stored to allow fetching the outputs from upstream tasks.

        Example:
            {
                "input_1": ["task_b", "task_b_out2"],
                "input_2": ["task_b", "task_b_out3"],
            }
        """
        self._task_parents = {
            source_task: tasks[source_task] for source_task, _ in mapping.values()
        }
        self._internal_mapping = deepcopy(mapping)

    def output(self) -> Dict[str, Any]:
        """Return a dictionary the keys of which are the names and values the output targets.

        The sorting is not crucial for the task, but is useful for reproducibility.
        """
        sorted_outputs = utils.sorted_dict(self._cwl_outputs)
        return {
            name: build_output_target(
                task=self,
                cwl_output=out,
                output_dir=self.output_dir,
            )
            for name, out in sorted_outputs.items()
        }

    def _get_parent_output_target(self, parent_name: str, output_name: str) -> Any:
        if self._task_parents is None:
            raise CWLError(
                f"Parent '{parent_name}' with output '{output_name}' is not available.\n"
                f"Class name: {type(self).__name__}"
            )
        return self._task_parents[parent_name].output()[output_name]

    def get_sorted_param_kwargs(self) -> Dict[str, Any]:
        """Return the task parameter dict sorted by key.

        Params are sorted again in case they have been added dynamically or via inheritance.
        """
        return utils.sorted_dict(self.param_kwargs)

    def get_task_inputs(self) -> Dict[str, Any]:
        """Return the inputs to the current variant task.

        Inputs are defined as the union of the parameters and the upstream inputs as mandated by
        the internal mapping.
        """
        inputs = dict(self.get_sorted_param_kwargs())

        internal_mapping = self._internal_mapping or {}

        input_to_parent_output = {
            input_name: self._get_parent_output_target(parent_name, parent_output_name)
            for input_name, (parent_name, parent_output_name) in internal_mapping.items()
        }

        for input_name, parent_target in input_to_parent_output.items():
            if isinstance(parent_target, luigi.target.FileSystemTarget):
                inputs[input_name] = parent_target.path
            else:
                inputs[input_name] = parent_target.resource.id

        return inputs

    def command(self) -> str:
        """Return the command to execute.

        The command is constructed from the _cmd command template after substituting the
        placeholders using the task's inputs.
        """
        inputs = self.get_task_inputs()
        cmd = self._command_template.format(**inputs)

        # TODO: This is a hack, we need sth better than this
        if "nexus_base" in self.param_kwargs:
            cmd += f" --task-digest {self.get_task_hexdigest()}"
        return cmd

    def get_task_hexdigest(self):
        """Return a hash generated out of the parameters of the task."""
        return hashing.get_task_hexdigest(self)

    def run(self) -> None:
        """Main execution body."""
        cwd = Path(self.output_dir)
        cmd = self.command()

        # so we can see the command, and pretend it took a while
        click.secho(f"[{cwd}]: {cmd}", fg="blue")

        res = subprocess.run(
            cmd, cwd=cwd, capture_output=True, encoding="utf-8", check=False, shell=True
        )

        if res.returncode != 0:
            raise CWLError(
                f"Command {cmd} failed to run.\n" f"stdout: {res.stdout}\n" f"stderr: {res.stderr}"
            )

        if res.stderr:
            L.warning("Found nonempty stderr in subprocess:\n %s", res.stderr)

        output = res.stdout
        L.debug("Subprocess output:\n\n%s\n\n", output)

        if self._stdout:
            outputs = self.output()
            with open(outputs[self._stdout].path, "w", encoding="utf-8") as fd:
                fd.write(output)


def build_task(tool: cwl.CommandLineTool, task_name: Optional[str] = None) -> Any:
    """Build a luigi task out of a cwl tool definition.

    The inputs of the tool will become luigi parameters and the outputs targets.

    Args:
        tool: The cwl CommandLineTool dataclass instance.
        task_name: Name of luigi task. If left unspecified, the tool's id will be used instead.

    Returns:
        A luigi task class.
    """
    cls_dict: Dict[str, Any] = _build_task_parameters(tool)

    cls_name = task_name or tool.id

    # This is temporary fix so that the same class object is used for a given class name always
    # It addresses the case where the main task may restart resulting in recreating the classes
    # which are attempted to be registered again to the global Register.
    for task_cls in Register._reg:
        name = task_cls.get_task_family()
        if name == f"{__name__}.{cls_name}":
            return task_cls

    cls_dict["__module__"] = __name__

    cls = type(cls_name, (_TaskAdapterMixin,), cls_dict)

    attributes: Dict[str, Any] = _build_task_attributes(tool)

    for key, value in attributes.items():
        if not (hasattr(cls, key) or key in cls.__annotations__):
            raise AttributeError(f"Attribute {key} does not exist in class.")
        setattr(cls, key, value)

    return cls


def _build_task_parameters(tool: cwl.CommandLineTool) -> Dict[str, Type[luigi.Parameter]]:
    """Create the luigi task's parameters.

    These parameters reflect the cwl inputs of the tool. CWL types are mapped to luigi types via
    the LUIGI_PARAMETER_TYPE_MAPPING map.

    Returns:
        A sorted by key dictionary. The sorting is important to ensure that the task hash, which is
            generated by its parameters is always the same for the same set of parameters.
    """
    inputs = {
        input_name: LUIGI_PARAMETER_TYPE_MAPPING[input_data.type]()
        for input_name, input_data in tool.inputs.items()
    }
    # the nexus parameters should not contributes to the task hash
    for input_name in ("nexus_base", "nexus_project", "nexus_org", "nexus_token"):
        if input_name in inputs:
            inputs[input_name].significant = False

    return utils.sorted_dict(inputs)


def _build_task_attributes(tool: cwl.CommandLineTool) -> Dict[str, Any]:

    outputs = dict(tool.outputs)

    stdout = None
    for name, out in outputs.items():
        if out.type == CWLType.STDOUT:
            stdout = name
    attributes = {
        "_command_template": tool.cmd(),
        "_cwl_outputs": outputs,
        "_stdout": stdout,
    }
    return attributes


def _build_inputs_mapping(workflow: cwl.Workflow) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Build a maping of the workflow inputs.

    Inputs are classified into internal and external, depending on whether they are passed from
    outside, via the config, or from upstream tasks in the workflow DAG respectively.
    """
    internal: DefaultDict[str, dict] = collections.defaultdict(dict)
    external: DefaultDict[str, dict] = collections.defaultdict(dict)

    for step in workflow.steps:
        name = step.id
        for input_name, input_source in step.inputs.items():
            parts = input_source.split("/", 1)
            if len(parts) == 1:
                external[name][input_name] = parts[0]
            else:
                internal[name][input_name] = parts

    return dict(internal), dict(external)


def build_workflow(
    workflow: cwl.Workflow,
    base_dir: Path,
    config: cwl.Config,
) -> Dict[str, Any]:
    """Build luigi task from cwl workflow and a config with the external inputs."""
    assert base_dir.is_absolute(), f"base_dir {base_dir} must be absolute."

    task_classes_standalone = _build_standalone_classes(workflow)

    internal, external = _build_inputs_mapping(workflow)

    task_classes = _build_dependent_classes(task_classes_standalone, internal)

    external = resolve_mapping_with_config(external, config)

    # initialize dependent tasks
    tasks = {}
    for name, task_class in task_classes.items():

        task_inputs = external.get(name, {})

        task_output_dir = base_dir.resolve() / task_class.__name__
        task_output_dir.mkdir(parents=True, exist_ok=True)
        task_inputs["output_dir"] = task_output_dir

        tasks[name] = task_class(**task_inputs)

    for task_name, internal_mapping in internal.items():
        tasks[task_name].link_required_tasks(tasks, internal_mapping)
    return tasks


def _build_standalone_classes(workflow: cwl.Workflow) -> Dict[str, Type[luigi.Task]]:
    """Build independent task classes that are not yet linked into a DAG."""
    return {step.id: build_task(step.run, task_name=step.id) for step in workflow.steps}


def _remove_dependent_parameters(
    task_class_standalone: Any, parameters_to_remove
) -> Type[luigi.Task]:
    """Create a copy and remove the parameter attributes from the class."""
    task_class = deepcopy(task_class_standalone)

    for name in parameters_to_remove:
        delattr(task_class, name)

    return task_class


def _build_dependent_classes(standalone_classes, mapping) -> Dict[str, Type[luigi.Task]]:
    """Build dependent classes.

    Make copies of the standalone task classes, where the parameters of the inputs that are
    goind to be retrieved through the DAG are removed.
    """
    task_classes = {}
    for name, task_class_standalone in standalone_classes.items():
        if name in mapping:
            cls = _remove_dependent_parameters(task_class_standalone, mapping[name])
        else:
            cls = task_class_standalone

        task_classes[name] = cls
    return task_classes


def resolve_mapping_with_config(mapping: Dict[str, Any], config: cwl.Config) -> Dict[str, Any]:
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
