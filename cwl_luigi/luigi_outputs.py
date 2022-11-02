"""Luigi target creation utils from cwl types."""
from functools import partial
from pathlib import Path
from typing import Any, Dict

import luigi
from luigi.contrib.ssh import RemoteTarget

from cwl_luigi import cwl
from cwl_luigi.cwl_types import CWLType
from cwl_luigi.nexusdb import NexusTarget


def build_output_target(
    task: luigi.Task,
    cwl_output: cwl.CommandLineToolOutput,
    output_dir: Path,
) -> luigi.Target:
    """Create an output target based on the output type."""
    cwl_output_to_target: Dict[CWLType, Any] = {
        CWLType.FILE: partial(_create_path_target, output_dir=output_dir),
        CWLType.STDOUT: partial(_create_path_target, output_dir=output_dir),
        CWLType.NEXUS_TYPE: _create_nexus_target,
    }

    target_function = cwl_output_to_target[cwl_output.type]

    return target_function(task, cwl_output)


def _create_path_target(
    task: luigi.Task, cwl_output: cwl.CommandLineToolOutput, output_dir: Path, is_local: bool = True
):
    """Create a target that is associated with a filesystem."""
    path = output_dir / cwl_output.path

    if is_local:
        # this can be used for debugging
        return luigi.LocalTarget(path=path)

    return RemoteTarget(path=path, host=task.host)


def _create_nexus_target(task: luigi.Task, cwl_output: cwl.CommandLineToolOutput):
    """Create a target that will be an entry in the kg database."""
    return NexusTarget(name=cwl_output.id, task=task)
