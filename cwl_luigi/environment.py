"""Environment related utilities."""
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Dict

from cwl_luigi.constants import (
    APPTAINER_EXECUTABLE,
    APPTAINER_IMAGEPATH,
    APPTAINER_MODULEPATH,
    APPTAINER_MODULES,
    APPTAINER_OPTIONS,
    MODULES_ENABLE_PATH,
    SPACK_MODULEPATH,
)


def _build_module_cmd(cmd: str, config: Dict[str, Any], allocation) -> str:
    """Wrap the command with modules."""
    modulepath = config.get("modulepath", SPACK_MODULEPATH)
    modules = config["modules"]

    if allocation:
        cmd = allocation.shell_command(cmd)

    return " && ".join(
        [
            f". {MODULES_ENABLE_PATH}",
            "module purge",
            f"export MODULEPATH={modulepath}",
            f"module load {' '.join(modules)}",
            f"echo MODULEPATH={modulepath}",
            "module list",
            cmd,
        ]
    )


def _build_apptainer_cmd(cmd: str, config: Dict[str, Any], allocation) -> str:
    """Wrap the command with apptainer/singularity."""
    modulepath = config.get("modulepath", APPTAINER_MODULEPATH)
    modules = config.get("modules", APPTAINER_MODULES)
    options = config.get("options", APPTAINER_OPTIONS)
    executable = config.get("executable", APPTAINER_EXECUTABLE)
    image = Path(APPTAINER_IMAGEPATH, config["image"])
    # the current working directory is used also inside the container
    cmd = f'{executable} exec {options} {image} bash <<EOF\ncd "$(pwd)" && {cmd}\nEOF\n'

    if allocation:
        cmd = allocation.shell_command(cmd)

    cmd = " && ".join(
        [
            f". {MODULES_ENABLE_PATH}",
            "module purge",
            f"module use {modulepath}",
            f"module load {' '.join(modules)}",
            "singularity --version",
            cmd,
        ]
    )
    return cmd


def _build_venv_cmd(cmd: str, config: Dict[str, Any], allocation):
    """Wrap the command with an existing virtual environment."""
    path = config["path"]

    if allocation:
        cmd = allocation.shell_command(cmd)

    return f". {path}/bin/activate && {cmd}"


@dataclass(frozen=True)
class Environment:
    """Runtime environment setup."""

    config: Dict[str, Any]
    _mapping: ClassVar[Dict[str, Any]] = {
        "MODULE": _build_module_cmd,
        "APPTAINER": _build_apptainer_cmd,
        "VENV": _build_venv_cmd,
    }

    def shell_command(self, cmd, allocation=None) -> str:
        """Get shell command combining the chosen environment and the current cmd."""
        build_function = self._mapping[self.config["env_type"]]
        return build_function(cmd=cmd, config=self.config, allocation=allocation)
