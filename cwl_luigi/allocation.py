"""Allocation related utilities."""
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from cwl_luigi.utils import load_yaml


@dataclass(frozen=True)
class Allocation:
    """Allocation configuration."""

    jobname: str
    salloc_command: str
    env_vars: Dict[str, str]

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "Allocation":
        """Create an Allocation instance from a configuration dictionary."""
        return cls(
            jobname=config["jobname"],
            salloc_command=config["salloc"],
            env_vars=config.get("env_vars", {}),
        )

    def shell_command(self, cmd) -> str:
        """Get shell command combining the allocation config and the current cmd."""
        cmd = _escape_single_quotes(cmd)
        cmd = f"stdbuf -oL -eL salloc -J {self.jobname} {self.salloc_command} srun sh -c '{cmd}'"
        # set the environment variables if needed
        if self.env_vars:
            variables = " ".join(f"{k}={v}" for k, v in self.env_vars.items())
            cmd = f"export {variables} && {cmd}"
        return cmd


@dataclass(frozen=True)
class AllocationBuilder:
    """Allocation builder class."""

    config: Dict[str, Any]

    @classmethod
    def from_config_path(cls, path: Path) -> "AllocationBuilder":
        """Construct an allocation builder from file."""
        return cls(load_yaml(path))

    def get_allocation(
        self, scaling_mode: Dict[str, Any], scaling_value: str, substitutions: Dict[str, str]
    ) -> Allocation:
        """Get an allocation based on scaling mode and scaling value."""
        scaling_mode = self.config["resources"][scaling_mode]
        if scaling_value not in scaling_mode:
            scaling_value = "default"

        config = deepcopy(scaling_mode[scaling_value])

        for key, value in substitutions.items():
            config["salloc"] = config["salloc"].replace(key, value)
            config["jobname"] = config["jobname"].replace(key, value)

        env_vars = config["env_vars"]
        for var_name, var_value in env_vars.items():
            if var_value in substitutions:
                env_vars[var_name] = env_vars[var_name].replace(var_value, substitutions[var_value])

        return Allocation.from_config(config)


def _escape_single_quotes(value):
    """Return the given string after escaping the single quote character."""
    return value.replace("'", "'\\''")
