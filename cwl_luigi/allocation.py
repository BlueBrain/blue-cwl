"""Allocation related utilities."""
from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class Allocation:
    """Allocation configuration."""

    config: Dict[str, Any]

    def shell_command(self, cmd) -> str:
        """Get shell command combining the allocation config and the current cmd."""
        slurm_config = self.config
        jobname = slurm_config["jobname"]
        salloc = slurm_config["salloc"]
        skip_srun = slurm_config.get("skip_srun", False)
        cmd = _escape_single_quotes(cmd)
        cmd = f"salloc -J {jobname} {salloc} {'' if skip_srun else 'srun'} sh -c '{cmd}'"
        # set the environment variables if needed
        env_vars = slurm_config.get("env_vars")
        if env_vars:
            variables = " ".join(f"{k}={v}" for k, v in env_vars.items())
            cmd = f"export {variables} && {cmd}"
        return cmd


def _escape_single_quotes(value):
    """Return the given string after escaping the single quote character."""
    return value.replace("'", "'\\''")
