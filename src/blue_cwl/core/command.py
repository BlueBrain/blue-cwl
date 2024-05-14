"""Local/Remote command building and execution."""

import logging
import os
import re
import subprocess
from pathlib import Path

from blue_cwl.core.config import SlurmConfig

L = logging.getLogger(__name__)


def run_command(
    str_command: str,
    *,
    process_constructor=subprocess.Popen,
    masked_vars: list[str] | None = None,
    redirect_to: str | os.PathLike[str] | None = None,
) -> None:
    """Execute a command using the process constructed from the process_constructor.

    Args:
        str_command: command string to execute.
        process_constructor: The process constructor to use. Default is the subprocess.Popen.
        masked_vars: Optional var names to mask when the command is printed.
    """
    if masked_vars is None:
        masked_vars = []

    command = f"set -e && {str_command}"

    process = process_constructor(
        ["bash", "-l"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if redirect_to:
        L.info("\n\nOutput redirected to %s\n", redirect_to)
        command = _redirect_to_file(command, redirect_to)
        masked_command = _mask(command, masked_vars)
        _write_command_to_file(masked_command, redirect_to)

    else:
        masked_command = _mask(command, masked_vars)

    L.info("Tool command:\n%s", masked_command)

    stdout, sterr = process.communicate(command.encode())

    _check_return_code(process, stdout, sterr, cmd=masked_command)

    return process


def _mask_token(string: str, name: str) -> str:
    """Mask the token value as ${name}."""
    return re.sub(rf"{name}=(\S+)", f"{name}=${name}", string)


def _mask(string: str, vars_to_mask: list[str]) -> str:
    """Mask vars in string."""
    for masked_var in vars_to_mask:
        string = _mask_token(string, masked_var)
    return string


def _check_return_code(process, stdout=None, stderr=None, cmd=None):
    """Check the return code of the process."""
    if process.returncode:
        msg = f"Process failed with exit code {process.returncode}"
        if stdout:
            msg += f"\nSTDOUT:{stdout.decode().strip()}"

        if process.stdout and not process.stdout.closed and process.stdout.readable():
            msg += f"\nSTDOUT:{process.stdout.read().decode().strip()}"
        if stderr:
            msg += f"\nSTDERR:{stderr.decode().strip()}"
        if process.stderr and not process.stderr.closed and process.stderr.readable():
            msg += f"\nSTDERR:{process.stderr.read().decode().strip()}"
        if cmd:
            msg += f"\nCOMMAND:\n{cmd}"
        raise RuntimeError(msg)


def build_salloc_command(slurm_config: SlurmConfig, cmd: str):
    """Build salloc command."""
    params = slurm_config.to_command_parameters()
    str_params = " ".join(params)
    return f"stdbuf -oL -eL salloc {str_params} srun {cmd}"


def _escape_single_quotes(value):
    """Return the given string after escaping the single quote character."""
    return value.replace("'", "'\\''")


def _write_command_to_file(cmd, filepath):
    text = f"COMMAND:\n{cmd}"
    Path(filepath).write_text(text, encoding="utf-8")


def _redirect_to_file(cmd, filepath):
    return f"( {cmd} ) >> {filepath} 2>&1"
