"""Shell command building for luigi workflow."""
import copy
import logging
import re
import subprocess
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from luigi.contrib.ssh import RemoteContext

from cwl_luigi.allocation import Allocation
from cwl_luigi.environment import Environment

L = logging.getLogger(__name__)


@dataclass(frozen=True)
class Command:
    """Executable command dataclass.

    Attributes:
        base_command: The base command without arguments as a list. For example '[git, commit]'.
        named_arguments: Named arguments in the command. Example: {"--myvar": 5}
        positional_arguments:
            List of unsorted positional arguments. Example: [(2, "bar"), (1, "foo")]
        environment: The runtime environment where the command can be executed.
    """

    base_command: List[str]
    named_arguments: Dict[str, str]
    positional_arguments: List[Tuple[int, str]]
    environment: Optional[Environment] = None
    allocation: Optional[Allocation] = None
    host_config: Optional[Dict[str, str]] = None

    def build(
        self,
        inputs: Dict[str, str],
        runtime_arguments: Optional[List[str]] = None,
    ) -> str:
        """Construct the final command.

        Args:
            inputs: A dictionary with the input names and values.
            runtime_arguments: Extra arguments to add during runtime.

        Returns: The command as a string.
        """
        command = copy.deepcopy(self.base_command)
        hack_map = {"nexus_base": "kg_base", "nexus_project": "kg_proj", "nexus_org": "kg_org"}

        for prefix, argument in self.named_arguments.items():
            if argument in hack_map:
                command.extend((prefix, inputs[hack_map[argument]]))
            else:
                command.extend((prefix, inputs[argument]))

        L.debug("Command with named arguments: %s", command)

        for _, argument in sorted(self.positional_arguments):
            command.append(inputs[argument])

        L.debug("Command with positional arguments: %s", command)

        if runtime_arguments:
            command.extend(runtime_arguments)
            L.debug("Command with runtime arguments: %s", command)

        str_command = " ".join(command)

        if self.environment:
            str_command = self.environment.shell_command(
                cmd=str_command, allocation=self.allocation
            )

        L.debug("Final command: %s", str_command)

        return str_command

    def execute(self, command: str):
        """Execute the command."""
        return run_command(command, host_config=self.host_config)


def run_command(command: str, host_config=None):
    """Run command."""
    if host_config:
        return _run_remote_salloc_command(command, host_config["host"])
    else:
        raise NotImplementedError


class _PPCMD:
    """Lazy print command object."""

    def __init__(self, cmd):
        self.cmd = cmd

    def __str__(self):
        """Format cmd."""
        # sanitize sensitive info from showing up in the logs
        return "\n".join(
            re.sub(r"export NEXUS_TOKEN=.+$|export KC_SCR=.+$", "#", s)
            for s in self.cmd.split("\n")
        )


@contextmanager
def _run_remote_salloc_command(command, host):

    command = f"set -e && {command}"

    L.debug("host: %s command:\n%s", host, _PPCMD(command))
    process = RemoteContext(host).Popen(
        ["bash -l"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    process.stdin.write(command.encode())
    process.stdin.close()

    process.wait()

    if process.poll() is not None:
        _check_return_code(process, cmd=str(_PPCMD(command)))

    return process


def _run_cmd(host, cmd, capture_output=False):
    """Run cmd on the host and by default return the result.

    Returns:
        Result of the command stripped.
    """
    L.debug("host: %s command:\n%s", host, _PPCMD(cmd))
    remote = RemoteContext(host)
    if capture_output:
        process = remote.Popen(
            ["bash", "-l"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate(cmd.encode())
        _check_return_code(process, stdout, stderr)
        return stdout.decode().strip()
    else:
        process = remote.Popen(["bash", "-l"], stdin=subprocess.PIPE)
        stdout, stderr = process.communicate(cmd.encode())
        _check_return_code(process, stdout, stderr)
        return None


def _check_return_code(process, stdout=None, stderr=None, cmd=None):
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
