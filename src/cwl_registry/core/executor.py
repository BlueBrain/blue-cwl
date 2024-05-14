"""CWL Executors."""

import abc
import subprocess

import luigi
from luigi.contrib.ssh import RemoteContext

from cwl_luigi.core import environment
from cwl_luigi.core.command import build_salloc_command, run_command
from cwl_luigi.core.common import CustomBaseModel
from cwl_luigi.core.config import RemoteConfig, SlurmConfig
from cwl_luigi.core.types import EnvVarDict

# Prevent warning for autoload_range deprecation
luigi.configuration.get_config().set("core", "autoload_range", "false")


class Executor(CustomBaseModel, abc.ABC):
    """Executor abstract class."""

    env_vars: dict[str, int | float | str] | None = None

    def build_command(
        self,
        base_command: list[str],
        *,
        env_vars: EnvVarDict | None = None,
        env_config: dict | None = None,
    ) -> str:
        """Build the executor's command.

        Args:
            base_command: The base tool command with arguments.
            env_vars: Optional external env vars to use.
            env_config: Tool executable location config.
        """
        str_command = " ".join(map(str, base_command))

        env_vars = self._build_env_vars(external_env_vars=env_vars)

        if env_vars:
            str_exports = "export " + " ".join(f"{key}={value}" for key, value in env_vars.items())
            str_command = f"{str_exports} && {str_command}"

        if env_config:
            str_command = environment.build_environment_command(str_command, env_config)

        return str_command

    def _build_env_vars(self, external_env_vars: EnvVarDict | None) -> EnvVarDict:
        """Construct env variables from internal and external entries."""
        return (self.env_vars or {}) | (external_env_vars or {})

    @abc.abstractmethod
    def run(
        self,
        command: str,
        *,
        redirect_to: str | None = None,
        masked_vars: list[str] | None = None,
        **kwargs,
    ) -> None:
        """Use executor to run command."""


class LocalExecutor(Executor):
    """Executor that is run locally via a single process."""

    def run(
        self,
        command: str,
        *,
        redirect_to: str | None = None,
        masked_vars: list[str] | None = None,
        **kwargs,
    ):
        """Simple local executor."""
        run_command(
            str_command=command,
            process_constructor=subprocess.Popen,
            redirect_to=redirect_to,
            masked_vars=masked_vars,
            **kwargs,
        )


class RemoteExecutor(Executor):
    """Executor that is run on a host."""

    remote_config: RemoteConfig

    def run(
        self,
        command: str,
        *,
        redirect_to: str | None = None,
        masked_vars: list[str] | None = None,
        **kwargs,
    ):
        """Simple local executor."""
        run_command(
            str_command=command,
            process_constructor=RemoteContext(host=self.remote_config.host).Popen,
            redirect_to=redirect_to,
            masked_vars=masked_vars,
            **kwargs,
        )


class SallocExecutor(RemoteExecutor):
    """Executor for salloc commands."""

    slurm_config: SlurmConfig

    def build_command(
        self,
        base_command: list[str],
        *,
        env_vars: EnvVarDict | None = None,
        env_config: dict | None = None,
    ) -> str:
        """Build the executor's command."""
        str_command = " ".join(map(str, base_command))

        str_command = build_salloc_command(self.slurm_config, str_command)

        env_vars = self._build_env_vars(external_env_vars=env_vars)

        if env_vars:
            str_exports = "export " + " ".join(f"{key}={value}" for key, value in env_vars.items())
            str_command = f"{str_exports} && {str_command}"

        if env_config:
            str_command = environment.build_environment_command(str_command, env_config)

        return str_command
