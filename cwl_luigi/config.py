"""Config definitions."""

from typing import Literal

from cwl_luigi.common import CustomBaseModel


class Config(CustomBaseModel):
    """Config base class."""

    def update(self, other):
        """Update config with other config."""
        assert type(other) is type(self), "Incompatible config types."
        return self.from_dict(self.to_dict() | other.to_dict())


class RemoteConfig(Config):
    """Remote host config."""

    host: str


class SlurmConfig(Config):
    """Standard set of Slurm configuration parameters."""

    chdir: str | None = None
    account: str | None = None
    partition: str = "prod"
    nodes: int | None = None
    qos: Literal["", "normal", "longjob", "bigjob", "jenkins"] | None = None
    ntasks: int | None = None
    ntasks_per_node: int | None = None
    cpus_per_task: int | None = None
    mpi: str | None = None
    mem: int | None = None
    mem_per_cpu: str | None = None
    constraint: str = "cpu"
    exclusive: bool = False
    time: str | None = None
    dependency: str | None = None
    job_name: str | None = None
    output: str | None = None
    array: str | None = None
    wait: bool = False

    def to_command_parameters(self) -> list[str]:
        """Convert the config into slurm parameters."""
        parameters = []
        for key, value in self.to_dict(by_alias=True).items():
            if value is None:
                continue
            key = key.replace("_", "-")
            if isinstance(value, bool):
                if value:
                    param = f"--{key}" if isinstance(value, bool) and value else f"--{key}={value}"
                else:
                    continue
            else:
                param = f"--{key}={value}"
            parameters.append(param)

        return parameters
