from unittest.mock import patch
import pytest
from pathlib import Path
from cwl_luigi.core import executor as test_module
from cwl_luigi.core.config import SlurmConfig, RemoteConfig

from luigi.contrib.ssh import RemoteContext


@pytest.fixture
def local_executor():
    return test_module.LocalExecutor()


@pytest.fixture
def env_config():
    return {"env_type": "MODULE", "modules": ["unstable", "my-module"]}


@pytest.fixture
def expected_env_config_command():
    return (
        ". /etc/profile.d/modules.sh && "
        "module purge && "
        "export MODULEPATH=/gpfs/bbp.cscs.ch/ssd/apps/bsd/modules/_meta && "
        "module load unstable my-module"
    )


def test_local_executor__build_command(local_executor, env_config, expected_env_config_command):
    base_command = ["echo", "foo"]
    env_vars = {"foo": 1, "bar": 2}

    res = local_executor.build_command(
        base_command=base_command, env_vars=env_vars, env_config=env_config
    )

    assert res == f"{expected_env_config_command} && export foo=1 bar=2 && echo foo"


def test_local_executor__run(tmp_path, local_executor, env_config, expected_env_config_command):
    out_file = Path(tmp_path / "test.txt")

    base_command = ["touch", str(out_file)]
    env_vars = {"foo": 1, "bar": 2}

    with patch("cwl_luigi.core.executor.run_command") as patched:
        local_executor.run(base_command, env_vars=env_vars, env_config=env_config)
        patched.assert_called_once()


@pytest.fixture
def remote_executor():
    return test_module.RemoteExecutor(remote_config=RemoteConfig(host="bbpv1.epfl.ch"))


def test_remote_executor__build_command(remote_executor, env_config, expected_env_config_command):
    base_command = ["echo", "foo"]
    env_vars = {"foo": 1, "bar": 2}

    res = remote_executor.build_command(
        base_command=base_command, env_vars=env_vars, env_config=env_config
    )

    assert res == f"{expected_env_config_command} && export foo=1 bar=2 && echo foo"


def test_remote_executor__run(remote_executor):
    base_command = ["echo", "foo"]
    env_vars = {"foo": 1, "bar": 2}

    with patch("cwl_luigi.core.executor.run_command") as patched:
        remote_executor.run(base_command, env_vars=env_vars)
        patched.assert_called_once()


@pytest.fixture
def salloc_executor():
    remote_config = RemoteConfig(host="bbpv1.epfl.ch")
    slurm_config = SlurmConfig(exclusive=True)
    return test_module.SallocExecutor(remote_config=remote_config, slurm_config=slurm_config)


def test_salloc_executor__run(salloc_executor, env_config, expected_env_config_command):
    base_command = ["echo", "foo"]

    env_vars = {"foo": 1, "bar": 2}

    res = salloc_executor.build_command(base_command, env_vars, env_config)

    assert res == f"{expected_env_config_command} && export foo=1 bar=2 && echo foo"


def test_salloc_executor__build_command(salloc_executor, env_config, expected_env_config_command):
    base_command = ["echo", "foo"]

    env_vars = {"foo": 1, "bar": 2}

    res = salloc_executor.build_command(base_command, env_vars=env_vars, env_config=env_config)

    assert res == (
        f"{expected_env_config_command} && "
        "export foo=1 bar=2 && "
        "stdbuf -oL -eL salloc --partition=prod --constraint=cpu --exclusive srun "
        "echo foo"
    )


def test_salloc_executor__run(salloc_executor, env_config, expected_env_config_command):
    base_command = ["echo", "foo"]
    env_vars = {"foo": 1, "bar": 2}

    with patch("cwl_luigi.core.executor.run_command") as patched:
        salloc_executor.run(base_command, env_vars=env_vars, env_config=env_config)
        patched.assert_called_once()
