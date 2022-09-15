import pytest
from unittest.mock import Mock, patch
from cwl_luigi import environment as tested
import cwl_luigi.environment
from cwl_luigi import constants


@pytest.fixture
def config_module():
    return {"modulepath": "/modulepath", "modules": ["archive/2022-03", "circuit-build/2.0.0"]}


MOCK_ALLOCATION = Mock(shell_command=lambda cmd: f"salloc -J X srun sh -c '{cmd}'")


MODULES_PREAMBLE = (
    f". {constants.MODULES_ENABLE_PATH} && "
    "module purge && "
    "export MODULEPATH=/modulepath && "
    "module load archive/2022-03 circuit-build/2.0.0 && "
    "echo MODULEPATH=/modulepath && "
    "module list"
)


def test_build_module_cmd__wout_allocation(config_module):

    cmd = "echo X"
    res = tested._build_module_cmd(cmd=cmd, config=config_module, allocation=None)
    assert res == f"{MODULES_PREAMBLE} && echo X"


def test_build_module_cmd__with_allocation(config_module):

    cmd = "echo X"
    res = tested._build_module_cmd(cmd=cmd, config=config_module, allocation=MOCK_ALLOCATION)
    assert res == f"{MODULES_PREAMBLE} && salloc -J X srun sh -c 'echo X'"


def test_build_apptainer_cmd__defaults():

    cmd = "echo X"
    config = {"image": "path"}
    res = tested._build_apptainer_cmd(cmd=cmd, config=config, allocation=None)

    assert res == (
        f". {constants.MODULES_ENABLE_PATH} && "
        "module purge && "
        f"module use {constants.APPTAINER_MODULEPATH} && "
        "module load unstable singularityce && "
        "singularity --version && "
        f"singularity exec {constants.APPTAINER_OPTIONS} {constants.APPTAINER_IMAGEPATH}/path "
        'bash <<EOF\ncd "$(pwd)" && echo X\nEOF\n'
    )


def test_build_apptainer_cmd__with_allocation():

    cmd = "echo X"
    config = {"image": "path"}

    res = tested._build_apptainer_cmd(cmd=cmd, config=config, allocation=MOCK_ALLOCATION)

    assert res == (
        f". {constants.MODULES_ENABLE_PATH} && "
        "module purge && "
        f"module use {constants.APPTAINER_MODULEPATH} && "
        "module load unstable singularityce && "
        "singularity --version && "
        "salloc -J X srun sh -c '"
        f"singularity exec {constants.APPTAINER_OPTIONS} {constants.APPTAINER_IMAGEPATH}/path "
        'bash <<EOF\ncd "$(pwd)" && echo X\nEOF\n'
        "'"
    )


def test_build_venv_cmd__wout_allocation():

    cmd = "echo X"
    config = {"path": "path"}
    res = tested._build_venv_cmd(cmd=cmd, config=config, allocation=None)
    assert res == ". path/bin/activate && echo X"


def test_build_venv_cmd__with_allocation():

    cmd = "echo X"
    config = {"path": "path"}
    res = tested._build_venv_cmd(cmd=cmd, config=config, allocation=MOCK_ALLOCATION)
    assert res == ". path/bin/activate && salloc -J X srun sh -c 'echo X'"


def test_environment__shell_command__module():

    config = {"env_type": "MODULE", "modules": ["unstable", "foo"]}
    environment = tested.Environment(config=config)
    res = environment.shell_command(cmd="echo X", allocation=MOCK_ALLOCATION)
    assert "unstable foo" in res
    assert "salloc" in res


def test_environment__shell_command__apptainer():

    config = {
        "env_type": "APPTAINER",
        "image": "singularity-image-path",
    }
    environment = tested.Environment(config=config)
    res = environment.shell_command(cmd="echo X", allocation=MOCK_ALLOCATION)
    assert "singularity-image-path" in res
    assert "salloc" in res


def test_environment__shell_command__venv():

    config = {
        "env_type": "VENV",
        "path": "venv-path",
    }
    environment = tested.Environment(config=config)
    res = environment.shell_command(cmd="echo X", allocation=MOCK_ALLOCATION)
    assert "venv-path" in res
    assert "salloc" in res
