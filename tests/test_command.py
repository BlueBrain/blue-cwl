import re
from unittest.mock import Mock

import pytest
from cwl_luigi import command as test_module
from cwl_luigi.config import SlurmConfig


def test_mask_token__single():
    string = "TOKENA=aasd.asdf-gdf.gd-f"
    res = test_module._mask_token(string, "TOKENA")
    assert res == "TOKENA=$TOKENA"


def test_mask_token__start():
    string = "TOKENA=aasd.asdf-gdf.gd-f TOKENB=sdsade"
    res = test_module._mask_token(string, "TOKENA")
    assert res == "TOKENA=$TOKENA TOKENB=sdsade"


def test_mask_token__end():
    string = "TOKENB=sdsade TOKENA=aasd.asdf-gdf.gd-f"
    res = test_module._mask_token(string, "TOKENA")
    assert res == "TOKENB=sdsade TOKENA=$TOKENA"


def test_mask_token__inside():
    string = "TOKENB=sdsade TOKENA=aasd.asdf-gdf.gd-f TOKENC=sdsade"
    res = test_module._mask_token(string, "TOKENA")
    assert res == "TOKENB=sdsade TOKENA=$TOKENA TOKENC=sdsade"


def test_mask_token__multiple():
    string = "TOKENB=sdsade TOKENA=aasd.asdf-gdf.gd-f TOKENC=sdsade TOKENA=aasd.asdf-gdf.gd-f"
    res = test_module._mask_token(string, "TOKENA")
    assert res == "TOKENB=sdsade TOKENA=$TOKENA TOKENC=sdsade TOKENA=$TOKENA"


def test_mask():
    string = "A=12324 NEXUS_TOKEN=aasd.asdf-gdf.gd-f B=3434 KC_SCR=ssda.3243-dfgr C=43534"
    res = test_module._mask(string, vars_to_mask=["NEXUS_TOKEN", "KC_SCR"])
    assert res == "A=12324 NEXUS_TOKEN=$NEXUS_TOKEN B=3434 KC_SCR=$KC_SCR C=43534"


def test_escape_single_quotes():
    """Test escaping single quotes in salloc command."""
    result = test_module._escape_single_quotes("foo's bar")
    assert result == r"foo'\''s bar"


def test_build_salloc_command():
    config = SlurmConfig.from_dict(
        {
            "ntasks": 1,
            "job_name": "my-job",
            "time": "1:00:00",
            "wait": False,
        }
    )

    cmd = "echo X"

    result = test_module.build_salloc_command(config, cmd)

    assert result == (
        "stdbuf -oL -eL salloc "
        "--partition=prod "
        "--ntasks=1 "
        "--constraint=cpu "
        "--time=1:00:00 "
        "--job-name=my-job "
        "srun echo X"
    )


def test_check_return_code__pass():
    mock = Mock()
    mock.returncode = 0

    test_module._check_return_code(mock)


def test_check_return_code__error():
    process = Mock()
    process.returncode = 1

    stdout = Mock()
    stdout.read.return_value = "my-stdout".encode()
    stdout.closed = False
    stdout.readable.return_value = True

    stderr = Mock()
    stderr.read.return_value = "my-stderr".encode()
    stderr.closed = False
    stderr.readable.return_value = True

    process.stdout = stdout
    process.stderr = stderr

    expected = (
        "Process failed with exit code 1\n"
        "STDOUT:my-stdout\n"
        "STDERR:my-stderr\n"
        "COMMAND:\n"
        "my-command"
    )
    with pytest.raises(RuntimeError, match=expected):
        test_module._check_return_code(process, cmd="my-command")


def test_check_return_code__error_2():
    process = Mock()
    process.returncode = 1
    process.stdout = None
    process.stderr = None

    stdout = "my-stdout".encode()
    stderr = "my-stderr".encode()

    expected = (
        "Process failed with exit code 1\n"
        "STDOUT:my-stdout\n"
        "STDERR:my-stderr\n"
        "COMMAND:\n"
        "my-command"
    )
    with pytest.raises(RuntimeError, match=expected):
        test_module._check_return_code(process, cmd="my-command", stdout=stdout, stderr=stderr)
