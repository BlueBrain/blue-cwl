import tempfile
import os
import traceback
from click.testing import CliRunner
from contextlib import contextmanager

from cwl_workflow.__main__ import main


def run_cli_command(cmd, tmp_dir):
    tmp_dir.mkdir()

    runner = CliRunner()

    with runner.isolated_filesystem(temp_dir=tmp_dir):
        result = runner.invoke(main, cmd, catch_exceptions=False)

    assert result.exit_code == 0, "".join(traceback.format_exception(*result.exc_info))


@contextmanager
def cwd(path):
    """Context manager to temporarily change the working directory."""
    original_cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(original_cwd)
