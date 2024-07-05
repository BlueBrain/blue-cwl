# SPDX-License-Identifier: Apache-2.0

"""Testing resources."""

import inspect
import os
from contextlib import contextmanager
from unittest.mock import patch


@contextmanager
def cwd(path):
    """Context manager to temporarily change the working directory."""
    original_cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(original_cwd)


def patchenv(**envvars):
    """Patch function environment."""
    return patch.dict(os.environ, envvars, clear=True)


def check_arg_consistency(cli_command, function):
    """Check that command has the same arguments as the function."""
    cmd_args = set(p.name for p in cli_command.params)
    func_args = set(inspect.signature(function).parameters.keys())
    assert cmd_args == func_args, (
        "Command arguments are not matching function ones:\n"
        f"Command args : {sorted(cmd_args)}\n"
        f"Function args: {sorted(func_args)}"
    )
