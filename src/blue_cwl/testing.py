# SPDX-License-Identifier: Apache-2.0

"""Testing resources."""

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
