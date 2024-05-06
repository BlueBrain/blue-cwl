import os
import yaml
import tempfile
from pathlib import Path

from cwl_luigi import utils as tested


def test_cwd():
    cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tdir:
        tdir = Path(tdir).resolve()

        with tested.cwd(tdir):
            assert os.getcwd() == str(tdir)
        assert os.getcwd() == str(cwd)
