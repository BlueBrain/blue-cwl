import pytest
import dataclasses
from pathlib import Path
from cwl_luigi import cwl_builder as tested
from cwl_luigi.utils import load_json

DATA_DIR = Path(__file__).parent / "data" / "ui"
