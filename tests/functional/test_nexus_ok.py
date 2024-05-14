import os
import certifi
from blue_cwl.nexus import get_forge
from kgforge.core import Resource


def test_nexus_ci():
    forge = get_forge(
        nexus_base="https://staging.nise.bbp.epfl.ch/nexus/v1",
        nexus_org="bbp_test",
        nexus_project="studio_data_11",
    )

    r = forge.retrieve(
        "https://bbp.epfl.ch/neurosciencegraph/data/8586fff5-8212-424c-bb52-73b514e93422"
    )

    r2 = Resource(name="Test", type="TestType")
    forge.register(r2)
