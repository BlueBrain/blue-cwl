from pathlib import Path

import pytest
import libsonata
from blue_cwl import validation as test_module
from blue_cwl.exceptions import CWLRegistryError


DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture(scope="module")
def nodes_file():
    return DATA_DIR / "nodes.h5"


def test_check_population_name_in_nodes(nodes_file):
    test_module.check_population_name_in_nodes("default", nodes_file)
    test_module.check_population_name_in_nodes("default2", nodes_file)

    with pytest.raises(CWLRegistryError, match="foo"):
        test_module.check_population_name_in_nodes("foo", nodes_file)


def test_check_properties_in_population(nodes_file):
    test_module.check_properties_in_population(
        population_name="default",
        nodes_file=nodes_file,
        property_names=["morphology", "mtype", "dynamics_params/holding_current"],
    )
    test_module.check_properties_in_population(
        population_name="default2",
        nodes_file=nodes_file,
        property_names=["morphology", "mtype", "dynamics_params/holding_current", "other2"],
    )


@pytest.mark.parametrize(
    "properties, expected",
    [
        (["foo"], "['foo']"),
        (["dynamics/foo"], "['dynamics/foo']"),
        (
            ["foo", "dynamics_params/holding_current", "dynamics_params/bar", "morphology"],
            "['foo', 'dynamics_params/bar']",
        ),
    ],
)
def test_check_properties_in_population__raises(nodes_file, properties, expected):
    with pytest.raises(CWLRegistryError, match=expected):
        test_module.check_properties_in_population(
            population_name="default",
            nodes_file=nodes_file,
            property_names=properties,
        )
