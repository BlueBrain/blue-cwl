from pathlib import Path
import pytest
from cwl_registry.utils import load_json

DATA_DIR = Path(__file__).parent / "data"


PREFIX = "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model"


@pytest.fixture(scope="session")
def detailed_circuit_metadata():
    return load_json(DATA_DIR / "detailed_circuit_metadata.json")


@pytest.fixture(scope="session")
def circuit_config_file():
    return DATA_DIR / "circuit_config.json"


@pytest.fixture(scope="session")
def circuit_config(circuit_config_file):
    return load_json(circuit_config_file)


@pytest.fixture(scope="session")
def emodel_metadata():
    return load_json(DATA_DIR / "emodel_metadata.json")


@pytest.fixture(scope="session")
def emodel_distribution():
    return load_json(DATA_DIR / "emodel_distribution.json")


@pytest.fixture(scope="session")
def emodel_workflow_metadata():
    return load_json(DATA_DIR / "emodel_workflow_metadata.json")


@pytest.fixture(scope="session")
def emodel_config():
    return load_json(DATA_DIR / "placeholder_emodel_config_distribution.json")


@pytest.fixture(scope="session")
def mock_get_emodel():
    def _mock_get_emodel(entry_id, entry_data, *args, **kwargs):
        if entry_id == f"{PREFIX}/65d6a42a-ec6f-4a17-b1d2-b6ff1c7225b8?rev=2":
            return "AAA__GEN_mtype__GEN_etype__emodel"

        if entry_id == f"{PREFIX}/ff571ada-fc08-4e6b-9b5c-1c21ee22ccb2?rev=1":
            return "AAA__GIN_mtype__GIN_etype__emodel"

        if entry_id == f"{PREFIX}/af16d001-7e73-4b10-88ef-868567d77242?rev=1":
            return "ACAd1__L1_DAC__bNAC__emodel"

        if entry_id == f"{PREFIX}/292427f3-fbdc-4e26-9731-d89b114441b3?rev=1":
            return "ACAd1__L1_DAC__cNAC"

        if entry_id == f"{PREFIX}/f91eeb30-c6e7-40f8-8917-fd8007ec8917?rev=2":
            return "ACAd1__L1_DAC__bNAC__override"

        raise ValueError(entry_id)

    return _mock_get_emodel


@pytest.fixture(scope="session")
def materialized_emodel_config():
    return load_json(DATA_DIR / "materialized_placeholder_emodel_config.json")


@pytest.fixture(scope="session")
def me_model_config():
    return load_json(DATA_DIR / "me_model_config_distribution.json")


@pytest.fixture(scope="session")
def materialized_me_model_config_file():
    return DATA_DIR / "materialized_me_model_config.json"


@pytest.fixture(scope="session")
def materialized_me_model_config(materialized_me_model_config_file):
    return load_json(materialized_me_model_config_file)
