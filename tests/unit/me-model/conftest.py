from pathlib import Path
from unittest.mock import patch

import pytest
from blue_cwl.utils import load_json

from entity_management.emodel import EModel

DATA_DIR = Path(__file__).parent / "data"


PREFIX = "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model"


@pytest.fixture
def detailed_circuit_metadata():
    return load_json(DATA_DIR / "detailed_circuit_metadata.json")


@pytest.fixture
def circuit_config_file():
    return DATA_DIR / "circuit_config.json"


@pytest.fixture
def circuit_config(circuit_config_file):
    return load_json(circuit_config_file)


@pytest.fixture
def emodel_metadata():
    return load_json(DATA_DIR / "emodel_metadata.json")


@pytest.fixture
def extraction_targets_configuration_metadata():
    return load_json(DATA_DIR / "extraction_targets_configuration_metadata.json")


@pytest.fixture
def extraction_targets_configuration_distribution_file():
    return str(DATA_DIR / "extraction_targets_configuration_distribution.json")


@pytest.fixture
def emodel_pipeline_settings_metadata():
    return load_json(DATA_DIR / "emodel_pipeline_settings_metadata.json")


@pytest.fixture
def emodel_pipeline_settings_distribution_file():
    return str(DATA_DIR / "emodel_pipeline_settings_distribution.json")


@pytest.fixture
def fitness_calculator_configuration_metadata():
    return load_json(DATA_DIR / "fitness_calculator_configuration_metadata.json")


@pytest.fixture
def fitness_calculator_configuration_distribution_file():
    return str(DATA_DIR / "fitness_calculator_configuration_distribution.json")


@pytest.fixture
def emodel_script_metadata():
    return load_json(DATA_DIR / "emodel_script_metadata.json")


@pytest.fixture
def emodel_script_distribution_file():
    return str(DATA_DIR / "emodel_script_distribution.hoc")


@pytest.fixture
def emodel_configuration_metadata():
    return load_json(DATA_DIR / "emodel_configuration_metadata.json")


@pytest.fixture
def emodel_configuration_distribution():
    return load_json(DATA_DIR / "emodel_configuration_distribution.json")


@pytest.fixture
def neuron_morphology_metadata():
    return load_json(DATA_DIR / "neuron_morphology_metadata.json")


@pytest.fixture
def neuron_morphology_distribution_file():
    return str(DATA_DIR / "neuron_morphology_distribution.swc")


@pytest.fixture
def subcellular_model_script_metadata():
    return load_json(DATA_DIR / "subcellular_model_script_metadata.json")


@pytest.fixture
def subcellular_model_script_distribution_file():
    return str(DATA_DIR / "subcellular_model_script_distribution.mod")


@pytest.fixture
def emodel(
    emodel_metadata,
    emodel_distribution_file,
    emodel_workflow_metadata,
    emodel_workflow_distribution,
    extraction_targets_configuration_metadata,
    extraction_targets_configuration_distribution_file,
    emodel_pipeline_settings_metadata,
    emodel_pipeline_settings_distribution_file,
    fitness_calculator_configuration_metadata,
    fitness_calculator_configuration_distribution_file,
    emodel_script_metadata,
    emodel_configuration_metadata,
    emodel_configuration_distribution,
    neuron_morphology_metadata,
    neuron_morphology_distribution_file,
    subcellular_model_script_metadata,
    subcellular_model_script_distribution_file,
    emodel_script_distribution_file,
):
    def _load_by_id(resource_id, *args, **kwargs):
        """Mock metadata fetching."""

        # EModel
        if resource_id == "emodel-id":
            return emodel_metadata

        # EModelWorkflow
        if "e1bd0904-ec25-42c5-b887-4512bd82e8b9" in resource_id:
            return emodel_workflow_metadata

        # ExtractionTargetsConfiguration
        if "08776acd-ea8c-4301-bc25-1d3e2dd46287" in resource_id:
            return extraction_targets_configuration_metadata

        # EModelPipelineSettings
        if "20c9b89b-9fcf-4f65-8023-12f2f6cf61c4" in resource_id:
            return emodel_pipeline_settings_metadata

        # FitnessCalculatorConfiguration
        if "dedff50b-a008-4df9-b761-71c8d481a117" in resource_id:
            return fitness_calculator_configuration_metadata

        # EModelScript
        if "c644ac45-a3d1-4af5-93b2-243df7ba6925" in resource_id:
            return emodel_script_metadata

        # EModelConfiguration
        if "7c555f0e-9ff9-475f-b023-19827aa1af90" in resource_id:
            return emodel_configuration_metadata

        # NeuronMorphology
        if "b173b1b2-3303-4e0d-93e7-fae3d2c0c2ac" in resource_id:
            return neuron_morphology_metadata

        # SubCellularModelScript
        if "5db8b922-e0a1-4ab8-9674-4998a5bebb3b" in resource_id:
            return subcellular_model_script_metadata

        raise ValueError(resource_id)

    def _download_file(url, *args, **kwargs):
        """Mock metadata fetching."""

        # EModel
        if "1d95404d-9e48-4fa1-bb9f-b0e2cd8dde53" in url:
            return emodel_distribution_file

        # ExtractionTargetsConfiguration
        if "b15ecf3a-f5ee-4a3e-aec7-f1af85b841c9" in url:
            return extraction_targets_configuration_distribution_file

        # EModelPipelineSettings
        if "033dcbd2-4d96-4189-afcb-f73e7b5f60f4" in url:
            return emodel_pipeline_settings_distribution_file

        # FitnessCalculatorConfiguration
        if "fba43245-e0a9-4226-95c5-7b63869db08a" in url:
            return fitness_calculator_configuration_distribution_file

        # EModelScript
        if "6d442738-f63c-49ac-8e09-30ab51f2cfad" in url:
            return emodel_script_distribution_file

        # NeuronMorphology
        if "625980da-a0be-4fff-8779-afe8eec0e84b" in url:
            return neuron_morphology_distribution_file

        # SubCellularModelScript
        if "9b9873ab-49d0-4c0d-8be3-5ac95c1ebc8c" in url:
            return subcellular_model_script_distribution_file

        raise ValueError(url)

    def _file_as_dict(url, *args, **kwargs):
        """Mock file_as_dict fetching."""

        # EModelWorkflow
        if "482b6a89-ae47-4572-8692-9adce7b0b7ed" in url:
            return emodel_workflow_distribution

        # EModelConfiguration
        if "247cf4b9-bc97-48f8-bcf8-8b033d7796a6" in url:
            return emodel_configuration_distribution

        raise ValueError(url)

    with (
        patch("entity_management.nexus.load_by_id", side_effect=_load_by_id),
        patch("entity_management.nexus.download_file", side_effect=_download_file),
        patch("entity_management.nexus.file_as_dict", side_effect=_file_as_dict),
    ):
        yield EModel.from_id("emodel-id")


@pytest.fixture
def emodel_distribution_file():
    return str(DATA_DIR / "emodel_distribution.json")


@pytest.fixture
def emodel_distribution(emodel_distribution_file):
    return load_json(emodel_distribution_file)


@pytest.fixture
def emodel_workflow_metadata():
    return load_json(DATA_DIR / "emodel_workflow_metadata.json")


@pytest.fixture
def emodel_workflow_distribution_file():
    return str(DATA_DIR / "emodel_workflow_distribution.json")


@pytest.fixture
def emodel_workflow_distribution(emodel_workflow_distribution_file):
    return load_json(emodel_workflow_distribution_file)


@pytest.fixture
def emodel_config():
    return load_json(DATA_DIR / "placeholder_emodel_config_distribution.json")


@pytest.fixture
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


@pytest.fixture
def materialized_emodel_config():
    return load_json(DATA_DIR / "materialized_placeholder_emodel_config.json")


@pytest.fixture
def me_model_config():
    return load_json(DATA_DIR / "me_model_config_distribution.json")


@pytest.fixture
def materialized_me_model_config_file():
    return DATA_DIR / "materialized_me_model_config.json"


@pytest.fixture
def materialized_me_model_config(materialized_me_model_config_file):
    return load_json(materialized_me_model_config_file)
