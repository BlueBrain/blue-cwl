from pathlib import Path
from unittest.mock import patch
import pytest

from cwl_registry.me_model import staging as test_module
from cwl_registry.utils import load_json

DATA_DIR = Path(__file__).parent / "data"


PREFIX = "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model"


# def test_materialize_emodel(emodel_metadata):
#    with patch("entity_management.nexus.load_by_id", return_value=emodel_metadata):
#        test_module._materialize_emodel(None, None, None, None)


def test_materialize_placeholder_emodel_config(
    tmp_path, emodel_config, materialized_emodel_config, mock_get_emodel
):
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    output_file = tmp_path / "output_file.json"

    with patch("cwl_registry.me_model.staging._materialize_emodel", side_effect=mock_get_emodel):
        res1 = test_module.materialize_placeholder_emodel_config(
            emodel_config,
            staging_dir=output_dir,
            output_file=output_file,
        )

    res2 = load_json(output_file)
    assert res1 == res2
    assert res1 == materialized_emodel_config


def test_materialize_placeholder_emodel_config__labels_only(
    tmp_path, emodel_config, mock_get_emodel
):
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    output_file = tmp_path / "output_file.json"

    with patch("cwl_registry.me_model.staging._materialize_emodel", side_effect=mock_get_emodel):
        res1 = test_module.materialize_placeholder_emodel_config(
            emodel_config,
            staging_dir=output_dir,
            labels_only=True,
            output_file=output_file,
        )
    res2 = load_json(output_file)

    assert res1 == res2
    assert res1 == {
        "AAA": {
            "GEN_mtype": {"GEN_etype": "AAA__GEN_mtype__GEN_etype__emodel"},
            "GIN_mtype": {"GIN_etype": "AAA__GIN_mtype__GIN_etype__emodel"},
        },
        "ACAd1": {"L1_DAC": {"bNAC": "ACAd1__L1_DAC__bNAC__emodel", "cNAC": "ACAd1__L1_DAC__cNAC"}},
    }


def test_materialize_me_model_config(
    tmp_path, me_model_config, emodel_config, materialized_me_model_config, mock_get_emodel
):
    staging_dir = tmp_path / "out"
    staging_dir.mkdir()

    output_file = staging_dir / "output_file.json"

    with (
        patch(
            "cwl_registry.me_model.staging.read_json_file_from_resource_id",
            return_value=emodel_config,
        ),
        patch("cwl_registry.me_model.staging._materialize_emodel", side_effect=mock_get_emodel),
    ):
        res1 = test_module.materialize_me_model_config(
            me_model_config,
            staging_dir=staging_dir,
            forge=None,
            output_file=output_file,
        )

    res2 = load_json(output_file)
    assert res1 == res2

    assert res1 == materialized_me_model_config


def test_materialize_me_model_config__empty_overrides(
    tmp_path, emodel_config, materialized_me_model_config, mock_get_emodel
):
    staging_dir = tmp_path / "out"
    staging_dir.mkdir()

    output_file = staging_dir / "output_file.json"

    me_model_config = {
        "variantDefinition": {
            "neurons_me_model": {"algorithm": "neurons_me_model", "version": "v1"}
        },
        "defaults": {
            "neurons_me_model": {
                "@id": "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/2ec96e9f-7254-44b5-bbcb-fdea3e18f110",
                "@type": ["PlaceholderEModelConfig", "Entity"],
            }
        },
        "overrides": {"neurons_me_model": {}},
    }

    with (
        patch(
            "cwl_registry.me_model.staging.read_json_file_from_resource_id",
            return_value=emodel_config,
        ),
        patch("cwl_registry.me_model.staging._materialize_emodel", side_effect=mock_get_emodel),
    ):
        res1 = test_module.materialize_me_model_config(
            me_model_config,
            staging_dir=staging_dir,
            forge=None,
            output_file=output_file,
        )

    assert res1["overrides"] == {"neurons_me_model": {}}
    assert res1["defaults"] == materialized_me_model_config["defaults"]