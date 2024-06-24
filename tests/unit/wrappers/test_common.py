from unittest.mock import Mock

import pytest

from blue_cwl.wrappers import common as test_module
from blue_cwl import utils
from blue_cwl.exceptions import CWLWorkflowError


def test_write_entity_id_to_file(tmp_path):
    out_file = tmp_path / "response.json"

    mock = Mock()
    mock.__class__.__name__ = "Entity"
    mock.get_id.return_value = "entity-id"

    test_module.write_entity_id_to_file(entity=mock, output_file=out_file)

    res = utils.load_json(out_file)
    assert res == {"@id": "entity-id", "@type": "Entity"}

    mock.get_id.return_value = None

    with pytest.raises(CWLWorkflowError, match="Entity 'Entity' has no id."):
        test_module.write_entity_id_to_file(entity=mock, output_file=out_file)
