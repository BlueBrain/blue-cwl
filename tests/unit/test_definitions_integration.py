from cwl_registry.variant import Variant


def test_pipeline_definitions_inputs_outputs():
    """Test that output names in the pipeline are compatible with their respective input names
    in the subsequent tool."""
    tools = [
        ("cell_composition", "cell_composition_manipulation"),
        ("cell_position", "neurons_cell_position"),
        ("placeholder", "emodel_assignment"),
        ("placeholder", "morphology_assignment"),
        ("connectome", "distance"),
        ("connectome_filtering", "synapses"),
    ]

    tools = [Variant.from_registry(g, v).tool_definition for g, v in tools]

    for prev_tool, curr_tool in zip(tools[:-1], tools[1:]):
        for output_name in prev_tool.outputs.keys():
            assert output_name in curr_tool.inputs
