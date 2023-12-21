from cwl_registry.variant import Variant


def test_pipeline_definitions_inputs_outputs():
    """Test that output names in the pipeline are compatible with their respective input names
    in the subsequent tool."""
    tools = [
        ("cell_composition", "cell_composition_manipulation", "v1"),
        ("cell_position", "neurons_cell_position", "v1"),
        ("mmodel", "neurons_mmodel", "v1"),
        ("memodel", "neurons_memodel", "v1"),
        ("connectome", "placeholder", "v1"),
        ("connectome_filtering", "synapses", "v1"),
    ]

    tools = [Variant.from_registry(g, v, r).tool_definition for g, v, r in tools]

    for prev_tool, curr_tool in zip(tools[:-1], tools[1:]):
        for output_name in prev_tool.outputs.keys():
            assert output_name in curr_tool.inputs
