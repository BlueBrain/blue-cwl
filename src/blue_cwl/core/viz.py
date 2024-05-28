"""CWL visualization."""

import io
from pathlib import Path

import matplotlib.image as mpimg
import matplotlib.pyplot as plt
import pydot


def show_workflow_graph_image(cwl_workflow):
    """Show workflow graph."""
    graph = _build_workflow_graph(cwl_workflow)

    png_str = graph.create_png()  # pylint: disable=no-member
    # treat the DOT output as an image file
    sio = io.BytesIO()
    sio.write(png_str)
    sio.seek(0)
    img = mpimg.imread(sio)

    # plot the image
    plt.imshow(img, aspect="equal")
    plt.axis("off")
    plt.show()


def write_workflow_graph_image(cwl_workflow, filepath):
    """Write workflow graph image."""
    graph = _build_workflow_graph(cwl_workflow)
    fmt = Path(filepath).suffix[1:]
    graph.write(filepath, format=fmt)


def _build_workflow_graph(cwl_workflow):
    def file_node(name, label):
        return pydot.Node(
            name,
            label=label,
            fontsize=20,
            shape="note",
            color="gold",
            fillcolor="gold",
            style="filled",
        )

    def step_node(name, label):
        return pydot.Node(
            name,
            label=label,
            fontsize=30,
            shape="oval",
            color="aquamarine",
            fillcolor="aquamarine",
            style="filled",
        )

    graph = pydot.Dot(repr(cwl_workflow), graph_type="digraph")

    # add inputs as indepdenent nodes
    for name in cwl_workflow.inputs:
        node_name = f"inputs__{name}"
        graph.add_node(file_node(node_name, name))

    # add workflow steps as independent nodes
    for step in cwl_workflow.iter_steps():
        graph.add_node(step_node(step.id, step.id))
        for name in step.outputs:
            node_name = f"{step.id}__{name}"
            graph.add_node(file_node(node_name, name))
            graph.add_edge(pydot.Edge(step.id, node_name))

    for step in cwl_workflow.iter_steps():
        # use a set to avoid creating multiple arrows from
        # the same source. We want a rough idea of the workflow.
        visited = set()
        for inp in step.inputs.values():
            res = inp.split_source_output()

            if res is None:
                continue

            for source_name, output_name in res:
                visited.add(source_name)

                if source_name is None:
                    source_name = f"inputs__{output_name}"
                else:
                    source_name = f"{source_name}__{output_name}"

                if source_name not in visited:
                    visited.add(source_name)

                    graph.add_edge(
                        pydot.Edge(
                            source_name,
                            step.id,
                        )
                    )

    return graph
