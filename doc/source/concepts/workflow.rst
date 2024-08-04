Workflow
========

A Workflow definition is one or more yaml files that follow the `Common Workflow Language specification <https://www.commonwl.org/v1.2/Workflow.html>`_

A full example of a generator task as a workflow CWL file is as follows:

.. code:: yaml

    cwlVersion: v1.2
    class: Workflow

    id: workflow_me_type_property
    label: mock-generator-workflow

    inputs:

        - id: cell_composition
          type: NexusType

        - id: output_dir
          type: Directory

    outputs:

        partial_circuit:
            type: NexusType
            outputSource: register/resource_file

    steps:
        - id: stage
          run: ./staging.cwl
          in:
            cell_composition: cell_composition
            output_dir: output_dir
          out:
            - composition_file

        - id: place
          run: ./place.cwl
          in:
             cell_composition: stage/composition_file
             output_dir: output_dir
          out:
             - nodes_file

        - id: register
          run: ./register.cwl
          in:
            cell_composition: cell_composition
            nodes_file: place/nodes_file
          out:
            - resource_file


Inputs
------

Workflow inputs are similar to CommandLineTool inputs, with the difference that only the id and the type of the input are specified. These inputs will then be used to be chanelled to the workflow steps where needed.

Outputs
-------

Workflow outputs utilize the `outputSource` key to specify the step and output file that the workflow will address as a final output.

Steps
-----

Each Workflow step connects the workflow inputs and outputs to a CommandLineTool handled by that step.

Mapping a workflow step to a CommandLineTool requires the following keys:

- id
- run
- in
- out

.. attention::
    Workflow step ids must be unique within a Workflow

run
~~~

The `run` key is used to specify either the path to the definition of the CommandLineTool or directly embed it.

When a Variant is registered as a NEXUS resource, all definitions specified as files will be embedded so that a single Workflow definition is registered. However, when added in the registry, relative paths in the same directory are allowed for readability.

.. note::
    Relative paths, e.g. ./file.cwl are resolved with respect to the location of the Workflow cwl file.

in
~~

Defines the input parameters to the workflow step. To channel the output of another step the id of the task should be followed by the output id separated by a forward slash as `task/output`.

If an output name is specified without a forward slash, then it is assumed a workflow global input is given.

out
~~~

The out section is a list where all the outputs that are needed within the worklow are listed. It is not necessary to specify all possible outputs that the CommandLineTool produces, only those that are going to be used by other tasks or the workflow global outputs.

