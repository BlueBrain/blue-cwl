CommandLineTool
===============

A CommandLineTool definition is a yaml file that follows the `Common Workflow Language specification <https://www.commonwl.org/v1.2/CommandLineTool.html>`_

A full example of a generator task defined as a CWL file is as follows:

.. code:: yaml

   cwlVersion: v1.2
    class: CommandLineTool

    id: me_type_property
    label: Morph-Electric type property generator

    baseCommand: ['blue-cwl', 'execute', 'neurons-cell-position']

    environment:
      env_type: MODULE
      modules:
        - unstable
        - brainbuilder
        - py-blue-cwl
      enable_internet: true

    executor:
      type: slurm
      slurm_config:
        partition: prod
        nodes: 1
        exclusive: true
        time: '8:00:00'
        account: proj134
      remote_config:
        host: bbpv1.epfl.ch
      env_vars:
        FOO: foo
        BAR: bar

    inputs:

        - id: region
          type: string
          inputBinding:
            prefix: --region

        - id: cell_composition
          type: NexusType
          inputBinding:
            prefix: --cell-composition

        - id: variant_config
          type: NexusType
          inputBinding:
            prefix: --variant-config

        - id: output_dir
          type: Directory
          inputBinding:
            prefix: --output-dir

    outputs:

        - id: partial_circuit
          type: NexusType
          doc: Circuit bundle with me-types and soma positions.
          outputBinding:
            glob: "partial-circuit.json"

NexusType
---------

A NexusType is a special type that is treated as a string or as a file.

.. attention::
    NexusType is an extension to the CWL standard and only a valid type in blue-cwl definitions.

A `NexusType`, when specified as an input, it's treated as a string, representing the nexus id of a resource, whereas as an output it is a file with the json-ld metadata of the registered resource at the end of the task.

.. note::
    An output NexusType resource file, being registered, always has an id.

NexusType outputs can be coerced to NexusType inputs when used in a workflow, by extracting the id from the json-ld metadata and passing it as a string input.

baseCommand
-----------

A `baseCommand` is a list of the CLI command that will be executed in the tool definition. Fixed arguments can be added there. CWL arguments section is not yet supported.

Inputs
------

A task's inputs in workflows can either be a NEXUS resource (NexusType) or regular CWL primitive types.

Given that task's may be executed in remote filesystems, local file paths are not preferred. However, if the filesystem is mounted it is also possible to work with files.

Outputs
-------

Workflow generator tasks should define nexus resources as outputs. These are picked by the workflow generators and are passed downstream as registred NEXUS resources for each step in the workflow.

Environment
-----------

For CWL environments please read :ref:`cwl-environments`.

Executor
--------

For CWL executors please read :ref:`cwl-executors`.
