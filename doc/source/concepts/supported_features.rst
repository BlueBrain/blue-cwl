Supported CWL features
======================

This section will list the CWL features that are supported by the blue-cwl implementation.

Parameter References
--------------------

CommandLineTool
~~~~~~~~~~~~~~~

Parameters references allow to dynamically reference input or output parameters.

.. code:: yaml

    cwlVersion: v1.2
    class: CommandLineTool
    baseCommand: [cp, -r]
    inputs:

      foo:
        type: string
        inputBindin: 1

      file:
        type: File
        inputBinding: 2

      dir:
        type: Directory
        inputBinding:
          position: 3

    outputs:

      r1:
        type: File
        outputBinding:
          glob: $(inputs.dir.basename)/file.txt

      r2:
        type: File
        outputBinding:
          glob: $(inputs.dir.path)/file.txt

      r3:
        type: string
        outputBinding:
          glob: $(inputs.foo)_bar

In the example above the `$()` notation is used to signify a parameter reference, which will be resolved during runtime.

Files and Directories are treated as objects that thave properties such as `path` or `basename`. 

Primitive types, such as strings, ints, and floats, are resolved by being substituted in the parameter reference.

Workflow
~~~~~~~~

An example of a Workflow with parameter references is as follows:

.. code:: yaml

    cwlVersion: v1.2
    class: Workflow

    inputs:
      input_file:
        type: File

      output_file:
        type: string

    outputs:
      out:
        type: File
        outputSource: s2/out_file

    steps:

      s1:
        in:
          source_file: input_file
          target_file:
            source: input_file
            valueFrom: $(self.path)_intm.txt
        out: [out_file]
        run: ./copy.cwl

      s2:
        in:
          source_file: s1/out_file
          target_file: output_file
        out: [out_file]
        run: ./copy.cwl

In the Workflow context parameter references are not specified the same way as in a ``CommandLineTool`` and need to follow the following format:

.. code:: yaml

  target_file:
    source: input_file
    valueFrom: $(self.path)_intm.txt

Defining a parameter reference is performed in two steps. First the source of the parameter reference is specified by pointing to a global ``Workflow`` input:

.. code:: yaml

    target_file:
      source: input_file

Or by specifying the step output to bring to the step's context:

.. code:: yaml

    target_file:
      source: s1/out_file

Subsequently the ``valueFrom`` directive is used to use the source that has been brought into context by referencing it via the keyword `self`.

.. code:: yaml

    target_file:
      source: s1/out_file
      valueFrom: $(self.basename)_foo.txt

Note that if the source is a ``File``, ``Directory``, or ``NexusType``, it needs to betreated as an object, whereas for the rest of the primitive types they are substituted as is.
For example, if the source was a string type:

.. code:: yaml

    target_file:
      source: name
      valueFrom: $(self)_foo.txt

Array Types
-----------

Array types allow specifying an input as an array of arguments, either by repeating the argument or by aggregating the array into a single argument separated by a symbol, usually a space.

CommandLineTool
~~~~~~~~~~~~~~~

Consider the following ``CommandLineTool`` definition with three different ways of defining type arrays:

.. code:: yaml

    cwlVersion: v1.2
    id: foo
    class: CommandLineTool
    inputs:
      filesA:
        type: File[]
        inputBinding:
          prefix: -A
          position: 1
      filesB:
        type:
          type: array
          items: File
          inputBinding:
            prefix: -B=
            separate: false
        inputBinding:
          position: 2
      filesC:
        type: File[]
        inputBinding:
          prefix: -C=
          itemSeparator: ","
          separate: false
          position: 4
    outputs:
      output_file:
        type: File
        outputBinding:
          glob: foo.txt
    baseCommand: touch foo.txt

Let's combine it with the following inputs:

The command from the definition above when combined with the following inputs:

.. code:: python

    input_values = {
        "filesA": ["a", "b", "c", "d"],
        "filesB": ["c", "d", "e", "f"],
        "filesC": ["g", "h"],
    }

.. code:: shell

  touch foo.txt -A a b c d -B=c -B=d -B=e -B=f -C=g,h

The default item separator is space. Therefore ``filesA`` input defined as type array of files notated as ``File[]``.

.. code:: yaml

    filesA:
      type: File[]
      inputBinding:
        prefix: -A
        position: 1

and this configuration will result in a command:

.. code:: shell

    -A a b c d

.. note::
    Following the original implementation, setting separate to true at this level it does not affect the final command.

To allow repeating an argument, a type ``array`` can be explicitly used, accompanied by an ``items`` key which specifies the type of the array.

.. code:: yaml

    filesB:
      type:
        type: array
        items: File
        inputBinding:
          prefix: -B=
          separate: false
      inputBinding:
        position: 2

and will result into:

.. code:: shell

    -B=c -B=d -B=e -B=f

Finally, an ``itemSeparator`` can be specified to contruct custom separation:

.. code:: yaml

      filesC:
        type: File[]
        inputBinding:
          prefix: -C=
          itemSeparator: ","
          separate: false
          position: 4

and the configuration will result into:

.. code:: shell

   -C=g,h 

Workflow
~~~~~~~~

Array types can also be used in workflows:

.. code:: yaml

    cwlVersion: v1.2
    class: Workflow

    id: array-types-workflow
    label: Use array types in a workflow


    inputs:
      filesA:
        type: File[]
      filesB:
        type: File[]
      one_file:
        type: File

    outputs:
      output_file:
        type: File
        outputSource: s1/output_file

    steps:

      - id: s0
        run: ./array_types.cwl
        in:
          filesA: filesA
          filesB: filesB
          filesC:
            valueFrom:
              - c_foo
              - c_bar
        out:
          - output_file

      - id: s1
        run: ./array_types.cwl
        in:
          filesA: filesA
          filesB:
            source:
              - s0/output_file
              - one_file
            valueFrom:
              - $(self[0].path)
              - $(self[1].path)
              - e.txt
              - f.txt
          filesC:
            source:
              - s0/output_file
              - one_file
        out:
          - output_file


To pass constants as an array for an array type:

.. code:: yaml

      filesC:
        valueFrom:
          - c_foo
          - c_bar


Array types can alse be combined with parameter references by specifying a source that is an array:

.. code:: yaml

      filesB:
        source:
          - s0/output_file
          - one_file
        valueFrom:
          - $(self[0].path)
          - $(self[1].path)
          - e.txt
          - f.txt

Naturally, when the source is specified as an array, ``self`` in ``valueFrom`` will also become an array to refer to the list of sources.
