Python API
==========

What is a variant?
------------------

A generator's variant is an executable tool or workflow that is defined using the Common Workflow Language (CWL).

It is the smallest unit of workflow execution that has NEXUS resources as inputs and outputs.

.. note::
    A Workflow may define many communicating intermediate sub-tasks with local targets, however, similar to a CommandLine tool its inputs and outputs should be NEXUS resources.

How are variants identified?
----------------------------

A generator's variant is uniquely identified by a tuple of (generator-name, variant-name, variant-version). For example (cell_position, neurons_cell_position, v1) is a unique variant of placing neuronal cells.

Local Variant definitions
-------------------------

Local variant definitions are split into two categories:

- Local definitions from file
- Definition in the registry

A variant can be loaded from a file as long as the identifying tuple (genertor_name, variant_name, variant_version) is provided:

.. code-block:: python

    v = Variant.from_file("variant.cwl", generator_name, variant_name, variant_version)

.. important::

    Contrary to the other ways of loading a variant, a local definition file does not include the identifying variant tuple, therefore the user must provide it when initializing from file.

The registry of generator definitions is stored within the blue-cwl package and is either available in the site-packages, if blue-cwl is pip installed in normal mode, or at the local repository location if pip installed in development/editable mode.

.. code-block:: python

    v = Variant.from_registry(generator_name, variant_name, variant_version)


Remote Variant definitions
--------------------------

Remote variant definitions are stored in NEXUS as json-ld resources.

.. code-block:: json

    {
      "@context": [
        "https://bluebrain.github.io/nexus/contexts/metadata.json",
        "https://bbp.neuroshapes.org"
      ],
      "@id": "resource-id",
      "@type": "Variant",
      "distribution": {
        "@type": "DataDownload",
        "contentUrl": "file-url",
        "encodingFormat": "application/cwl",
        "name": "definition.cwl"
      },
      "generatorName": "mmodel",
      "name": "mmodel|neurons_mmodel|v3",
      "variantName": "neurons_mmodel",
      "version": "v3"
    }

where ``generatorName``, ``variantName``, and ``version`` properties are mandatory and are used to search the variants in that project.

The variant registered in that resource can be fetched with:

.. code-block:: python

    v = Variant.from_id(resource_id, base=nexus_base, org=nexus_org, proj=nexus_proj, use_auth=nexus_token)

Or via searching the NEXUS project with:

.. code-block:: python

    v = Variant.from_search(generator_name, variant_name, variant_version, base=nexus_base, org=nexus_org, proj=nexus_proj, use_auth=nexus_token)


.. attention::

    The combination of generatorName, variantName, and version properties triplet is assumed unique within a project. There should be no second resource with the exact same triplet.

