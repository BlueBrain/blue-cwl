CLI Wrappers
============

``blue-cwl`` wrappers are helper command line interfaces for the various stages of the workflow. The most common categories of wrappers are:

- staging
- registering
- tools

Staging Wrappers
----------------

Staging CLI wrappers convert NEXUS resources to local files or directories, which can be then used downstream.
They decouple comunication with a database and tool execution that most of times works with local files or directories.

Although there is no limitation on how many staging tasks a workflow may have, usually one is created that stages all NEXUS resources that are needed in the workflow.

.. note::
    Staging tasks need ``NEXUS_BASE``, ``NEXUS_ORG``, ``NEXUS_PROJ``, ``NEXUS_TOKEN`` in their environment in order to talk to NEXUS and download the resources.

Registering Wrappers
--------------------

Registering CLI wrappers do the opposite of staging ones: they convert local files or directories into NEXUS resources.

Although there is no limitation on how many registering tasks a workflow may have, usually one is created that registers the final resource that is assigned to the workflow main output.

.. note::
    Registering tasks need ``NEXUS_BASE``, ``NEXUS_ORG``, ``NEXUS_PROJ``, ``NEXUS_TOKEN`` in their environment in order to talk to NEXUS and download the resources.

Tool Wrappers
-------------

Tool wrappers are CLIs that wrap a python tool so that it can readily be used in a workflow.
They are rarely needed, however sometimes it is more convenient to create a tailored CLI for a workflow task, rather than using the tool's existing one.

.. warning::
    Wrapping tools comes at a price: the tool needs to be imported in python and by extension become a direct dependency of ``blue-cwl``. It is therefore preferred to avoid increasing the dependencies if the tool can be invoked using :ref:`cwl-environments`.
