CWL Executors
=============

CWL executors allow specifying the local or remote executor that will launch the command.

.. attention::
    Executors definitions is an extension to the CWL standard and only a valid section in blue-cwl definitions.

Local vs. Remote executors
--------------------------

A LocalExecutor is the default executor in a CWL specification and will be assigned if no `executor` section is present in the definition. To explicitly spefify one:

.. code:: yaml

    executor:
      type: local
      env_vars:
        FOO: foo
        BAR: bar

A RemoteExecutor requires a host to be executed via ssh. A `remote_config` section needs to be added in the executor configuration as follows:

.. code:: yaml

    executor:
      type: slurm
      remote_config:
        host: bbpv1.epfl.ch

SallocExecutor
~~~~~~~~~~~~~~

A SallocExecutor is a slurm executor that builds a command using a blocking salloc directive.

.. code:: yaml

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

For the full slurm config parameters see :class:`blue_cwl.core.config.SlurmConfig`
