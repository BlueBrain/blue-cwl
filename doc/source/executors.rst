Executor Definitions
====================

Executor definitions allow specifying how a CommandLineTool will be executor.

Local Executor (Default)
------------------------


.. code-block:: yaml

   executor:
     type: local


SLURM Executor
--------------


.. code-block:: yaml

   executor:
     type: slurm
     slurm_config:
       partition: prod
       nodes: 1
       exclusive: true
       time: '8:00:00'
     remote_config:
       host: bbpv1.epf.ch
