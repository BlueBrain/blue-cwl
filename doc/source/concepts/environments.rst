Environments
============

Environments allow specifying how to make the command line tool available as an executable.

.. attention::
    Environment definitions is an extension to the CWL standard and only a valid section in blue-cwl definitions.

Specifying a custom module
--------------------------

To use a custom module, you can add an ``environments.yaml`` file with the following content:

.. code-block:: yaml

    version: 1
    env_config:
      brainbuilder:
        env_type: MODULE
        modules:
        - archive/2022-03
        - brainbuilder/0.17.0

If needed, you can specify also the ``modulepath`` as in the following example:

.. code-block:: yaml

    version: 1
    env_config:
      brainbuilder:
        env_type: MODULE
        modulepath: /gpfs/bbp.cscs.ch/ssd/apps/bsd/modules/_meta
        modules:
        - archive/2022-03
        - brainbuilder/0.17.0


Specifying a custom virtual environment
---------------------------------------

To use an existing custom virtual environment, you can add an ``environments.yaml`` file with the following content:

.. code-block:: yaml

    version: 1
    env_config:
      brainbuilder:
        env_type: VENV
        path: /absolute/path/to/existing/venv/

The configuration key ``path`` should be set to the directory containing the existing python virtual environment.
Alternatively, it may be set to any existing file, that will be sourced before executing the commands in the rule.

If needed, it's possible to specify some optional keys as in the following example:

.. code-block:: yaml

    version: 1
    env_config:
      brainbuilder:
        env_type: VENV
        path: /absolute/path/to/existing/venv/
        modulepath: /path/to/spack/modules
        modules:
        - archive/2023-02
        - hpe-mpi/2.25.hmpt

.. warning::

    In most cases, you shouldn't load modules that modify PYTHONPATH to avoid issues with conflicting versions of the libraries.


Specyfing an Apptainer/Singularity image
----------------------------------------

To use a custom Apptainer/Singularity image, you can add an ``environments.yaml`` file with the following content:

.. code-block:: yaml

    version: 1
    env_config:
      brainbuilder:
        env_type: APPTAINER
        image: /path/to/apptainer/image.sif

If needed, it's possible to specify some optional keys as in the following example:

.. code-block:: yaml

    version: 1
    env_config:
      brainbuilder:
        env_type: APPTAINER
        image: /path/to/apptainer/image.sif
        executable: singularity
        options: "--cleanenv --containall --bind $TMPDIR:/tmp,/gpfs/bbp.cscs.ch/project"
        modulepath: /path/to/spack/modules
        modules:
        - archive/2022-06
        - singularityce
