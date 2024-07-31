
.. _installation:

Installation
============

blue-cwl can be easily installed using pip:

.. code-block:: console

    pip install blue-cwl


Optional Dependencies
---------------------

blue-cwl has the following optional dependencies:

Visualization
~~~~~~~~~~~~~

To install optional dependencies for visualization:

.. code-block:: console

    pip install blue-cwl[viz]

Or install dependencies manually with:

.. code-block:: console

    pip install matplotlib pydot

pydot requires GraphViz using your system's package manager. For example in Ubuntu:

.. code-block:: console

    sudo apt install graphviz

Install from repository
-----------------------

To install directly from the blue-cwl repository:

.. code-block:: console

    pip install git+https://github.com/BlueBrain/blue-cwl.git
    # or with visualization
   pip install git+https://github.com/BlueBrain/blue-cwl.git[viz]
