Concepts
========

The Generator is at the epicenter of the blue-cwl library.

It is a distinct group of variants, i.e. tools or workflows, designed to perform a specific task within a workflow.

.. tip::
    An example of a Generator is "cell placement". A Variant, which is a specific realization of cell placement, would be "neuron cell placement". A version would define a particular iteration of the tool or workflow that implements the neuron cell placement algorithm.


How to read the following sections
----------------------------------

When working with a Generator's Variant, three key aspects need to be considered: the API for interacting with the Variant, defining the Variant using the Common Workflow Language (CWL), and creating CLI wrappers to handle intermediate tasks such as staging and registration.

.. toctree::
    :maxdepth: 2

    variant
    definitions
    wrappers
