# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))

from pkg_resources import get_distribution


# -- Project information -----------------------------------------------------

project = "blue-cwl"

# The short X.Y version
version = get_distribution("blue_cwl").version

# The full version, including alpha/beta/rc tags
release = version


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = []

# Add any paths that contain templates here, relative to this directory.
# templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx-bluebrain-theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ['_static']

html_theme_options = {
    "repo_url": "https://github.com/BlueBrain/blue-cwl",
    "repo_name": "BlueBrain/blue-cwl",
    "metadata_distribution": "blue_cwl",
}

html_title = "blue-cwl"

# If true, links to the reST sources are added to the pages.
html_show_sourcelink = False

import os
from pathlib import Path
from blue_cwl.variant import iter_registered_variants
from blue_cwl.core import cwl
from blue_cwl.utils import create_dir

graph_dir = create_dir("./generated")

for variant in iter_registered_variants():
    tool = variant.tool_definition
    name = f"{variant.generator_name}__{variant.variant_name}__{variant.version}.svg"
    if isinstance(tool, cwl.Workflow):
        tool.write_image(filepath=graph_dir / name)
