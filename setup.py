#!/usr/bin/env python

import importlib.util

from setuptools import setup, find_packages

# read the contents of the README file
with open("README.rst", encoding="utf-8") as f:
    README = f.read()

spec = importlib.util.spec_from_file_location(
    "cwl_luigi.version",
    "cwl_luigi/version.py",
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
VERSION = module.__version__

setup(
    name="cwl-luigi",
    author="bbp-ou-nse",
    author_email="bbp-ou-nse@groupes.epfl.ch",
    version=VERSION,
    description="Make CWL workflows executable by Luigi",
    long_description=README,
    long_description_content_type="text/x-rst",
    url="https://bbpteam.epfl.ch/documentation/projects/cwl-luigi",
    project_urls={
        "Tracker": "https://bbpteam.epfl.ch/project/issues/projects/NSETM/issues",
        "Source": "git@bbpgitlab.epfl.ch:nse/cwl-luigi.git",
    },
    license="BBP-internal-confidential",
    install_requires=[
        "pyyaml>=6.0",
        "luigi>=3.0",
        "click>=8.0",
        "jsonschema>=3.0",
        "cwl-registry>=0.1.1",
    ],
    python_requires=">=3.9",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "cwl_luigi": ["cwl_luigi/schemas/*"],
    },
    extras_require={"docs": ["sphinx", "sphinx-bluebrain-theme"]},
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Education",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
    ],
)
