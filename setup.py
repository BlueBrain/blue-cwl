#!/usr/bin/env python
import importlib.util
from pathlib import Path

from setuptools import setup, find_packages

spec = importlib.util.spec_from_file_location(
    "blue_cwl.version",
    "src/blue_cwl/version.py",
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
VERSION = module.__version__


setup(
    name="blue-cwl",
    author="bbp-ou-nse",
    author_email="bbp-ou-nse@groupes.epfl.ch",
    version=VERSION,
    description="Common Workflow Language tool definitions",
    long_description=Path("README.rst").read_text(encoding="utf-8"),
    long_description_content_type="text/x-rst",
    url="https://bbpteam.epfl.ch/documentation/projects/blue-cwl",
    project_urls={
        "Tracker": "https://bbpteam.epfl.ch/project/issues/projects/NSETM/issues",
        "Source": "git@bbpgitlab.epfl.ch:nse/blue-cwl.git",
    },
    license="BBP-internal-confidential",
    entry_points={"console_scripts": ["blue-cwl=blue_cwl.cli:main"]},
    install_requires=[
        "click>=8.0",
        "libsonata",
        "numpy",
        "pandas",
        "voxcell",
        "joblib",
        "entity_management>=1.2.45",  # latest emodel definitions
        "blue-cwl>=1.0.0",
        "pyarrow>=3.0.0",
        "fz_td_recipe>=0.2.0",  # support for json recipe
        "pydantic",
        "morph-tool",
        "jsonschema",
        "luigi"
    ],
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    python_requires=">=3.10",
    extras_require={
        "docs": ["sphinx", "sphinx-bluebrain-theme"],
    },
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Education",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
    ],
)
