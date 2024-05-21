# SPDX-License-Identifier: Apache-2.0

"""CWL Registry Exceptions."""


class CWLRegistryError(Exception):
    """CWL registry exception class."""


class CWLWorkflowError(CWLRegistryError):
    """CWL Workflow exception class."""


class SchemaValidationError(Exception):
    """Schemal validation exception class."""
