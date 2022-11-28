"""CWL Registry Exceptions."""


class CWLRegistryError(Exception):
    """CWL registry exception class."""


class CWLWorkflowError(CWLRegistryError):
    """CWL Workflow exception class."""
