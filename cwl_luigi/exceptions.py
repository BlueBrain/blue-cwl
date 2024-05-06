"""Exceptions module."""


class CWLError(Exception):
    """Generic cwl building error."""


class ReferenceResolutionError(CWLError):
    """Parameter reference resolution error."""
