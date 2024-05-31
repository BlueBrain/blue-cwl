# SPDX-License-Identifier: Apache-2.0

"""Exceptions module."""


class CWLError(Exception):
    """Generic cwl building error."""


class ReferenceResolutionError(CWLError):
    """Parameter reference resolution error."""


class CWLValidationError(CWLError):
    """CWL Validation error."""


class InputConcretizationError(CWLError):
    """Input concretization error."""
