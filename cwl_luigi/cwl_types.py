"""Types module."""
import enum

from cwl_luigi.exceptions import CWLError


class CWLType(enum.Enum):
    """Common Workflow Language types.

    See https://www.commonwl.org/v1.2/Workflow.html#CWLType
    """

    NULL = enum.auto()
    BOOLEAN = enum.auto()
    INT = enum.auto()
    LONG = enum.auto()
    FLOAT = enum.auto()
    DOUBLE = enum.auto()
    STRING = enum.auto()
    FILE = enum.auto()
    DIRECTORY = enum.auto()
    STDOUT = enum.auto()

    @staticmethod
    def from_string(string_value: str):
        """Convert a string to its respective CWLType."""
        string_to_enum = {
            "null": CWLType.NULL,
            "boolean": CWLType.BOOLEAN,
            "int": CWLType.INT,
            "long": CWLType.LONG,
            "float": CWLType.FLOAT,
            "double": CWLType.DOUBLE,
            "string": CWLType.STRING,
            "File": CWLType.FILE,
            "Directory": CWLType.DIRECTORY,
            "stdout": CWLType.STDOUT,
        }
        if string_value not in string_to_enum:
            raise CWLError(
                f"Unknown type '{string_value}'. Expected on of {list(string_to_enum.keys())}."
            )
        return string_to_enum[string_value]


class CWLWorkflowType(enum.Enum):
    """Workflow node types."""

    INPUT = enum.auto()
    OUTPUT = enum.auto()
    STEP = enum.auto()
