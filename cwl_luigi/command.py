"""Shell command building for luigi workflow."""
import copy
from typing import Dict, List, Optional, Tuple

from cwl_luigi.environment import Environment


def build_command(
    base_command: List[str],
    named_arguments: Dict[str, str],
    positional_arguments: List[Tuple[int, str]],
    environment: Optional[Environment] = None,
) -> str:
    """Build command."""
    command = copy.deepcopy(base_command)
    assert isinstance(command, list)

    for prefix, placeholder in named_arguments.items():
        command.extend((prefix, f"{{{placeholder}}}"))

    for _, placeholder in sorted(positional_arguments):
        command.append(f"{{{placeholder}}}")

    str_command = " ".join(map(str, command))

    if environment:
        return environment.shell_command(cmd=str_command)

    return str_command
