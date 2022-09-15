"""Shell command building for luigi workflow."""
from typing import Optional

from cwl_luigi.environment import Environment


def build_command(
    base_command: str,
    environment: Optional[Environment] = None,
) -> str:
    """Build command."""
    if environment:
        return environment.shell_command(cmd=base_command)

    return base_command
