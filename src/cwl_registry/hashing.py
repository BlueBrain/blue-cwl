"""Hashing."""

from hashlib import sha256


def get_target_hexdigest(task_hexdigest: str, target_name: str) -> str:
    """Combine tasks's hash with the target name's one to create a unique target hash."""
    h = sha256()

    h.update(task_hexdigest.encode())

    h.update(target_name.encode())

    return h.hexdigest()
