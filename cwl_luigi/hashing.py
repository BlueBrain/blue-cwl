"""Task and Targe hashing utils."""
from hashlib import sha256


def get_task_hexdigest(task) -> str:
    """Create a salted id/version for this task without lineage.

    Taken from https://github.com/gorlins/salted/blob/master/salted_demo.py

    Returns: A unique, deterministic hexdigest for this target.
    """
    msg = ",".join(
        [
            # Basic capture of input type
            task.__class__.__name__,
        ]
        + [
            # Depending on strictness - skipping params is acceptable if
            # output already is partitioned by their params; including every
            # param may make hash *too* sensitive
            f"{param_name}={repr(task.param_kwargs[param_name])}"
            for param_name, param in sorted(task.get_params())
            if param.significant
        ]
    )
    return sha256(msg.encode()).hexdigest()


def get_target_hexdigest(task_hexdigest: str, target_name: str) -> str:
    """Combine tasks's hash with the target name's one to create a unique target hash."""
    h = sha256()
    h.update(task_hexdigest.encode())
    h.update(target_name.encode())

    return h.hexdigest()
