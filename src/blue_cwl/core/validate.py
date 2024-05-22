# SPDX-License-Identifier: Apache-2.0

"""Validation utils."""

import json
from importlib import resources
from typing import Any

import jsonschema
import yaml

from blue_cwl.core.exceptions import CWLError, CWLValidationError


def validate_schema(data: dict[str, Any], schema_name: str) -> None:  # pragma: no cover
    """Validata data against the schema with 'schema_name'."""
    schema = _read_schema(schema_name)

    cls = jsonschema.validators.validator_for(schema)
    cls.check_schema(schema)
    validator = cls(schema)
    errors = validator.iter_errors(data)

    messages: list[str] = []
    for error in errors:
        if error.context:
            messages.extend(map(_format_error, error.context))
        else:
            messages.append(_format_error(error))

    if messages:
        raise CWLError("\n".join(messages) + f"\ndata:\n{_format_data(data)}")


def _format_data(data: dict) -> str:
    return json.dumps(data, indent=2)


def _read_schema(schema_name: str) -> dict[str, Any]:
    """Load a schema and return the result as a dictionary."""
    resource = resources.files("blue_cwl") / "core" / "schemas" / schema_name
    content = resource.read_text()
    return yaml.safe_load(content)


def _format_error(error) -> str:
    paths = " -> ".join(map(str, error.absolute_path))
    return f"[{paths}]: {error.message}"


def validate_workflow(workflow) -> None:
    """Validate workflow template."""
    input_names = workflow.inputs.keys()

    workflow_steps = {s.id: s for s in workflow.iter_steps()}

    def _validate_step_inputs(step):
        """Check that each step has valid input references to upstream outputs."""
        errs = []
        for step_input in step.inputs.values():
            step_source = step_input.source

            if step_source is None:
                continue

            for source_name, source_output in step_input.split_source_output():
                if source_name is None:
                    if source_output not in input_names:
                        errs.append(
                            {
                                "type": "OutputNotInWorkflowInputs",
                                "output": source_output,
                                "workflow_inputs": sorted(input_names),
                            }
                        )

                elif source_name not in workflow_steps:
                    errs.append(
                        {
                            "type": "InvalidInputSource",
                            "source": source_name,
                            "output": source_output,
                            "workflow_sources": sorted(workflow_steps),
                        }
                    )
                else:
                    upstream_outputs = workflow_steps[source_name].outputs

                    if source_output not in upstream_outputs:
                        errs.append(
                            {
                                "type": "InvalidInputSourceOutput",
                                "source": source_name,
                                "output": source_output,
                                "source_outputs": sorted(upstream_outputs),
                            }
                        )

        return errs

    errors = {}

    for step in workflow_steps.values():
        step_errors = _validate_step_inputs(step)

        if step_errors:
            errors[step.id] = step_errors

    if errors:
        raise CWLValidationError(
            json.dumps(
                errors,
                indent=2,
            )
        )
