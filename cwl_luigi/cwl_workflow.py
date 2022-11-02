"""Common Workflow Language execution tasks."""
import json
from pathlib import Path

import luigi

from cwl_luigi import cwl
from cwl_luigi.cwl_builder import build_workflow_cwl_datasets
from cwl_luigi.cwl_luigi import build_workflow
from cwl_luigi.utils import load_yaml, write_yaml


class CWLWorkflow(luigi.Task):
    """Common Workflow Language task.

    Args:
        recipe_file (luigi.Parameter):
            Path to the UI recipe.
        nexus_token (luigi.Parameter):
            Authentication token
        nexus_base (luigi.Parameter):
            Nexus endpoint. Example: https://bbp.epfl.ch/nexus/v1
        nexus_org (luigi.Parameter):
            Nexus organization. Example: bbp
        nexus_project (luigi.Parameter):
            Nexus project. Example: mmb-point-neuron-framework-model
        output_dir (luigi.Parameter): Path to the output directory.
    """

    recipe_file = luigi.Parameter()
    nexus_token = luigi.Parameter()
    nexus_base = luigi.Parameter()
    nexus_org = luigi.Parameter()
    nexus_project = luigi.Parameter()
    output_dir = luigi.Parameter()

    def output(self):
        """Placeholder."""
        return luigi.LocalTarget(path=str(Path(self.output_dir, "artifacts.json")))

    def run(self):
        """Main execution method."""
        output_dir = Path(self.output_dir).resolve()
        output_dir.mkdir(exist_ok=True)

        cwl_dir = output_dir / "cwl"
        cwl_dir.mkdir(exist_ok=True)

        workflow_dict, config_dict = build_workflow_cwl_datasets(
            recipe_dict=load_yaml(self.recipe_file),
            nexus_token=self.nexus_token,
            nexus_org=self.nexus_org,
            nexus_project=self.nexus_project,
            nexus_base=self.nexus_base,
        )

        # writing the cwl datasets to files is not necessary, but it is useful for debugging
        workflow_file = cwl_dir / "workflow.yml"
        write_yaml(filepath=workflow_file, data=workflow_dict)

        config_file = cwl_dir / "config.yml"
        write_yaml(filepath=config_file, data=config_dict)

        # build cwl graph from workflow template file
        workflow = cwl.Workflow.from_cwl(Path(workflow_file))

        # workflow parameter values for template inputs
        config = cwl.Config.from_cwl(config_file)

        build_dir = output_dir / "build"
        build_dir.mkdir(parents=True, exist_ok=True)

        tasks = build_workflow(workflow, base_dir=build_dir, config=config)

        yield tasks

        with self.output().open("w") as fd:
            json.dump({"placeholder": "asdf"}, fd)
