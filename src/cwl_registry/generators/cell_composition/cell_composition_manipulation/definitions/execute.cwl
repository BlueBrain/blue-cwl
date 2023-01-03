cwlVersion: v1.2
class: CommandLineTool

id: me_type_property
label: Morph-Electric type property generator
stdout: stdout.txt

baseCommand: ['cwl-registry', 'execute', 'cell-composition-manipulation']


environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj134/workflows/environments/venv-densities


inputs:

    - id: nexus_base
      type: string
      inputBinding:
        prefix: --nexus-base

    - id: nexus_org
      type: string
      inputBinding:
        prefix: --nexus-org

    - id: nexus_project
      type: string
      inputBinding:
        prefix: --nexus-project

    - id: nexus_token
      type: string
      inputBinding:
        prefix: --nexus-token

    - id: base_composition_summary
      type: NexusType
      inputBinding:
        prefix: --base-composition-summary

    - id: base_density_distribution
      type: NexusType
      inputBinding:
        prefix: --base-density-distribution

    - id: atlas_release
      type: NexusType
      inputBinding:
        prefix: --atlas-release

    - id: recipe
      type: NexusType
      inputBinding:
        prefix: --recipe

    - id: task_digest
      type: string
      inputBinding:
        prefix: --task-digest

    - id: output_dir
      type: Directory
      inputBinding:
        prefix: --output-dir

outputs:

    - id: cell_composition_manipulation__density_distribution
      type: NexusType
