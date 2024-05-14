cwlVersion: v1.2
class: CommandLineTool

id: cell_composition_manipulation
label: Cell Composition Manipulation

baseCommand: ['blue-cwl', 'execute', 'cell-composition-manipulation', 'mono-execution']


environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj134/scratch/zisis/sub-workflows/venv311
  enable_internet: true

executor:
  type: slurm
  slurm_config:
    partition: prod
    nodes: 1
    exclusive: true
    time: '2:00:00'
    account: proj134
  remote_config:
    host: bbpv1.epfl.ch

inputs:

    - id: region
      type: NexusType
      inputBinding:
        prefix: --region

    - id: base_cell_composition
      type: NexusType
      inputBinding:
        prefix: --base-cell-composition-id

    - id: configuration
      type: NexusType
      inputBinding:
        prefix: --configuration-id

    - id: variant
      type: NexusType
      inputBinding:
        prefix: --variant-id

    - id: output_dir
      type: Directory
      inputBinding:
        prefix: --output-dir

outputs:

    - id: cell_composition
      type: NexusType
      outputBinding:
        glob: "$(inputs.output_dir.path)/resource.json"
