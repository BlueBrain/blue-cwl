cwlVersion: v1.2
class: CommandLineTool

id: cell_composition_manipulation_register
label: cell-composition-manipulation-register

environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj134/scratch/zisis/sub-workflows/venv311

executor:
  type: slurm
  slurm_config:
    partition: prod
    account: proj134
    exclusive: true
    time: '1:00:00'
    nodes: 1
    mem: 0
  remote_config:
    host: bbpv1.epfl.ch

baseCommand: ["blue-cwl", "execute", "cell-composition-manipulation", "register"]

inputs:

  - id: base_cell_composition_id
    type: NexusType
    inputBinding:
      prefix: --base-cell-composition-id

  - id: cell_composition_volume_file
    type: File
    inputBinding:
      prefix: --cell-composition-volume-file

  - id: cell_composition_summary_file
    type: File
    inputBinding:
      prefix: --cell-composition-summary-file
    
  - id: output_dir
    type: Directory
    inputBinding:
      prefix: --output-dir

outputs:

  - id: cell_composition
    type: NexusType
    outputBinding:
      glob: $(inputs.output_dir.path)/resource.json
