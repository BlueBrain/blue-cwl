cwlVersion: v1.2
class: CommandLineTool

id: transform
label: transform

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

baseCommand: ["blue-cwl", "execute", "connectome-generation-placeholder", "transform"]

inputs:

  - id: atlas_file
    type: File
    inputBinding:
      prefix: --atlas-file

  - id: macro_config_file
    type: File
    inputBinding:
      prefix: --macro-config-file

  - id: micro_config_file
    type: File
    inputBinding:
      prefix: --micro-config-file

  - id: circuit_file
    type: File
    inputBinding:
      prefix: --circuit-config-file
    
  - id: transform_dir
    type: Directory
    inputBinding:
      prefix: --transform-dir

outputs:

  - id: recipe_file
    type: File
    outputBinding:
      glob: $(inputs.transform_dir.path)/recipe.json
