cwlVersion: v1.2
class: CommandLineTool

id: create-directories
label: create-directories

environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj134/scratch/zisis/sub-workflows/venv311

executor:
  type: slurm
  slurm_config:
    partition: prod
    account: proj134
    exclusive: true
    time: '00:10:00'
    ntasks: 1
    mem: 0
  remote_config:
    host: bbpv1.epfl.ch

baseCommand: ["blue-cwl", "execute", "mmodel-neurons", "setup"]

inputs:

  - id: output_dir
    type: Directory
    inputBinding:
      prefix: --output-dir

outputs:

  - id: stage_dir
    type: Directory
    outputBinding:
      glob: $(inputs.output_dir.path)/stage

  - id: build_dir
    type: Directory
    outputBinding:
      glob: $(inputs.output_dir.path)/build

  - id: transform_dir
    type: Directory
    outputBinding:
      glob: $(inputs.output_dir.path)/transform

  - id: morphologies_dir
    type: Directory
    outputBinding:
      glob: $(inputs.output_dir.path)/build/morphologies
