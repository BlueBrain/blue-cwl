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
    time: '16:00:00'
    nodes: 5
    cpus_per_task: 40
    ntasks_per_node: 1
    mem: 0
  remote_config:
    host: bbpv1.epfl.ch

baseCommand:
  - parallel-manipulator
  - -v
  - manipulate-connectome
  - --parallel
  - --keep-parquet
  - --resume

inputs:

  - id: recipe_file
    type: File
    inputBinding:
      position: 1

  - id: output_dir
    type: Directory
    inputBinding:
      prefix: --output-dir

outputs:

  - id: parquet_dir
    type: Directory
    outputBinding:
      glob: $(inputs.output_dir.path)/parquet
