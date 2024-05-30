cwlVersion: v1.2
class: CommandLineTool

id: emodel_currents
label: emodel-currents

environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj134/scratch/zisis/sub-workflows/venv311

executor:
  type: slurm
  slurm_config:
    partition: prod
    account: proj134
    exclusive: true
    time: '12:00:00'
    constraint: nvme
    nodes: 1
    mem: 0
  remote_config:
    host: bbpv1.epfl.ch
  env_vars:
    NEURON_MODULE_OPTIONS: --nogui

baseCommand: ["emodel-generalisation", "-v", "--no-progress", "compute_currents"]

inputs:

  - id: nodes_file
    type: File
    inputBinding:
      prefix: --input-path

  - id: out_nodes_file
    type: string
    inputBinding:
      prefix: --output-path

  - id: morphologies_dir
    type: Directory
    inputBinding:
      prefix: --morphology-path

  - id: mechanisms_dir
    type: Directory
    inputBinding:
      prefix: --mech-path

  - id: hoc_dir
    type: Directory
    inputBinding:
      prefix: --hoc-path

  - id: parallel_lib
    type: string
    inputBinding:
      prefix: --parallel-lib

outputs:

  - id: nodes_file
    type: File
    outputBinding:
      glob: $(inputs.out_nodes_file)
