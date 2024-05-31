cwlVersion: v1.2
class: CommandLineTool

id: emodel_adapt
label: emodel-adapt

environment:
  env_type: MODULE
  modules:
    - unstable
    - py-emodel-generalisation

executor:
  type: slurm
  slurm_config:
    partition: prod
    account: proj134
    exclusive: true
    time: '11:00:00'
    constraint: nvme
    nodes: 1
    mem: 0
  remote_config:
    host: bbpv1.epfl.ch
  env_vars:
    NEURON_MODULE_OPTIONS: --nogui

baseCommand: ["emodel-generalisation", "-v", "--no-progress", "adapt", "--no-reuse"]

inputs:

  - id: nodes_file
    type: File
    inputBinding:
      prefix: --input-node-path

  - id: out_nodes_file
    type: string
    inputBinding:
      prefix: --output-node-path

  - id: morphologies_dir
    type: Directory
    inputBinding:
      prefix: --morphology-path

  - id: mechanisms_dir
    type: Directory
    inputBinding:
      prefix: --mech-path

  - id: recipe_file
    type: File
    inputBinding:
      prefix: --config-path

  - id: out_hoc_dir
    type: string
    inputBinding:
      prefix: --output-hoc-path

  - id: configs_dir
    type: Directory
    inputBinding:
      prefix: --local-config-path

  - id: work_dir
    type: string
    inputBinding:
      prefix: --local-dir

  - id: parallel_lib
    type: string
    inputBinding:
      prefix: --parallel-lib

outputs:

  - id: nodes_file
    type: File
    outputBinding:
      glob: $(inputs.out_nodes_file)

  - id: hoc_dir
    type: Directory
    outputBinding:
      glob: $(inputs.out_hoc_dir)
