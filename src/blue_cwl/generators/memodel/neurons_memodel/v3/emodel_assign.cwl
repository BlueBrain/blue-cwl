cwlVersion: v1.2
class: CommandLineTool

id: emodel_assign
label: emodel-assign

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
    time: '01:00:00'
    nodes: 1
    mem: 0
  remote_config:
    host: bbpv1.epfl.ch
  env_vars:
    NEURON_MODULE_OPTIONS: --nogui

baseCommand: ["emodel-generalisation", "-v", "--no-progress", "assign"]

inputs:

  - id: nodes_file
    type: File
    inputBinding:
      prefix: --input-node-path

  - id: recipe_file
    type: File
    inputBinding:
      prefix: --config-path

  - id: out_nodes_file
    type: string
    inputBinding:
      prefix: --output-node-path

  - id: out_configs_dir
    type: string
    inputBinding:
      prefix: --local-config-path

outputs:

  - id: nodes_file
    type: File
    outputBinding:
      glob: $(inputs.out_nodes_file)

  - id: configs_dir
    type: Directory
    outputBinding:
      glob: $(inputs.out_configs_dir)
