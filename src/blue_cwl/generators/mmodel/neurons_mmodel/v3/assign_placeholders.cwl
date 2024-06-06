cwlVersion: v1.2
class: CommandLineTool

id: assign_placeholders
label: assign-placeholders

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

baseCommand: ["blue-cwl", "execute", "mmodel-neurons", "assign-placeholders"]

inputs:

  - id: nodes_file
    type: File
    inputBinding:
      prefix: --nodes-file

  - id: config_file
    type: File
    inputBinding:
      prefix: --config-file

  - id: out_nodes_file
    type: string
    inputBinding:
      prefix: --out-nodes-file

  - id: out_morphologies_dir
    type: string
    inputBinding:
      prefix: --out-morphologies-dir

outputs:

  - id: nodes_file
    type: File
    outputBinding:
      glob: $(inputs.out_nodes_file)
