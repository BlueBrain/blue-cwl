cwlVersion: v1.2
class: CommandLineTool

id: split-nodes
label: split-nodes

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

baseCommand: ["blue-cwl", "execute", "mmodel-neurons", "split"]

inputs:

  - id: nodes_file
    type: File
    inputBinding:
      prefix: --nodes-file

  - id: canonical_config_file
    type: File
    inputBinding:
      prefix: --canonical-config-file

  - id: output_dir
    type: Directory
    inputBinding:
      prefix: --output-dir

outputs:

  - id: synthesized_nodes_file
    type: File
    outputBinding:
      glob: $(inputs.output_dir.path)/canonicals.h5

  - id: placeholder_nodes_file
    type: File
    outputBinding:
      glob: $(inputs.output_dir.path)/placeholders.h5
