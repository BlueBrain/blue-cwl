cwlVersion: v1.2
class: CommandLineTool

id: neurons_cell_position_init_cells
label: neurons-cell-position-init-cells

environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj134/scratch/zisis/sub-workflows/venv311

executor:
  type: slurm
  slurm_config:
    partition: prod
    account: proj134
    exclusive: true
    time: '0:05:00'
    nodes: 1
    mem: 0
  remote_config:
    host: bbpv1.epfl.ch

baseCommand: ["blue-cwl", "execute", "neurons-cell-position", "init-cells"]

inputs:

  - id: region
    type: string
    inputBinding:
      prefix: --region

  - id: output_dir
    type: Directory
    inputBinding:
      prefix: --output-dir

outputs:

  - id: nodes_file
    type: File
    outputBinding:
      glob: $(inputs.output_dir.path)/init_nodes.h5
