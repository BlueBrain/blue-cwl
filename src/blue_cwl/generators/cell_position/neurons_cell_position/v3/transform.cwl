cwlVersion: v1.2
class: CommandLineTool

id: neurons_cell_position_transform
label: neurons-cell-position-transform

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

baseCommand: ["blue-cwl", "execute", "neurons-cell-position", "transform"]

inputs:

  - id: region_file
    type: File
    inputBinding:
      prefix: --region-file

  - id: densities_file
    type: File
    inputBinding:
      prefix: --densities-file

  - id: transform_dir
    type: Directory
    inputBinding:
      prefix: --transform-dir

outputs:

  - id: mtype_taxonomy_file
    type: File
    outputBinding:
      glob: $(inputs.transform_dir.path)/mtype_taxonomy.tsv

  - id: mtype_composition_file
    type: File
    outputBinding:
      glob: $(inputs.transform_dir.path)/mtype_composition.yml
