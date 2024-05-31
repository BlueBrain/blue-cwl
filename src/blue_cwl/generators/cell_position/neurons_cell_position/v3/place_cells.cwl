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

baseCommand: ["brainbuilder", "cells", "place"]

inputs:

  - id: composition
    type: File
    inputBinding:
      prefix: --composition

  - id: mtype_taxonomy
    type: File
    inputBinding:
      prefix: --mtype-taxonomy

  - id: atlas_dir
    type: Directory
    inputBinding:
      prefix: --atlas

  - id: atlas_cache
    type: Directory
    inputBinding:
      prefix: --atlas-cache

  - id: region
    type: string
    inputBinding:
      prefix: --region

  - id: soma_placement
    type: string
    inputBinding:
      prefix: --soma-placement
    default: basic

  - id: density_factor
    type: string
    inputBinding:
      prefix: --density-factor
    default: 1.0

  - id: atlas_property
    type: string[]
    inputBinding:
      prefix: --atlas-property

  - id: sort_by
    type: string
    inputBinding:
      prefix: --sort-by
    default: 'region,mtype'

  - id: seed
    type: string
    inputBinding:
      prefix: --seed
    default: 0

  - id: output_nodes_file
    type: string
    inputBinding:
      prefix: --output

  - id: init_cells_file
    type: File
    inputs:
      prefix: --input

outputs:

  - id: nodes_file
    type: File
    outputBinding:
      glob: $(inputs.output_nodes_file)
