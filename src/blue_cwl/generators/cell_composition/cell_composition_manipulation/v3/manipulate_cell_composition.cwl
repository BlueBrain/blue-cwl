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
    time: '1:00:00'
    nodes: 1
    mem: 0
  remote_config:
    host: bbpv1.epfl.ch

baseCommand: ["blue-cwl", "execute", "cell-composition-manipulation", "manipulate-cell-composition"]

inputs:

  - id: atlas_file
    type: File
    inputBinding:
      prefix: --atlas-file

  - id: recipe_file
    type: File
    inputBinding:
      prefix: --manipulation-file

  - id: region_selection_file
    type: File
    inputBinding:
      prefix: --region-selection-file
  
  - id: densities_file
    type: File
    inputBinding:
      prefix: --cell-composition-volume-file

  - id: materialized_densities_file
    type: File
    inputBinding:
      prefix: --materialized-cell-composition-volume-file

  - id: output_dir
    type: Directory
    inputBinding:
      prefix: --output-dir

outputs:

  - id: cell_composition_volume_file
    type: File
    outputBinding:
      glob: $(inputs.output_dir.path)/cell_composition_volume.json

  - id: cell_composition_summary_file
    type: File
    outputBinding:
      glob: $(inputs.output_dir.path)/cell_composition_summary.json
