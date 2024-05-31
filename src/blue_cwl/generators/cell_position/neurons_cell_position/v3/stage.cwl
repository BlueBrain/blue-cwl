cwlVersion: v1.2
class: CommandLineTool

id: stage_nexus_resources
label: stage-nexus-resources

environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj134/scratch/zisis/sub-workflows/venv311

executor:
  type: slurm
  slurm_config:
    partition: prod
    account: proj134
    exclusive: true
    time: '00:10:00'
    ntasks: 1
    mem: 0
  remote_config:
    host: bbpv1.epfl.ch

baseCommand: ["blue-cwl", "execute", "neurons-cell-position", "stage"]

inputs:

  - id: region_id
    type: NexusType
    inputBinding:
      prefix: --region-id

  - id: configuration_id
    type: NexusType
    inputBinding:
      prefix: --configuration-id

  - id: cell_composition_id
    type: NexusType
    inputBinding:
      prefix: --cell-composition-id

  - id: stage_dir
    type: Directory
    inputBinding:
      prefix: --stage-dir

outputs:

  - id: region_file
    type: File
    outputBinding:
      glob: $(inputs.stage_dir.path)/region.txt

  - id: atlas_file
    type: File
    outputBinding:
      glob: $(inputs.stage_dir.path)/atlas.json

  - id: densities_file
    type: File
    outputBinding:
      glob: $(inputs.stage_dir.path)/densities.parquet
