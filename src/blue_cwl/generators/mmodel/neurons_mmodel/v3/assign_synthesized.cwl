cwlVersion: v1.2
class: CommandLineTool

id: assign_synthesized
label: assign-synthesized

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

baseCommand: 
  - region-grower
  - synthesize-morphologies
  - --out-morph-ext h5
  - --out-morph-ext asc
  - --max-files-per-dir 10000
  - --max-drop-ration 0.5
  - --rotation-jitter-std 10
  - --seed 0
  - --hide-progress-bar
  - --overwrite

inputs:

  - id: nodes_file
    type: File
    inputBinding:
      prefix: --input-cells

  - id: out_nodes_file
    type: string
    inputBinding:
      prefix: --out-cells

  - id: out_morphologies_dir
    type: string
    inputBinding:
      prefix: --out-morph-dir

  - id: atlas_dir
    type: Directory
    inputBinding:
      prefix: --atlas

  - id: region_file
    type: File
    inputBinding:
      prefix: --region-structure
    
  - id: parameters_file
    type: File
    inputBinding:
      prefix: --tmd-parameters

  - id: distributions_file
    type: File
    inputBinding:
      prefix: --tmd-distributions

outputs:

  - id: nodes_file
    type: File
    outputBinding:
      glob: $(inputs.out_nodes_file)
