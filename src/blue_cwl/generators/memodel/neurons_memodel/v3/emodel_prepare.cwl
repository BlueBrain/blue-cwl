cwlVersion: v1.2
class: CommandLineTool

id: emodel_prepare
label: emodel-prepare

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
  env_vars:
    NEURON_MODULE_OPTIONS: --nogui

baseCommand: ["emodel-generalisation", "-v", "prepare"]

inputs:

  - id: config_file
    type: File
    inputBinding:
      prefix: --config-path

  - id: work_dir
    type: string
    inputBinding:
      prefix: --local-config-path

  - id: out_mechanisms_dir
    type: string
    inputBinding:
      prefix: --mechanisms-path

outputs:

  - id: work_dir
    type: Directory
    outputBinding:
      glob: $(inputs.work_dir)

  - id: mechanisms_dir
    type: Directory
    outputBinding:
      glob: $(inputs.out_mechanisms_dir)
