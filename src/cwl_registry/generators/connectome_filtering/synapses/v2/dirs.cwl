
cwlVersion: v1.2
class: CommandLineTool

id: create-directories
label: create-directories

environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj134/scratch/zisis/sub-workflows/venv311

executor:
  type: slurm
  slurm_config:
    partition: prod
    account: proj134
    exclusive: true
    time: '00:05:00'
    ntasks: 1
    mem: 0
  remote_config:
    host: bbpv1.epfl.ch


baseCommand: ["cwl-registry", "execute", "connectome-filtering-synapses", "dirs"]


inputs:

  - id: output_stage_dir
    type: string
    inputBinding:
      prefix: --output-stage-dir

  - id: output_build_dir
    type: string
    inputBinding:
      prefix: --output-build-dir

outputs:

  - id: stage_dir
    type: Directory
    outputBinding:
      glob: $(inputs.output_stage_dir)

  - id: build_dir
    type: Directory
    outputBinding:
      glob: $(inputs.output_build_dir)
