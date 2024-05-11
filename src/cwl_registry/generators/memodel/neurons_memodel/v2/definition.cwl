cwlVersion: v1.2
class: CommandLineTool

id: placeholder_morphology_assignment
label: Morphology Assignment
stdout: stdout.txt

baseCommand: ['cwl-registry', 'execute', 'me-model', 'mono-execution']


environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj134/scratch/zisis/sub-workflows/venv311
  enable_internet: true

executor:
  type: slurm
  slurm_config:
    partition: prod
    account: proj134
    exclusive: true
    time: '24:00:00'
    nodes: 1
    mem: 0
  remote_config:
    host: bbpv1.epfl.ch
  env_vars:
    ME_MODEL_MULTIPROCESSING_BACKEND: multiprocessing

# TODO: Convert to workflow with per-task allocations
resources:
  default:
    partition: prod
    account: proj134
    exclusive: true
    time: '12:00:00'
    nodes: 1
    mem: 0
    constraint: nvme
  sub-tasks:
  - partition: prod
    account: proj134
    exclusive: true
    time: '1:00:00'
    nodes: 1
    mem: 0
  - partition: prod
    account: proj134
    exclusive: true
    time: '11:00:00'
    constraint: nvme
    nodes: 1 
    mem: 0
  - partition: prod
    account: proj134
    exclusive: true
    time: '12:00:00'
    constraint: nvme
    nodes: 1
    mem: 0

inputs:

    - id: configuration
      type: NexusType
      inputBinding:
        prefix: --configuration-id

    - id: circuit
      type: NexusType
      inputBinding:
        prefix: --circuit-id

    - id: variant
      type: NexusType
      inputBinding:
        prefix: --variant-id

    - id: output_dir
      type: Directory
      inputBinding:
        prefix: --output-dir

outputs:

    - id: partial_circuit
      type: NexusType
      doc: Circuit bundle with me-types and morphologies.
      outputBinding:
        glob: "$(inputs.output_dir.path)/circuit_me_model_bundle.json"
