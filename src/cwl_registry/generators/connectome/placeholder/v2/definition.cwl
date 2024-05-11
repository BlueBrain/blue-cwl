cwlVersion: v1.2
class: CommandLineTool

id: connectome_generation_placeholder
label: Placeholder connectome manipulation with parallel execution
stdout: stdout.txt

baseCommand: ['cwl-registry', 'execute', 'connectome-generation-placeholder', 'mono-execution']

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
    OMP_NUM_THREADS: 40
    MPI_OPENMP_INTEROP: 1

resources:

  default:

    partition: prod
    nodes: 5
    ntasks_per_node: 1
    cpus_per_task: 40
    exclusive: true
    time: '1-00:00:00'
    mem: 0
    account: proj134

  sub-tasks:

    # connectome-manipulator
    - partition: prod
      nodes: 5
      ntasks_per_node: 1
      cpus_per_task: 40
      exclusive: true
      time: '16:00:00'
      mem: 0
      account: proj134

    # parquet to sonata conversion
    - partition: prod
      nodes: 2
      ntasks_per_node: 10
      cpus_per_task: 4
      exclusive: true
      time: '8:00:00'
      mem: 0
      account: proj134


inputs:

    - id: configuration
      type: NexusType
      inputBinding:
        prefix: --configuration-id

    - id: circuit
      type: NexusType
      inputBinding:
        prefix: --circuit-id

    - id: macro_connectome_config
      type: NexusType
      inputBinding:
        prefix: --macro-connectome-config-id

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
      doc: Circuit bundle with connectivity.
      outputBinding:
        glob: "$(inputs.output_dir.path)/resource.json"
