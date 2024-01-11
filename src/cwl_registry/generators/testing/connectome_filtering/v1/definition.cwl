cwlVersion: v1.2
class: CommandLineTool

id: connectome_distance_dependent
label: Distance dependent connectome manipulation
stdout: stdout.txt

baseCommand: ['cwl-registry', 'execute', 'connectome-filtering-synapses']


environment:
  env_type: MODULE
  modules:
    - unstable
    - spykfunc
    - parquet-converters
    - py-cwl-registry
  enable_internet: true


resources:
    default:
        partition: prod
        account: proj134
        nodes: 20
        time: '12:00:00'
        ntasks_per_node: 1
        mem: 0
        exclusive: true
        constraint: nvme

    sub-tasks:

      # functionalizer 
      - partition: prod
        account: proj134
        nodes: 20
        time: '16:00:00'
        ntasks_per_node: 1
        mem: 0
        exclusive: true
        constraint: nvme

      # parquet to sonata conversion
      - partition: prod
        nodes: 100
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
        prefix: --configuration

    - id: partial_circuit
      type: NexusType
      inputBinding:
        prefix: --partial-circuit

    - id: variant_config
      type: NexusType
      inputBinding:
        prefix: --variant-config

    - id: output_dir
      type: Directory
      inputBinding:
        prefix: --output-dir

outputs:

    - id: partial_circuit
      type: NexusType
      doc: Circuit bundle with connectivity.
      outputBinding:
        glob: "output_circuit.json"
