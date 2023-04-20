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
    - cwl-registry
  enable_internet: true


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
