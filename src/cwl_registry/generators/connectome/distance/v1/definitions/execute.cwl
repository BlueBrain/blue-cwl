cwlVersion: v1.2
class: CommandLineTool

id: connectome_distance_dependent
label: Distance dependent connectome manipulation with parallel execution
stdout: stdout.txt

baseCommand: ['cwl-registry', 'execute', 'connectome-distance-dependent']


environment:
  env_type: MODULE
  modules:
    - archive/2023-05
    - py-connectome-manipulator/0.0.4
    - py-cwl-registry/0.3.7
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
        glob: "circuit_connectome_bundle.json"
