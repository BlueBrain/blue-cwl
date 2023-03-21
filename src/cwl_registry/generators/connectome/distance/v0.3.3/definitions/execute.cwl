cwlVersion: v1.2
class: CommandLineTool

id: connectome_distance_dependent
label: Distance dependent connectome manipulation
stdout: stdout.txt

baseCommand: ['cwl-registry', 'execute', 'connectome-distance-dependent']


environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj134/workflows/environments/venv-v0.3.3
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

    - id: "circuit_connectome_bundle"
      type: NexusType
      doc: Circuit bundle with connectivity.
      outputBinding:
        glob: "circuit_connectome_bundle.json"
