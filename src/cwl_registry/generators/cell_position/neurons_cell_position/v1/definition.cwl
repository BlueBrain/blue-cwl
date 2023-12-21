cwlVersion: v1.2
class: CommandLineTool

id: me_type_property
label: Morph-Electric type property generator
stdout: stdout.txt

baseCommand: ['cwl-registry', 'execute', 'neurons-cell-position']


environment:
  env_type: MODULE
  modules:
    - unstable
    - brainbuilder
    - py-cwl-registry
  enable_internet: true

inputs:

    - id: region
      type: string
      inputBinding:
        prefix: --region

    - id: cell_composition
      type: NexusType
      inputBinding:
        prefix: --cell-composition

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
      doc: Circuit bundle with me-types and soma positions.
      outputBinding:
        glob: "partial-circuit.json"
