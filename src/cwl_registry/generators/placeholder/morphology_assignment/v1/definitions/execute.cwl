cwlVersion: v1.2
class: CommandLineTool

id: placeholder_morphology_assignment
label: Morphology Assignment
stdout: stdout.txt

baseCommand: ['cwl-registry', 'execute', 'placeholder-morphology-assignment']


environment:
  env_type: MODULE
  modules:
    - unstable
    - py-cwl-registry
  enable_internet: true


inputs:

    - id: region
      type: string
      inputBinding:
        prefix: --region

    - id: partial_circuit
      type: NexusType
      inputBinding:
        prefix: --partial-circuit

    - id: mtype_morphologies
      type: NexusType
      inputBinding:
        prefix: --mtype-morphologies

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
      doc: Circuit bundle with me-types and morphologies.
      outputBinding:
        glob: "circuit_morphologies_bundle.json"