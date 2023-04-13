cwlVersion: v1.2
class: CommandLineTool

id: placeholder_emodel_assignment
label: EModel assignment
stdout: stdout.txt

baseCommand: ['cwl-registry', 'execute', 'placeholder-emodel-assignment']


environment:
  env_type: MODULE
  modules:
    - unstable
    - py-cwl-registry/0.3.6
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

    - id: etype_emodels
      type: NexusType
      inputBinding:
        prefix: --etype-emodels

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
      doc: Circuit bundle with emodels.
      outputBinding:
        glob: "circuit_emodels_bundle.json"
