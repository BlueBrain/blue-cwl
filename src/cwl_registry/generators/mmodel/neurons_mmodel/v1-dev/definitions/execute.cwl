cwlVersion: v1.2
class: CommandLineTool

id: placeholder_morphology_assignment
label: Morphology Assignment
stdout: stdout.txt

baseCommand: ['cwl-registry', 'execute', 'mmodel-neurons']


environment:
  env_type: MODULE
  modulepath: /gpfs/bbp.cscs.ch/ssd/apps/bsd/pulls/2135/config/modules/_meta
  modules:
    - unstable
    - py-cwl-registry
    - py-region-grower
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
      doc: Circuit bundle with me-types and morphologies.
      outputBinding:
        glob: "circuit_morphologies_bundle.json"
