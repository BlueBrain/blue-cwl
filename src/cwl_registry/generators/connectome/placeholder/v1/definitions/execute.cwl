cwlVersion: v1.2
class: CommandLineTool

id: connectome_generation_placeholder
label: Placeholder connectome manipulation with parallel execution
stdout: stdout.txt

baseCommand: ['cwl-registry', 'execute', 'connectome-generation-placeholder']


environment:
  env_type: MODULE
  modules:
    - unstable
    - py-cwl-registry
    - py-connectome-manipulator/0.0.9
  env_vars:
    OMP_NUM_THREADS: 40
    MPI_OPENMP_INTEROP: 1
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

    - id: macro_connectome_config
      type: NexusType
      inputBinding:
        prefix: --macro-connectome-config

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
