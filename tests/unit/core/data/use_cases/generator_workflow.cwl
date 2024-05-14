cwlVersion: v1.2
class: Workflow

id: workflow_me_type_property
label: mock-generator-workflow

inputs:

    - id: region
      type: string

    - id: cell_composition
      type: NexusType

    - id: variant_config
      type: NexusType

    - id: output_dir
      type: Directory

outputs:

    partial_circuit: 
        type: NexusType
        outputSource: register/partial_circuit

steps:
    - id: stage_cell_composition
      in:
        cell_composition: cell_composition
        output_dir: output_dir
      out:
        - staged_resource
      run:
          cwlVersion: v1.2
          class: CommandLineTool
          id: stage_cell_composition
          environment:
            env_type: MODULE
            modules:
              - unstable
              - py-blue-cwl
          executor:
            type: slurm
            slurm_config:
              partition: prod
              nodes: 1
              exclusive: true
              time: '8:00:00'
              account: proj134
            remote_config:
              host: bbpv1.epfl.ch
            env_vars:
              FOO: foo
          baseCommand: ["blue-cwl", "stage", "cell-composition"]
          inputs:
            - id: cell_composition
              type: NexusType
              inputBinding:
                position: 1
            - id: output_dir
              type: Directory
              inputBinding:
                position: 2
          outputs:
            - id: staged_resource
              type: File
              outputBinding:
                glob: $(inputs.output_dir.path)/staged_resource.json

    - id: stage_variant_config
      in:
        variant_config: variant_config
        output_dir: output_dir
      out:
        - staged_resource
      run:
          cwlVersion: v1.2
          class: CommandLineTool
          id: stage_variant_config
          environment:
            env_type: MODULE
            modules:
              - unstable
              - py-blue-cwl
          executor:
            type: slurm
            slurm_config:
              partition: prod
              ntasks: 1
              exclusive: true
              time: '8:00:00'
              account: proj134
            remote_config:
              host: bbpv1.epfl.ch
          baseCommand: ["blue-cwl", "stage", "variant-config"]
          inputs:
            - id: variant_config
              type: NexusType
              inputBinding:
                position: 1
            - id: output_dir
              type: Directory
              inputBinding:
                position: 2
          outputs:
            - id: staged_resource
              type: File
              outputBinding:
                glob: $(inputs.output_dir.path)/staged_resource.json

    - id: neurons_cell_position
      in:
         cell_composition: stage_cell_composition/staged_resource
         variant_config: stage_variant_config/staged_resource
         output_dir: output_dir
      out:
         - partial_circuit
      run:
          cwlVersion: v1.2
          class: CommandLineTool
          id: stage_variant_config
          environment:
            env_type: MODULE
            modules:
              - unstable
              - py-blue-cwl
          executor:
            type: slurm
            slurm_config:
              partition: prod
              ntasks: 1
              exclusive: true
              time: '8:00:00'
              account: proj134
            remote_config:
              host: bbpv1.epfl.ch
          baseCommand: ["blue-cwl", "execute", "placement"]
          inputs:
            - id: cell_composition
              type: File
              inputBinding:
                position: 1
            - id: variant_config
              type: File
              inputBinding:
                position: 2
            - id: output_dir
              type: Directory
              inputBinding:
                position: 3
          outputs:
            - id: partial_circuit
              type: File
              outputBinding:
                glob: $(inputs.output_dir.path)/circuit.json

    - id: register
      in:
        cell_composition: cell_composition
        variant_config: variant_config
        circuit_config: neurons_cell_position/partial_circuit
      out:
        - resource
      run:
        cwlVersion: v1.2
        class: CommandLineTool
        baseCommand: ["blue-cwl", "register", "circuit"]
        inputs:
          - id: cell_composition
            type: NexusType
            inputBinding:
              position: 1
              prefix: "--cell-composition"
          - id: variant_config
            type: NexusType
            inputBinding:
              position: 2
              prefix: "--variant-config"
          - id: circuit_config
            type: File
            inputBinding:
              position: 3
              prefix: "--circuit-config"
              position: 3
        outputs:
          - id: partial_circuit
            type: NexusType
            outputBinding:
              glob: circuit.json

