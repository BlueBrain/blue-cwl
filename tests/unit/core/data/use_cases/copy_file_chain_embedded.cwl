cwlVersion: v1.2
class: Workflow

id: copy-file-chain
label: Copy files in a chain

inputs:

    input_file:
      type: File

    output_dir:
      type: Directory

    overwrite:
      type: boolean
      default: false

outputs:

    output_file:
      type: File
      outputSource: s2/output_file

steps:

  - id: s0
    run:
        cwlVersion: v1.2
        class: CommandLineTool
        id: write-command
        baseCommand: ['python3', './copy_file.py']

        inputs:
          input_file:
            type: File
            inputBinding:
              position: 1

          output_file:
            type: File
            inputBinding:
              position: 2

          overwrite:
            type: boolean
            inputBinding:
              prefix: --overwrite
            default: false

        outputs:
          output_file:
            type: File
            outputBinding:
              glob: $(inputs.output_file.path)
    in:
      input_file: input_file
      output_file:
        source: output_dir
        valueFrom: $(self.path)/s0_output_file.txt
      overwrite:
        default: true
    out:
      - output_file

  - id: s1
    run:
        cwlVersion: v1.2
        class: CommandLineTool
        id: write-command
        baseCommand: ['python3', './copy_file.py']

        inputs:
          input_file:
            type: File
            inputBinding:
              position: 1

          output_file:
            type: File
            inputBinding:
              position: 2

          overwrite:
            type: boolean
            inputBinding:
              prefix: --overwrite
            default: false

        outputs:
          output_file:
            type: File
            outputBinding:
              glob: $(inputs.output_file.path)
    in:
      input_file: s0/output_file
      output_file:
        source: output_dir
        valueFrom: $(self.path)/s1_output_file.txt
      overwrite: overwrite
    out:
      - output_file

  - id: s2
    run:
        cwlVersion: v1.2
        class: CommandLineTool
        id: write-command
        baseCommand: ['python3', './copy_file.py']

        inputs:
          input_file:
            type: File
            inputBinding:
              position: 1

          output_file:
            type: File
            inputBinding:
              position: 2

          overwrite:
            type: boolean
            inputBinding:
              prefix: --overwrite
            default: false

        outputs:
          output_file:
            type: File
            outputBinding:
              glob: $(inputs.output_file.path)
    in:
      input_file: s1/output_file
      output_file:
        source: output_dir
        valueFrom: $(self.path)/s2_output_file.txt
      overwrite:
        default: true
    out:
      - output_file
