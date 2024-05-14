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
      glob: $(inputs.output_file)
