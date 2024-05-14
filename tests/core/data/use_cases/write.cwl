cwlVersion: v1.2
class: CommandLineTool
id: write-command
baseCommand: ./write.py

inputs:
  message:
    type: string
    inputBinding:
      position: 1

outputs:
  example_file:
    type: File
    outputBinding:
      glob: $(inputs.message)_file-output.txt
