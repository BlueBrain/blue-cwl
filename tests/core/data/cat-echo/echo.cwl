cwlVersion: v1.2
class: CommandLineTool
id: echo-command
baseCommand: ./echo-and-write.py

inputs:
  message:
    type: string
    inputBinding:
      position: 1

outputs:
  example_file:
    type: File
    outputBinding:
      glob: file-output.txt
