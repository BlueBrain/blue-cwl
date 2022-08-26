cwlVersion: v1.2
class: CommandLineTool
baseCommand: ./echo-and-write.py

stdout: output.txt

inputs:
  message:
    type: string
    inputBinding:
      position: 1

outputs:
  example_stdout:
    type: stdout

  example_file:
    type: File
    outputBinding:
      glob: file-output.txt
