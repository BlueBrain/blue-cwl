cwlVersion: v1.2
class: CommandLineTool
baseCommand: cat
stdout: output.txt

inputs:
  f0:
    type: File
    inputBinding:
      position: 1

  f1:
    type: File
    inputBinding:
      position: 2

outputs:
  cat_out:
    type: stdout
