cwlVersion: v1.2
class: CommandLineTool

id: cat-command
baseCommand: cat

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
    type: File
    outputBinding:
        glob: output.txt
