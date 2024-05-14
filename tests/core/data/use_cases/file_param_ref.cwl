cwlVersion: v1.2
class: CommandLineTool
baseCommand: [cp, -r]
inputs:
  indir:
    type: Directory
    inputBinding:
      position: 1
outputs:
  r1:
    type: File
    outputBinding:
      glob: $(inputs.indir.basename)/file.txt

  r2:
    type: File
    outputBinding:
      glob: $(inputs.indir.path)/file.txt

  r3:
    type: Directory
    outputBinding:
      glob: $(inputs.indir.path)/subdir

