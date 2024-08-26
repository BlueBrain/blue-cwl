cwlVersion: v1.2
class: CommandLineTool
baseCommand: [cp, -r]
inputs:
  indir:
    type: Directory
    inputBinding:
      position: 1
  outdir:
    type: string
    inputBinding:
      position: 2
outputs:
  r1:
    type: File
    outputBinding:
      glob: $(inputs.outdir)/file1.txt

  r2:
    type: File
    outputBinding:
      glob: $(inputs.outdir)/file2.txt

  r3:
    type: Directory
    outputBinding:
      glob: $(inputs.outdir)/subdir
