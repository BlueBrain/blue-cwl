cwlVersion: v1.2
class: CommandLineTool
baseCommand: [cp]
inputs:
  source_file:
    type: File
    inputBinding:
      position: 1
  target_file:
    type: string
    inputBinding:
      position: 2
outputs:
  out_file:
    type: File
    outputBinding:
      glob: $(inputs.target_file)
