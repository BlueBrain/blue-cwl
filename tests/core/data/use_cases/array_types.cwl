cwlVersion: v1.2
id: foo
class: CommandLineTool
inputs:
  filesA:
    type: File[]
    inputBinding:
      prefix: -A
      position: 1
  filesB:
    type:
      type: array
      items: File
      inputBinding:
        prefix: -B=
        separate: false
    inputBinding:
      position: 2
  filesC:
    type: File[]
    inputBinding:
      prefix: -C=
      itemSeparator: ","
      separate: false
      position: 4
outputs:
  output_file:
    type: File
    outputBinding:
      glob: foo.txt
baseCommand: touch foo.txt
