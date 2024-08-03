import sys
import shutil
import argparse
from pathlib import Path


def main(input_file, output_file, overwrite):
    if not overwrite and Path(output_file).exists():
        raise RuntimeError("Output file exists.")

    shutil.copyfile(input_file, output_file)


if __name__ == "__main__":

    args = sys.argv[1:]

    if "--overwrite" in args:
        args.remove("--overwrite")
        overwrite = True
    else:
        overwrite = False

    input_file, output_file = args

    main(input_file, output_file, overwrite)
