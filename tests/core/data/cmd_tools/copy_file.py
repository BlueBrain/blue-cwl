import sys
import shutil
import click
from pathlib import Path


@click.command()
@click.argument("input_file")
@click.argument("output_file")
@click.option("--overwrite", is_flag=True)
def main(input_file, output_file, overwrite):
    if not overwrite and Path(output_file).exists():
        raise RuntimeError("Output file exists.")

    shutil.copyfile(input_file, output_file)


if __name__ == "__main__":
    main()
