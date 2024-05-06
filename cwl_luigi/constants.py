"""Constants definitions."""

SPACK_MODULEPATH = "/gpfs/bbp.cscs.ch/ssd/apps/bsd/modules/_meta"
MODULES_ENABLE_PATH = "/etc/profile.d/modules.sh"

APPTAINER_MODULEPATH = SPACK_MODULEPATH
APPTAINER_MODULES = ["unstable", "singularityce"]
APPTAINER_EXECUTABLE = "singularity"
APPTAINER_OPTIONS = "--cleanenv --containall --bind $TMPDIR:/tmp,/gpfs/bbp.cscs.ch/project"
APPTAINER_IMAGEPATH = "/gpfs/bbp.cscs.ch/ssd/containers"
