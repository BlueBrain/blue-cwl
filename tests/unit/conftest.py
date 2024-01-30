from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import voxcell

from cwl_registry.utils import load_json, write_json

DATA_DIR = Path(__file__).parent / "data"


MTYPE_URIS = {
    "L23_BP": "http://uri.interlex.org/base/ilx_0383198",
    "L5_TPC:A": "http://uri.interlex.org/base/ilx_0381365",
}


ETYPE_URIS = {
    "cADpyr": "http://bbp.epfl.ch/neurosciencegraph/ontologies/etypes/cADpyr",
    "dSTUT": "http://uri.interlex.org/base/ilx_0738202",
}


@pytest.fixture(scope="session")
def hierarchy_file():
    return DATA_DIR / "mba_hierarchy_v3l23split.json"


@pytest.fixture(scope="session")
def region_map(hierarchy_file):
    return voxcell.RegionMap.load_json(hierarchy_file)


@pytest.fixture(scope="session")
def annotation_file():
    return DATA_DIR / "atlas/brain_regions.nrrd"


@pytest.fixture(scope="session")
def annotation(annotation_file):
    return voxcell.VoxelData.load_nrrd(annotation_file)


@pytest.fixture
def region_volumes(region_map):
    from cwl_registry import brain_regions

    return brain_regions.volumes(region_map, brain_regions.all_acronyms())


@pytest.fixture(scope="session")
def density_distribution_file(tmpdir_factory, annotation):
    tdir = Path(tmpdir_factory.mktemp("densities"))

    v1_raw = np.zeros_like(annotation.raw)
    # mask selected so that it includes 0 and two regions: 320, 656
    v1_raw[100:120, 100:102, 100:120] = 20000.0
    path1 = tdir / "L23_BP__dSTUT_density.nrrd"
    annotation.with_data(v1_raw).save_nrrd(path1)

    v2_raw = np.zeros_like(annotation.raw)
    # empty
    path2 = tdir / "L5_TPCA__cADpyr_density.nrrd"
    annotation.with_data(v2_raw).save_nrrd(path2)

    df = pd.DataFrame(
        [
            ("L23_BP", MTYPE_URIS["L23_BP"], "dSTUT", ETYPE_URIS["dSTUT"], str(path1)),
            ("L5_TPC:A", MTYPE_URIS["L5_TPC:A"], "cADpyr", ETYPE_URIS["cADpyr"], str(path2)),
        ],
        columns=["mtype", "mtype_url", "etype", "etype_url", "path"],
    )

    distribution_file = tdir / "density_distribution.parquet"
    df.to_parquet(path=distribution_file)

    return distribution_file


@pytest.fixture(scope="session")
def density_distribution(density_distribution_file):
    return pd.read_parquet(density_distribution_file)


@pytest.fixture(scope="session")
def cell_composition_summary():
    return load_json(DATA_DIR / "schemas/cell_composition_summary_distribution.json")
