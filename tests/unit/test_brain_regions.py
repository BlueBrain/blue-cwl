from cwl_registry import brain_regions as test_module

from numpy import testing as npt


def test_all_acronyms():
    res = test_module.all_acronyms()
    assert len(res) == 708


def test_volumes(region_map):
    acronyms = ["SSp-bfd2", "CA1"]

    res = test_module.volumes(region_map, acronyms)

    assert res.index.tolist() == acronyms
    npt.assert_allclose(res.values, [534765625.0, 10209234375.0])
