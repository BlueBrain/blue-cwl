"""Functions applied on libsonata populations."""
import pandas as pd

HRM = ["hemisphere", "region", "mtype"]


def _make_categorical(population, name):
    codes = population.get_enumeration(name, population.select_all())
    categories = population.enumeration_values(name)
    return pd.Categorical.from_codes(codes=codes, categories=categories)


def _get_HRM_multi_index(population):
    df = pd.DataFrame({name: _make_categorical(population, name) for name in HRM})
    return df.set_index(HRM).index


def get_HRM_counts(population) -> pd.Series:
    """Return the number of cells for each (hemisphere, region, mtype) in the population."""
    df = pd.DataFrame({name: _make_categorical(population, name) for name in HRM})

    counts = df.groupby(HRM).value_counts()

    # remove the zeros from the grouping
    counts = counts[counts != 0]

    return counts


def _get_HRM_properties(population, properties: list[str]) -> pd.DataFrame:
    categoricals = population.enumeration_names

    def get_attribute(name):
        if name in categoricals:
            return _make_categorical(population, name)
        return population.get_attribute(name, population.select_all())

    return pd.DataFrame(
        {name: get_attribute(name) for name in properties}, index=_get_HRM_multi_index(population)
    )


def get_HRM_positions(population):
    """Return positions in population indexed by hemisphere, region, and mtype."""
    return _get_HRM_properties(population, ["x", "y", "z"])
