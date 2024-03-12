"""Functions applied on libsonata populations."""

import libsonata
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

    counts = df.groupby(HRM, observed=True).value_counts()

    # remove the zeros from the grouping
    counts = counts[counts != 0]

    return counts


def _get_HRM_properties(population, properties: list[str], selection=None) -> pd.DataFrame:
    categoricals = population.enumeration_names
    selection = selection or population.select_all()

    def get_attribute(name):
        if name in categoricals:
            return _make_categorical(population, name)
        return population.get_attribute(name, selection)

    return pd.DataFrame(
        {name: get_attribute(name) for name in properties}, index=_get_HRM_multi_index(population)
    )


def get_HRM_positions(population):
    """Return positions in population indexed by hemisphere, region, and mtype."""
    return _get_HRM_properties(population, ["x", "y", "z"])


def get_pathways(
    edge_population: libsonata.EdgePopulation,
    source_node_population: libsonata.NodePopulation,
    target_node_population: libsonata.NodePopulation,
    properties: list[str],
) -> pd.DataFrame:
    """Return the properties of the pathways.

    Args:
        edge_population: The libsonata edge population.
        source_node_population: The source node population.
        target_node_population: The target node population.
        properties: The list of N properties to fetch.

    Returns:
        A dataframe of 2xN columns where properties are prefixed by 'source' or 'target'.
        For example for two properties [region, mtype] the resulting dataframe will have
        [source_region, source_mtype, target_region, target_mtype]
    """
    source_nodes = edge_population.source_nodes(edge_population.select_all())
    target_nodes = edge_population.target_nodes(edge_population.select_all())

    source_properties = (
        _get_HRM_properties(
            population=source_node_population,
            properties=properties,
            selection=libsonata.Selection(source_nodes),
        )
        .reset_index(drop=True)
        .rename(columns={prop: f"source_{prop}" for prop in properties})
    )

    target_properties = (
        _get_HRM_properties(
            population=target_node_population,
            properties=properties,
            selection=libsonata.Selection(target_nodes),
        )
        .reset_index(drop=True)
        .rename(columns={prop: f"target_{prop}" for prop in properties})
    )

    result = pd.concat([source_properties, target_properties], axis=1)

    return result
