"""Variant entry."""

import importlib.resources
import io
import logging
import tempfile
from copy import deepcopy
from pathlib import Path
from typing import cast

from blue_cwl.core import cwl, parse_cwl_file
from entity_management.base import attributes
from entity_management.core import DataDownload, Entity
from entity_management.nexus import sparql_query
from entity_management.util import AttrOf, unquote_uri_path

from blue_cwl.exceptions import CWLRegistryError
from blue_cwl.typing import StrOrPath
from blue_cwl.utils import dump_yaml, load_yaml, write_yaml

L = logging.getLogger(__name__)


CWL_DEFINITION_FILENAME = "definition.cwl"


# pylint: disable=no-member


@attributes(
    {
        "hasPart": AttrOf(list[dict]),
    }
)
class _CompatEntity(Entity):
    pass


@attributes(
    {
        "generator_name": AttrOf(str),
        "variant_name": AttrOf(str),
        "version": AttrOf(str),
        "name": AttrOf(str, default=None),
        "distribution": AttrOf(DataDownload, default=None),
        "allocation_resources": AttrOf(_CompatEntity, default=None),
        "definitions": AttrOf(_CompatEntity, default=None),
    },
    repr=False,
)
class Variant(Entity):
    """Variant class."""

    def __repr__(self):
        """Return repr string."""
        return f"Variant({self.generator_name}, {self.variant_name}, {self.version})"

    @property
    def overview(self) -> str:
        """Return detailed representation."""
        return (
            f"generator_name: {self.generator_name}\n"
            f"variant_name  : {self.variant_name}\n"
            f"version       : {self.version}\n\n"
            f"content\n-------\n\n{dump_yaml(self.get_content())}"
        )

    def get_content(self, *, token: str | None = None) -> dict:
        """Return definition content."""
        return _load_variant_distribution(self, token=token)

    @classmethod
    def from_id(
        cls,
        resource_id: str,
        *,
        base: str | None = None,
        org: str | None = None,
        proj: str | None = None,
        use_auth: str | None = None,
        **kwargs,
    ):  # pylint: disable=arguments-differ
        """Load entity from resource id."""

        def raise_on_no_result(resource_id: str, *_, **__) -> None:
            raise CWLRegistryError(f"Variant id {resource_id} was not found.")

        return super().from_id(
            resource_id=resource_id,
            on_no_result=raise_on_no_result,
            base=base,
            org=org,
            proj=proj,
            use_auth=use_auth,
            **kwargs,
        )

    @classmethod
    def from_search(
        cls,
        generator_name: str,
        variant_name: str,
        version: str,
        *,
        base: str | None = None,
        org: str | None = None,
        proj: str | None = None,
        token: str | None = None,
    ) -> "Variant":
        """Create a Variant instance by searching the Knowledge Graph."""
        resource_id = search_variant_in_nexus(
            generator_name=generator_name,
            variant_name=variant_name,
            version=version,
            base=base,
            org=org,
            proj=proj,
            token=token,
            raise_if_not_found=True,
        )
        return cls.from_id(
            resource_id=cast(str, resource_id), base=base, org=org, proj=proj, use_auth=token
        )

    @classmethod
    def from_file(
        cls, filepath: Path, generator_name: str, variant_name: str, version: str
    ) -> "Variant":
        """Create Variant object from variant file definition."""
        return cls(
            generator_name=generator_name,
            variant_name=variant_name,
            version=version,
            distribution=_create_local_variant_distribution(path=filepath),
            name=f"{generator_name}|{variant_name}|{version}",
        )

    @classmethod
    def from_registry(cls, generator_name: str, variant_name: str, version: str) -> "Variant":
        """Create Variant object from registry entry."""
        return cls.from_file(
            filepath=_get_variant_file(generator_name, variant_name, version),
            generator_name=generator_name,
            variant_name=variant_name,
            version=version,
        )

    @property
    def tool_definition(self) -> cwl.CommandLineTool | cwl.Workflow:
        """Return the cwl definition for this variant."""
        # TODO: Deprecate
        return self.definition

    @property
    def definition(self) -> cwl.CommandLineTool | cwl.Workflow:
        """Return the cwl definition for this variant."""
        content = deepcopy(self.get_content())

        # workaround until the cwl supports executors/resources within the definition
        if "resources" in content:
            del content["resources"]

        # TODO: make CommandLineTool constructor work with both the data and the file
        with tempfile.NamedTemporaryFile(suffix=".cwl") as tfile:
            write_yaml(data=content, filepath=tfile.name)
            return parse_cwl_file(tfile.name)

    def evolve(self, **changes) -> "Variant":
        """Create a new Variant instance with updated attributes.

        Note: if 'path' is passed, a local distribution will be added in the new instance.
        """
        if "path" in changes:
            changes["distribution"] = _create_local_variant_distribution(path=changes.pop("path"))

        changes["name"] = (
            f"{changes.get('generator_name', self.generator_name)}|"
            f"{changes.get('variant_name', self.variant_name)}|"
            f"{changes.get('version', self.version)}"
        )
        return super().evolve(**changes)

    def publish(
        self,
        *,
        update: bool = False,
        base: str | None = None,
        org: str | None = None,
        proj: str | None = None,
        token: str | None = None,
        **kwargs,
    ):  # pylint: disable=arguments-differ
        """Publish or update Variant entity."""
        variant = self

        # Variants are assumed unique within each bucket. If the variant is local, a search is made
        # to get the remote variant id, if any, and update it.
        if not variant.get_id():
            resource_id = search_variant_in_nexus(
                generator_name=self.generator_name,
                variant_name=self.variant_name,
                version=self.version,
                base=base,
                org=org,
                proj=proj,
                token=token,
                raise_if_not_found=False,
            )
            if resource_id:
                variant = variant.evolve()
                # pylint: disable=protected-access
                variant._force_attr("_id", resource_id)

        if variant.get_id() and not update:
            raise CWLRegistryError(
                f"Variant {self} already registered with id {variant.get_id()}. "
                "To update the existing resource set update=True."
            )

        # local distribution not yet registered in nexus
        if variant.distribution.url is not None:
            path = variant.distribution.get_url_as_path()

            with tempfile.TemporaryDirectory() as tdir:
                definition_file = str(Path(tdir, "definition.cwl"))
                packed_data = _pack(load_yaml(path), Path(path).parent)
                write_yaml(data=packed_data, filepath=definition_file)
                buffer = io.BytesIO(Path(unquote_uri_path(definition_file)).read_bytes())

            # the from_file classmethod uploads the distribution file to nexus
            distribution = DataDownload.from_file(
                file_like=buffer,
                name="definition.cwl",
                content_type="application/cwl",
                base=base,
                org=org,
                proj=proj,
                use_auth=token,
            )

            # make a new DataDownload instance replacing the local with the remote distribution
            variant = variant.evolve(distribution=distribution)

        return super(Variant, variant).publish(
            base=base,
            org=org,
            proj=proj,
            use_auth=token,
            **kwargs,
        )


def _create_local_variant_distribution(path: StrOrPath) -> DataDownload:
    path = Path(path).resolve()

    if not path.exists():
        raise CWLRegistryError(f"Path {path} does not exist.")

    if path.suffix != ".cwl":
        raise CWLRegistryError(f"Path {path} does not have a cwl suffix.")

    return DataDownload(
        url=f"file://{path}",
        name="definition.cwl",
        encodingFormat="application/cwl",
    )


def _load_variant_distribution(
    variant: Variant,
    *,
    token: str | None = None,
) -> dict:
    distribution = variant.distribution

    # backwards compatibility to old format
    if distribution is None:
        if not variant.allocation_resources:
            raise CWLRegistryError("No allocation resources found.")

        if not variant.definitions:
            raise CWLRegistryError("No definitions found.")

        payload = load_yaml(
            unquote_uri_path(
                variant.definitions.hasPart[0]["distribution"]["atLocation"]["location"]
            )
        )
        payload.update(
            load_yaml(
                unquote_uri_path(
                    variant.allocation_resources.hasPart[0]["distribution"]["atLocation"][
                        "location"
                    ]
                )
            )
        )
        return payload

    # local distribution
    if distribution.url is not None:
        path = unquote_uri_path(distribution.url)
        return _pack(load_yaml(path), Path(path).parent)

    # remote distribution
    with tempfile.TemporaryDirectory() as tdir:
        path = distribution.download(path=tdir, use_auth=token)
        return _pack(load_yaml(path), Path(path).parent)


def _pack(document: dict, base_dir: Path):
    if document["class"] == "CommandLineTool":
        return document

    base_dir = base_dir.resolve()

    if isinstance(document["steps"], dict):
        for value in document["steps"].values():
            tool = value["run"]
            if isinstance(tool, str):
                value["run"] = load_yaml(base_dir / tool)

    for step in document["steps"]:
        tool = step["run"]
        if isinstance(tool, str):
            step["run"] = load_yaml(base_dir / tool)

    return document


def search_variant_in_nexus(
    generator_name: str,
    variant_name: str,
    version: str,
    *,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
    raise_if_not_found: bool = False,
) -> str | None:
    """Search for a variant with the given generator_name, variant_name, and version in nexus.

    Args:
        generator_name: Generator's name. Example: 'cell_position'
        variant_name: Variant's name. Example: 'neurons_cell_position'
        version: The version of this variant's definition. Example: 'v1-dev'
        base: Nexus instance base url.
        org: Nexus organization.
        proj: Nexus project.
        token: Optional OAuth token.
        raise_if_not_found: If True it raises an error if no variant is found.

    Returns:
        Variant's id if a variant is found, None otherwise.
    """
    query = f"""
        PREFIX sch: <http://schema.org/>
        PREFIX bmo: <https://bbp.epfl.ch/ontologies/core/bmo/>
        PREFIX nxv: <https://bluebrain.github.io/nexus/vocabulary/>
        SELECT ?id
        WHERE {{
          ?id a bmo:Variant ;
             nxv:deprecated false ;
             bmo:generator_name '{generator_name}' ;
             bmo:variant_name '{variant_name}' ;
             sch:version '{version}' .
        }}
        LIMIT 1
    """
    result = sparql_query(
        query=query,
        base=base,
        org=org,
        proj=proj,
        token=token,
    )["results"]["bindings"]

    if result:
        resource_id = result[0]["id"]["value"]
        L.info(
            "Found id %s for variant (%s, %s, %s)",
            resource_id,
            generator_name,
            variant_name,
            version,
        )
        return resource_id

    if raise_if_not_found:
        raise RuntimeError(
            f"No variant definition found:\n"
            f"Generator Name: {generator_name}\n"
            f"Variant Name  : {variant_name}\n"
            f"Version       : {version}\n"
            f"Sparql Query:\n{query}"
        )
    return result


def _get_variant_directory(generator_name: str, variant_name: str, version: str) -> Path:
    package_path = importlib.resources.files("blue_cwl")
    L.debug("Package path: %s", package_path)
    return _get_variant_dir(package_path, generator_name, variant_name, version)


def _get_variant_file(generator_name: str, variant_name: str, version: str) -> Path:
    variant_dir = _get_variant_directory(generator_name, variant_name, version)

    if match := list(variant_dir.glob(CWL_DEFINITION_FILENAME)):
        return match[0]

    raise CWLRegistryError(f"There is no '{CWL_DEFINITION_FILENAME}' in {variant_dir}.")


def _check_directory_exists(directory: Path) -> Path:
    if not directory.exists():
        raise CWLRegistryError(f"Directory '{directory}' does not exist.")
    return directory


def _get_variant_dir(package_path, generator_name: str, variant_name: str, version: str) -> Path:
    root_dir = _check_directory_exists(package_path / "generators")
    generator_dir = _check_directory_names(root_dir / generator_name)
    variant_dir = _check_directory_names(generator_dir / variant_name)

    version_dir = _check_directory_names(variant_dir / version)

    return version_dir


def _check_directory_names(directory: Path) -> Path:
    if not directory.is_dir():
        names = sorted(x.name for x in directory.parent.iterdir() if x.is_dir())
        raise CWLRegistryError(
            f"Directory '{directory.name}' does not exist. " f"Available names: {names}"
        )
    return directory
