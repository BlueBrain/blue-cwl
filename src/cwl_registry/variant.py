"""Variant entry."""
import importlib.resources
import logging
import tempfile
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path

from cwl_luigi import cwl
from entity_management.core import DataDownload
from entity_management.nexus import sparql_query
from entity_management.util import unquote_uri_path

from cwl_registry.entity import Variant as VariantEntity
from cwl_registry.exceptions import CWLRegistryError
from cwl_registry.nexus import get_forge, get_resource
from cwl_registry.utils import dump_yaml, load_yaml, write_yaml

L = logging.getLogger(__name__)


@dataclass
class Variant:
    """Variant class."""

    generator_name: str
    variant_name: str
    version: str
    content: dict

    def __repr__(self):
        """Return repr string."""
        return f"Variant({self.generator_name}, {self.variant_name}, {self.version})"

    @property
    def overview(self):
        """Return detailed representation."""
        return (
            f"generator_name: {self.generator_name}\n"
            f"variant_name  : {self.variant_name}\n"
            f"version       : {self.version}\n\n"
            f"content\n-------\n\n{dump_yaml(self.content)}"
        )

    @classmethod
    def from_id(
        cls,
        resource_id,
        *,
        base: str = None,
        org: str = None,
        proj: str = None,
        token: str = None,
        cross_bucket: bool = False,
    ):
        """Create a Variant object from a nexus definition resource."""
        generator_name, variant_name, version, content = _get_variant_data(
            resource_id,
            base=base,
            org=org,
            proj=proj,
            token=token,
            cross_bucket=cross_bucket,
        )
        return cls(
            generator_name=generator_name,
            variant_name=variant_name,
            version=version,
            content=content,
        )

    @classmethod
    def from_search(
        cls,
        generator_name: str,
        variant_name: str,
        version: str,
        *,
        base: str = None,
        org: str = None,
        proj: str = None,
        token: str = None,
    ) -> "Variant":
        """Create a Variant instance by searching the Knowledge Graph."""
        resource_id = _find_variant_id_from_nexus(
            generator_name=generator_name,
            variant_name=variant_name,
            version=version,
            base=base,
            org=org,
            proj=proj,
            token=token,
        )
        return cls.from_id(resource_id=resource_id, base=base, org=org, proj=proj, token=token)

    @classmethod
    def from_file(
        cls, filepath: Path, generator_name: str, variant_name: str, version: str
    ) -> "Variant":
        """Create Variant object from variant file definition."""
        return cls(
            generator_name=generator_name,
            variant_name=variant_name,
            version=version,
            content=load_yaml(filepath),
        )

    @classmethod
    def from_registry(cls, generator_name: str, variant_name: str, version: str) -> "Variant":
        """Create Variant object from registry entry."""
        filepath = _get_variant_file(generator_name, variant_name, version)
        return cls.from_file(
            filepath=filepath,
            generator_name=generator_name,
            variant_name=variant_name,
            version=version,
        )

    @property
    def tool_definition(self):
        """Return the cwl definition for this variant."""
        content = deepcopy(self.content)

        # workaround until the cwl supports executors/resources within the definition
        if "resources" in content:
            del content["resources"]

        with tempfile.NamedTemporaryFile(suffix=".cwl") as tfile:
            write_yaml(data=content, filepath=tfile.name)
            return cwl.CommandLineTool.from_cwl(tfile.name)

    def publish(
        self,
        *,
        update: bool = False,
        base: str = None,
        org: str = None,
        proj: str = None,
        token: str = None,
    ):
        """Publish or update Variant entity."""
        try:
            existing_id = _find_variant_id_from_nexus(
                self.generator_name,
                self.variant_name,
                self.version,
                base=base,
                org=org,
                proj=proj,
                token=token,
            )
        except RuntimeError:
            existing_id = None

        if existing_id and not update:
            raise CWLRegistryError(f"Variant {self} already registered in KG with id {existing_id}")

        with tempfile.NamedTemporaryFile(suffix=".cwl") as tfile:
            write_yaml(data=self.content, filepath=tfile.name)

            distribution = DataDownload.from_file(
                tfile.name,
                name="definition.cwl",
                content_type="application/cwl",
                base=base,
                org=org,
                proj=proj,
                use_auth=token,
            )

        if existing_id and update:
            variant = VariantEntity.from_id(
                existing_id, base=base, org=org, proj=proj, use_auth=token, cross_bucket=True
            ).evolve(distribution=distribution)
        else:
            variant = VariantEntity(
                name=f"{self.generator_name}|{self.variant_name}|{self.version}",
                generator_name=self.generator_name,
                variant_name=self.variant_name,
                version=self.version,
                distribution=distribution,
            )

        return variant.publish(base=base, org=org, proj=proj, use_auth=token)


def _get_variant_data(
    resource_id,
    *,
    base: str = None,
    org: str = None,
    proj: str = None,
    token: str = None,
    cross_bucket: bool = False,
):
    try:
        variant_entity = VariantEntity.from_id(
            resource_id,
            base=base,
            org=org,
            proj=proj,
            use_auth=token,
            cross_bucket=cross_bucket,
        )

        assert variant_entity is not None, f"{resource_id} failed to instantiate."

        with tempfile.TemporaryDirectory() as tdir:
            filepath = variant_entity.distribution.download(path=tdir, use_auth=token)
            content = load_yaml(filepath)

        return (
            variant_entity.generator_name,
            variant_entity.variant_name,
            variant_entity.version,
            content,
        )
    except TypeError:
        # legacy resources
        forge = get_forge(nexus_base=base, nexus_org=org, nexus_project=proj, nexus_token=token)
        resource = get_resource(forge, resource_id)
        assert resource is not None

        definitions = get_resource(forge, resource.definitions.id)
        path = unquote_uri_path(definitions.hasPart[0].distribution.atLocation.location)

        content = load_yaml(path)

        resources = get_resource(forge, resource.allocation_resources.id)
        path = unquote_uri_path(resources.hasPart[0].distribution.atLocation.location)

        content["resources"] = load_yaml(path)["resources"]

        return (
            resource.generator_name,
            resource.variant_name,
            resource.version,
            content,
        )


def _find_variant_id_from_nexus(
    generator_name: str,
    variant_name: str,
    version: str,
    *,
    base: str = None,
    org: str = None,
    proj: str = None,
    token: str = None,
) -> str:
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
    )[
        "results"
    ]["bindings"]

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
    raise RuntimeError(
        f"No variant definition found:\n"
        f"Generator Name: {generator_name}\n"
        f"Variant Name  : {variant_name}\n"
        f"Version       : {version}\n"
        f"Sparql Query:\n{query}"
    )


def _get_variant_directory(generator_name: str, variant_name: str, version: str):
    package_path = importlib.resources.files("cwl_registry")
    L.debug("Package path: %s", package_path)

    variant_dir = _get_variant_dir(package_path, generator_name, variant_name, version)

    return variant_dir


def _get_variant_file(generator_name: str, variant_name: str, version: str):
    variant_dir = _get_variant_directory(generator_name, variant_name, version)

    files = list(variant_dir.glob("*.cwl"))

    if len(files) == 1:
        return files[0]
    else:
        raise CWLRegistryError(f"One cwl file is allowed in {variant_dir}. Found: {len(files)}.")


def _check_directory_exists(directory: Path) -> Path:
    if not directory.exists():
        raise CWLRegistryError(f"Directory '{directory}' does not exist.")
    return directory


def _get_variant_dir(package_path, generator_name: str, variant_name: str, version: str) -> str:
    root_dir = _check_directory_exists(package_path / "generators")
    generator_dir = _check_directory_names(root_dir / generator_name)
    variant_dir = _check_directory_names(generator_dir / variant_name)

    version_dir = _check_directory_names(variant_dir / version)

    return version_dir


def _check_directory_names(directory: Path):
    if not directory.is_dir():
        names = sorted(x.name for x in directory.parent.iterdir() if x.is_dir())
        raise CWLRegistryError(
            f"Directory '{directory.name}' does not exist. " f"Available names: {names}"
        )
    return directory
