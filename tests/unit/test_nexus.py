import os
import jwt
import tempfile
import pytest
import requests
from unittest.mock import patch
from unittest.mock import Mock
from pathlib import Path
from cwl_registry import nexus as tested
from kgforge.core import Resource
from datetime import datetime

from functools import partial, wraps


import entity_management.state
from tests.unit.utils import patchenv
import entity_management.state


def _create_test_file(path, text):
    with open(path, "w", encoding="utf-8") as fd:
        fd.write(text)
    return path


@pytest.fixture
def mock_forge():
    with tempfile.TemporaryDirectory() as tdir:
        tdir = Path(tdir)

        parameters1_path = _create_test_file(tdir / "parameters1.yml", "foo1")
        parameters2_path = _create_test_file(tdir / "parameters2.yml", "foo2")
        definitions1_path = _create_test_file(tdir / "definitions1.cwl", "foo3")

        mock_variant = Resource(
            type="VariantConfig",
            generator_name="foo",
            variant_name="bar",
            version="0.1.0",
            configs=Resource(id="variant-parameters-id", type="VariantParameters"),
            allocation_resources=Resource(id="variant-resources-id", type="VariantResources"),
            definitions=Resource(id="variant-definitions-id", type="VariantDefinitions"),
        )
        mock_parameters = Resource(
            generator_name="foo",
            variant_name="bar",
            version="0.1.0",
            type="VariantParameters",
            hasPart=[
                Resource(
                    distribution=Resource(
                        atLocation=Resource(location=f"file://{str(parameters1_path)}"),
                        name="parameters1.yml",
                    ),
                ),
                Resource(
                    distribution=Resource(
                        atLocation=Resource(location=f"file://{str(parameters2_path)}"),
                        name="parameters2.yml",
                    ),
                ),
            ],
        )
        mock_definitions = Resource(
            generator_name="foo",
            variant_name="bar",
            version="0.1.0",
            type="VariantDefinitions",
            hasPart=[
                Resource(
                    distribution=Resource(
                        atLocation=Resource(location=f"file://{str(definitions1_path)}"),
                        name="definitions1.cwl",
                    ),
                ),
            ],
        )
        mock_resources = Resource(
            generator_name="foo",
            variant_name="bar",
            version="0.1.0",
            type="VariantDefinitions",
            hasPart=[],
        )
        mock_kg = {
            "variant-config-id": mock_variant,
            "variant-parameters-id": mock_parameters,
            "variant-resources-id": mock_resources,
            "variant-definitions-id": mock_definitions,
        }

        mock = Mock()
        mock.tdir = tdir
        mock.retrieve = lambda resource_id, cross_bucket: mock_kg[resource_id]

        yield mock


@patch(f"{tested.__name__}._decode")
def test_has_expired(mock):
    mock.return_value = {"exp": 2000}
    has_expired = tested._has_expired(mock)
    assert has_expired

    mock.return_value = {"exp": datetime.timestamp(datetime.now())}
    has_expired = tested._has_expired(mock)
    assert has_expired

    # within the seconds to expire
    mock.return_value = {
        "exp": datetime.timestamp(datetime.now()) + tested.SECONDS_TO_EXPIRATION / 2
    }
    has_expired = tested._has_expired(mock)
    assert has_expired

    mock.return_value = {
        "exp": datetime.timestamp(datetime.now()) + tested.SECONDS_TO_EXPIRATION * 2
    }
    has_expired = tested._has_expired(mock)
    assert not has_expired


def _create_token(token_type, is_expired=False):
    token_json = {}

    if token_type == "offline":
        token_json["typ"] = "Refresh"
    else:
        token_json["typ"] = "Bearer"

    if is_expired:
        token_json["exp"] = datetime.timestamp(datetime.now())
    else:
        token_json["exp"] = datetime.timestamp(datetime.now()) + 3600

    return jwt.encode(token_json, "secret")


def with_token(token_type, is_expired=False):
    """Generate token and set state to using that token."""

    def inner(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            token = _create_token(token_type, is_expired)

            env_vars = {"NEXUS_TOKEN": token} if token else {}
            with patch.dict(os.environ, env_vars, clear=True):
                old_access_token = tested.state.ACCESS_TOKEN
                old_offline_token = tested.state.OFFLINE_TOKEN

                # reset state
                tested.state.ACCESS_TOKEN = tested.state.OFFLINE_TOKEN = None
                tested.state.set_token(token)

                func(*args, **kwargs)

                # restore state
                tested.state.ACCESS_TOKEN = old_access_token
                tested.state.OFFLINE_TOKEN = old_offline_token

        return wrapper

    return inner


def with_state(offline_token=None, access_token=None):
    def inner(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with patch.dict(os.environ, {}, clear=True):
                old_access_token = tested.state.ACCESS_TOKEN
                old_offline_token = tested.state.OFFLINE_TOKEN

                # reset state
                tested.state.ACCESS_TOKEN = access_token
                tested.state.OFFLINE_TOKEN = offline_token

                func(*args, **kwargs)

                # restore state
                tested.state.ACCESS_TOKEN = old_access_token
                tested.state.OFFLINE_TOKEN = old_offline_token

        return wrapper

    return inner


@with_token("access", is_expired=False)
def test_state_access_token():
    """Test ACCESS_TOKEN is set in state when NEXUS_TOKEN has 'Bearer' type."""
    assert tested.state.ACCESS_TOKEN == os.environ["NEXUS_TOKEN"]
    assert tested.state.OFFLINE_TOKEN is None


@with_token("offline", is_expired=False)
def test_state_offline_token():
    """Test OFFLINE_TOKEN is set in state when NEXUS_TOKEN has 'Refresh' type."""
    assert tested.state.ACCESS_TOKEN == None
    assert tested.state.OFFLINE_TOKEN == os.environ["NEXUS_TOKEN"]


@with_token("access", is_expired=False)
def test_get_valid_token__None__ACCESS_TOKEN__not_expired():
    """Test getting a valid access token from the environment."""
    res = tested._get_valid_token(token=None, force_refresh=False)
    assert res == os.environ["NEXUS_TOKEN"]


@with_token("access", is_expired=True)
def test_get_valid_token__None__ACCESS_TOKEN__expired():
    """Test getting an expired access token from the environment.
    With no refresh token available the expired token should be returned.
    """
    res = tested._get_valid_token(token=None, force_refresh=False)
    assert res == os.environ["NEXUS_TOKEN"]


@pytest.mark.parametrize("force", (True, False))
@pytest.mark.parametrize("is_expired", (True, False))
@with_state(offline_token=None, access_token=None)
def test_get_valid_token__token(force, is_expired):
    """Test user passing the access token. Cannot be refreshed if there is not offline token."""
    token = _create_token("access", is_expired=is_expired)
    res = tested._get_valid_token(token=token, force_refresh=force)
    assert res == token


@pytest.mark.parametrize("force", (True, False))
@pytest.mark.parametrize("is_expired", (True, False))
@with_state(offline_token=None, access_token=True)
def test_get_valid_token__token__existing_access_token(force, is_expired):
    """Test user passing the access token. It should override the existing one.
    Cannot be refreshed if there is not offline token."""
    token = _create_token("access", is_expired=is_expired)
    res = tested._get_valid_token(token=token, force_refresh=force)
    assert res == token


@with_token("offline", is_expired=False)
def test_get_valid_token__None__OFFLINE_TOKEN__not_expired():
    """Test getting a valid access token from refreshing an offline one."""
    with patch.object(entity_management.state.KEYCLOAK, "refresh_token") as patched:
        mock_valid_token = _create_token("access", is_expired=False)

        patched.return_value = {"access_token": mock_valid_token}

        res = tested._get_valid_token(token=None, force_refresh=False)

        assert res == mock_valid_token
        assert tested.state.ACCESS_TOKEN == mock_valid_token
        assert tested.state.OFFLINE_TOKEN == os.environ["NEXUS_TOKEN"]


@with_state(
    offline_token=_create_token("offline", is_expired=False),
    access_token=_create_token("access_token", is_expired=False),
)
def test_get_valid_token__None__OFFLINE_TOKEN__ACCESS_TOKEN__not_expired():
    """Test access token is not refreshed when not expired."""
    assert entity_management.state.ACCESS_TOKEN is not None
    assert entity_management.state.OFFLINE_TOKEN is not None

    with patch.object(entity_management.state.KEYCLOAK, "refresh_token") as patched:
        mock_valid_token = _create_token("access", is_expired=True)

        patched.return_value = {"access_token": mock_valid_token}

        res = tested._get_valid_token(token=None, force_refresh=False)

        # access token should not be renewed because it is not expired
        assert entity_management.state.ACCESS_TOKEN is not None
        assert res == entity_management.state.ACCESS_TOKEN


@with_state(
    offline_token=_create_token("offline", is_expired=False),
    access_token=_create_token("access_token", is_expired=True),
)
def test_get_valid_token__None__OFFLINE_TOKEN__ACCESS_TOKEN__expired():
    """Test access token is refreshed when expired."""
    assert entity_management.state.ACCESS_TOKEN is not None
    assert entity_management.state.OFFLINE_TOKEN is not None

    with patch.object(entity_management.state.KEYCLOAK, "refresh_token") as patched:
        mock_valid_token = _create_token("access", is_expired=True)

        patched.return_value = {"access_token": mock_valid_token}

        res = tested._get_valid_token(token=None, force_refresh=False)

        # access token should be renewed because it is expired
        assert entity_management.state.ACCESS_TOKEN is not None
        assert res == mock_valid_token


@with_state(
    offline_token=_create_token("offline", is_expired=False),
    access_token=_create_token("access_token", is_expired=False),
)
def test_get_valid_token__None__OFFLINE_TOKEN__ACCESS_TOKEN__REFRESH__not_expired():
    """Test access token is renewed when force_refresh is True and there is an offline token."""
    assert entity_management.state.ACCESS_TOKEN is not None
    assert entity_management.state.OFFLINE_TOKEN is not None

    with patch.object(entity_management.state.KEYCLOAK, "refresh_token") as patched:
        mock_valid_token = _create_token("access", is_expired=True)

        patched.return_value = {"access_token": mock_valid_token}

        res = tested._get_valid_token(token=None, force_refresh=True)

        # access token should be renewed because it is expired
        assert entity_management.state.ACCESS_TOKEN is not None
        assert res == mock_valid_token


@with_state(
    offline_token=_create_token("offline", is_expired=False),
    access_token=_create_token("access_token", is_expired=True),
)
def test_refresh_token_on_failure():
    """Test access token is renewed if the wrapped function throws an http 401 error."""

    def mock_raiser(token):
        if tested._has_expired(token):
            e = requests.HTTPError("error")
            e.url = "my-url"
            e.code = "my-code"
            e.hdrs = "my-hrds"
            e.fp = 1
            e.request = Mock()
            e.response = Mock()
            e.response.status_code = 401
            e.response.request = Mock(body="my-body")
            raise e

        return token

    token = entity_management.state.ACCESS_TOKEN
    assert tested._has_expired(token)

    with patch.object(entity_management.state.KEYCLOAK, "refresh_token") as patched:
        patched.return_value = {"access_token": _create_token("access", is_expired=False)}

        new_token = tested._refresh_token_on_failure(mock_raiser)(token=token)

        assert not tested._has_expired(new_token)

        # expired token
        with pytest.raises(requests.HTTPError):
            patched.return_value = {"access_token": _create_token("access", is_expired=True)}
            new_token = tested._refresh_token_on_failure(mock_raiser)(token=token)


def test_retrieve_variant_data(mock_forge):
    data = tested.retrieve_variant_data(mock_forge, "variant-config-id")

    assert data["configs"] == {
        "parameters1.yml": mock_forge.tdir / "parameters1.yml",
        "parameters2.yml": mock_forge.tdir / "parameters2.yml",
    }
    assert data["definitions"] == {
        "definitions1.cwl": mock_forge.tdir / "definitions1.cwl",
    }
    assert data["resources"] == {}


def test_retrieve_variant_data__with_staging(mock_forge):
    with tempfile.TemporaryDirectory() as tdir:
        tdir = Path(tdir)
        data = tested.retrieve_variant_data(mock_forge, "variant-config-id", staging_dir=tdir)

        assert data["configs"] == {
            "parameters1.yml": tdir / "configs/parameters1.yml",
            "parameters2.yml": tdir / "configs/parameters2.yml",
        }
        assert data["configs"]["parameters1.yml"].is_symlink()
        assert data["configs"]["parameters2.yml"].is_symlink()

        assert data["configs"]["parameters1.yml"].resolve() == mock_forge.tdir / "parameters1.yml"
        assert data["configs"]["parameters2.yml"].resolve() == mock_forge.tdir / "parameters2.yml"

        assert data["definitions"] == {
            "definitions1.cwl": tdir / "definitions/definitions1.cwl",
        }
        assert data["definitions"]["definitions1.cwl"].is_symlink()
        assert (
            data["definitions"]["definitions1.cwl"].resolve()
            == mock_forge.tdir / "definitions1.cwl"
        )

        assert data["resources"] == {}
