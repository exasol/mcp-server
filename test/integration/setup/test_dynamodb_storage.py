"""Integration-style tests for the DynamoDB storage backend using moto server mode."""

import asyncio

import pytest

from exasol.ai.mcp.server.setup.generic_auth import (
    ENV_DYNAMODB_AWS_ACCESS_KEY_ID,
    ENV_DYNAMODB_AWS_SECRET_ACCESS_KEY,
    ENV_DYNAMODB_ENDPOINT_URL,
    ENV_DYNAMODB_REGION_NAME,
    ENV_DYNAMODB_TABLE_NAME,
    ENV_STORAGE_BACKEND,
    create_client_storage,
)

pytest.importorskip("moto", reason="moto not installed")
pytest.importorskip("aioboto3", reason="aioboto3 not installed")

from moto.server import ThreadedMotoServer  # noqa: E402


@pytest.fixture(scope="module")
def moto_server(run_on_itde):
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server._server.server_address
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture(autouse=True)
def _dynamodb_env(monkeypatch, moto_server):
    monkeypatch.setenv(ENV_STORAGE_BACKEND, "dynamodb")
    monkeypatch.setenv(ENV_DYNAMODB_TABLE_NAME, "test-oauth-tokens")
    monkeypatch.setenv(ENV_DYNAMODB_REGION_NAME, "us-east-1")
    monkeypatch.setenv(ENV_DYNAMODB_AWS_ACCESS_KEY_ID, "test")
    monkeypatch.setenv(ENV_DYNAMODB_AWS_SECRET_ACCESS_KEY, "test")
    monkeypatch.setenv(ENV_DYNAMODB_ENDPOINT_URL, moto_server)


def test_dynamodb_storage_put_and_get():
    storage = create_client_storage()

    async def run():
        async with storage:
            await storage.put("key1", {"token": "abc"}, collection="sessions")
            return await storage.get("key1", collection="sessions")

    assert asyncio.run(run()) == {"token": "abc"}


def test_dynamodb_storage_missing_key_returns_none():
    storage = create_client_storage()

    async def run():
        async with storage:
            return await storage.get("nonexistent", collection="sessions")

    assert asyncio.run(run()) is None


def test_dynamodb_storage_delete():
    storage = create_client_storage()

    async def run():
        async with storage:
            await storage.put("key1", {"token": "abc"}, collection="sessions")
            await storage.delete("key1", collection="sessions")
            return await storage.get("key1", collection="sessions")

    assert asyncio.run(run()) is None
