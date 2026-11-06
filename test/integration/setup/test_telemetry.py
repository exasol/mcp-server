import json

import pytest
from exasol.telemetry import client as telemetry
from exasol.telemetry.client import config as telemetry_config
from exasol.telemetry.client.config import (
    ENV_CI,
    ENV_DISABLE,
    ENV_ENDPOINT,
)

from exasol.ai.mcp.server.main import (
    _PROJECT_SHORT_TAG,
    setup_telemetry,
)
from exasol.ai.mcp.server.version import __version__


def _reset_telemetry() -> None:
    # shutdown() keeps the stored configuration, which would prevent track() from
    # setting up the library again with the endpoint of the next test.
    telemetry.shutdown(flush_buffers=False)
    telemetry_config.store(None)


@pytest.fixture(autouse=True)
def _clean_telemetry_state():
    _reset_telemetry()
    yield
    _reset_telemetry()


def test_setup_telemetry_sends_started_event(httpserver, monkeypatch) -> None:
    """
    Exercises the real exasol-telemetry-client machinery (background worker
    thread, HTTP POST) end to end, instead of mocking ``telemetry.track``,
    to verify that ``setup_telemetry`` actually delivers the "started" event.
    """
    monkeypatch.setenv(ENV_ENDPOINT, httpserver.url_for("/telemetry"))
    monkeypatch.delenv(ENV_DISABLE, raising=False)
    monkeypatch.delenv(ENV_CI, raising=False)
    httpserver.expect_request("/telemetry", method="POST").respond_with_json({})

    setup_telemetry()
    telemetry.shutdown(flush_buffers=True)

    request, _ = httpserver.log[0]
    message = json.loads(request.get_data())
    assert message["category"] == _PROJECT_SHORT_TAG
    assert message["productVersion"] == __version__
    assert "started" in message["features"]
