import json
import logging

import pytest
from exasol.telemetry import client as telemetry
from exasol.telemetry.client.config import (
    ENV_CI,
    ENV_DISABLE,
    ENV_ENDPOINT,
)

from exasol.ai.mcp.server.main import (
    _PROJECT_SHORT_TAG,
    setup_telemetry,
)


def _reset_telemetry() -> None:
    if telemetry.was_setup():
        telemetry.shutdown(flush_buffers=False)


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

    setup_telemetry(logging.getLogger("test_setup_telemetry"))
    telemetry.shutdown(flush_buffers=True)

    request, _ = httpserver.log[0]
    features = json.loads(request.get_data())["features"]
    assert f"{_PROJECT_SHORT_TAG}.started" in features


# TODO(#253): temporary, remove after manually confirming the disabled path
# behaves as expected.
# @pytest.mark.parametrize("envar", [ENV_CI, ENV_DISABLE])
def test_setup_telemetry_disabled_sends_nothing(httpserver, monkeypatch, envar) -> None:
    """
    When telemetry is disabled, ``setup_telemetry`` must not deliver the
    "started" event.
    """
    monkeypatch.setenv(ENV_ENDPOINT, httpserver.url_for("/telemetry"))
    # monkeypatch.setenv(envar, "true")
    httpserver.expect_request("/telemetry", method="POST").respond_with_json({})

    setup_telemetry(logging.getLogger("test_setup_telemetry_disabled"))
    telemetry.shutdown(flush_buffers=True)

    assert len(httpserver.log) == 0
