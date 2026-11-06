"""OpenAPI access and mutation/replay metadata matches enforced product contracts."""

import pytest

from scripts.openapi_schemas import document


@pytest.mark.parametrize(argnames="auth_enabled", argvalues=[False, True])
def test_openapi_security_and_mutation_headers_match_access_mode(*, auth_enabled: bool) -> None:
    schema = document(auth_enabled=auth_enabled)
    paths = schema["paths"]
    assert isinstance(paths, dict)
    security = [{"HTTPBearer": []}] if auth_enabled else [{"HTTPBearer": []}, {}]
    operation = paths["/api/v1/servers"]["get"]
    assert operation["security"] == security
    for method in ("patch", "delete"):
        operation = paths["/api/v1/servers/{server_id}"][method]
        header = next(p for p in operation["parameters"] if p["name"] == "If-Match")
        assert header["in"] == "header" and header["required"] is True
    submission = paths["/api/v1/simulations"]["post"]
    key = next(p for p in submission["parameters"] if p["name"] == "Idempotency-Key")
    assert key["required"] is False and key["schema"]["maxLength"] == 128


def test_openapi_describes_stream_replay_and_actual_download_media_types() -> None:
    schema = document(auth_enabled=False)
    paths = schema["paths"]
    assert isinstance(paths, dict)
    stream = paths["/api/v1/simulations/{run_id}/stream"]["get"]
    header = next(p for p in stream["parameters"] if p["name"] == "Last-Event-ID")
    assert header["schema"]["default"] == "0"
    assert "text/event-stream" in stream["responses"]["200"]["content"]
    archive = paths["/api/v1/simulations/{run_id}/inputs"]["get"]
    assert "application/zip" in archive["responses"]["200"]["content"]
    artifact = paths["/api/v1/simulations/{run_id}/artifact"]["get"]
    assert "application/x-ndjson" in artifact["responses"]["200"]["content"]
