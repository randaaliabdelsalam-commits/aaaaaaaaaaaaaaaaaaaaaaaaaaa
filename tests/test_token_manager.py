from __future__ import annotations

import threading
from unittest.mock import MagicMock

import pytest

from sync.token_manager import TokenError, TokenManager


def _resp(body=None):
    r = MagicMock()
    r.json.return_value = body or {}
    return r


def _set_creds(monkeypatch, client_id="cid", client_secret="csec", refresh_token="rt"):
    monkeypatch.setenv("client_id", client_id)
    monkeypatch.setenv("client_secret", client_secret)
    monkeypatch.setenv("refresh_token", refresh_token)


def test_missing_env_vars_raise(monkeypatch):
    monkeypatch.delenv("client_id", raising=False)
    monkeypatch.delenv("client_secret", raising=False)
    monkeypatch.delenv("refresh_token", raising=False)

    tm = TokenManager(session=MagicMock())
    with pytest.raises(TokenError, match="Missing required OAuth env vars"):
        tm.get(force_refresh=True)


def test_invalid_client_transitions_from_3x10s_to_hourly(monkeypatch):
    _set_creds(monkeypatch)
    session = MagicMock()
    session.post.return_value = _resp({"error": "invalid_client"})

    stop_event = threading.Event()
    wait_calls = []

    def fake_wait(seconds):
        wait_calls.append(seconds)
        if len(wait_calls) >= 4:
            stop_event.set()
            return True
        return False

    stop_event.wait = fake_wait

    tm = TokenManager(session=session, stop_event=stop_event)

    with pytest.raises(TokenError, match="stopped due to shutdown"):
        tm.get(force_refresh=True)

    assert wait_calls[:3] == [10, 10, 10]
    assert wait_calls[3] == 3600


def test_hash_changed_recovery_path(monkeypatch, caplog):
    _set_creds(monkeypatch, client_id="old")
    session = MagicMock()
    session.post.side_effect = [
        _resp({"error": "invalid_client"}),
        _resp({"error": "invalid_client"}),
        _resp({"error": "invalid_client"}),
        _resp({"error": "invalid_client"}),
        _resp({"access_token": "ok"}),
    ]
    stop_event = threading.Event()

    calls = {"n": 0}

    def fake_wait(seconds):
        calls["n"] += 1
        if seconds == 3600:
            monkeypatch.setenv("client_id", "new")
        return False

    stop_event.wait = fake_wait

    tm = TokenManager(session=session, stop_event=stop_event)
    token = tm.get(force_refresh=True)
    assert token == "ok"
    assert "credentials changed, auto-retrying" in caplog.text


def test_unchanged_hash_alert_path(monkeypatch):
    _set_creds(monkeypatch)
    session = MagicMock()
    session.post.return_value = _resp({"error": "invalid_client"})
    stop_event = threading.Event()

    waits = {"n": 0}

    def fake_wait(seconds):
        waits["n"] += 1
        if waits["n"] >= 4:
            stop_event.set()
            return True
        return False

    stop_event.wait = fake_wait

    tm = TokenManager(session=session, stop_event=stop_event)
    with pytest.raises(TokenError):
        tm.get(force_refresh=True)

    assert tm._misconfigured is True
