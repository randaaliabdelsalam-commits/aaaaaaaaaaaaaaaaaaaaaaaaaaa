from unittest.mock import MagicMock

import pytest
import requests

from sync.token_manager import TokenError, TokenManager


def _resp(status_code=200, body=None):
    r = MagicMock()
    r.status_code = status_code
    r.json.return_value = body or {}
    r.text = str(body)
    return r


def test_get_fetches_once_and_caches():
    session = MagicMock()
    session.post.return_value = _resp(200, {"access_token": "T1"})
    tm = TokenManager("cid", "csec", "rt", "https://example/oauth", session=session)

    assert tm.get() == "T1"
    assert tm.get() == "T1"
    assert session.post.call_count == 1


def test_force_refresh_calls_again():
    session = MagicMock()
    session.post.side_effect = [
        _resp(200, {"access_token": "A"}),
        _resp(200, {"access_token": "B"}),
    ]
    tm = TokenManager("cid", "csec", "rt", "https://example/oauth", session=session)
    assert tm.get() == "A"
    assert tm.get(force_refresh=True) == "B"
    assert session.post.call_count == 2


def test_invalidate_then_get_refreshes():
    session = MagicMock()
    session.post.side_effect = [
        _resp(200, {"access_token": "A"}),
        _resp(200, {"access_token": "B"}),
    ]
    tm = TokenManager("cid", "csec", "rt", "https://example/oauth", session=session)
    tm.get()
    tm.invalidate()
    assert tm.get() == "B"


def test_token_endpoint_failure_raises_token_error():
    session = MagicMock()
    session.post.return_value = _resp(401, {"error": "invalid_code"})
    tm = TokenManager("cid", "csec", "rt", "https://example/oauth", session=session)
    with pytest.raises(TokenError):
        tm.get()


def test_missing_access_token_field_raises():
    session = MagicMock()
    session.post.return_value = _resp(200, {"hello": "world"})
    tm = TokenManager("cid", "csec", "rt", "https://example/oauth", session=session)
    with pytest.raises(TokenError):
        tm.get()


def test_network_error_surfaces_as_token_error():
    session = MagicMock()
    session.post.side_effect = requests.ConnectionError("boom")
    tm = TokenManager("cid", "csec", "rt", "https://example/oauth", session=session)
    with pytest.raises(TokenError):
        tm.get()


def test_proactive_refresh_after_50_minutes():
    session = MagicMock()
    session.post.side_effect = [
        _resp(200, {"access_token": "old"}),
        _resp(200, {"access_token": "new"}),
    ]
    clock = [0.0]
    tm = TokenManager("cid", "csec", "rt", "https://example/oauth",
                      session=session, time_func=lambda: clock[0])
    assert tm.get() == "old"
    clock[0] = 50 * 60 + 1
    assert tm.get() == "new"
