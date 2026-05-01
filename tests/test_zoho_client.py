from unittest.mock import MagicMock, patch

import pytest
import requests

from sync.rate_limiter import ZohoTrafficGate
from sync.zoho_client import ZohoClient, ZohoError, ZohoRetryableError


class FakeTokens:
    def __init__(self):
        self.value = "TOKEN-1"
        self.invalidate_count = 0
        self.force_count = 0
        self.stale_count = 0

    def get(self, *, stale_token=None, force_refresh=False):
        if stale_token is not None:
            self.stale_count += 1
            if self.value == stale_token:
                self.value = "TOKEN-2"
        elif force_refresh:
            self.force_count += 1
            self.value = "TOKEN-2"
        return self.value

    def invalidate(self):
        self.invalidate_count += 1


def _resp(sc, body=None, headers=None):
    r = MagicMock()
    r.status_code = sc
    r.headers = headers or {}
    r.content = b"{}" if body is not None else b""
    r.json.return_value = body or {}
    r.text = str(body)
    return r


def _client(session, max_attempts=3):
    tokens = FakeTokens()
    limiter = ZohoTrafficGate(0.0, 0.0, max_concurrency=0, rate_per_minute=0)
    sleeps: list[float] = []
    c = ZohoClient(
        "owner", "carton", "https://api.example.com/api/v2",
        tokens, limiter, session=session, max_attempts=max_attempts,
        sleep_func=sleeps.append,
    )
    return c, tokens, sleeps


def test_add_record_success_201_returns_id():
    session = MagicMock()
    session.request.return_value = _resp(201, {"data": {"ID": "999"}})
    c, _, _ = _client(session)
    assert c.add_record("Items_Data", {"Item_Code": "X"},
                        priority=ZohoTrafficGate.REALTIME) == "999"


def test_add_record_success_200_also_accepted():
    session = MagicMock()
    session.request.return_value = _resp(200, {"data": [{"ID": "abc"}]})
    c, _, _ = _client(session)
    assert c.add_record("Items_Data", {}, priority=0) == "abc"


def test_validation_400_raises_zoho_error():
    session = MagicMock()
    session.request.return_value = _resp(400, {"error": "bad"})
    c, _, _ = _client(session)
    with pytest.raises(ZohoError) as exc:
        c.add_record("Items_Data", {}, priority=0)
    assert exc.value.status_code == 400


def test_401_triggers_stale_token_refresh_and_retries():
    session = MagicMock()
    session.request.side_effect = [
        _resp(401, {"error": "auth"}),
        _resp(201, {"data": {"ID": "ok"}}),
    ]
    c, tokens, _ = _client(session)
    assert c.add_record("Items_Data", {}, priority=0) == "ok"
    assert tokens.stale_count == 1


def test_429_raises_retryable_with_retry_after():
    session = MagicMock()
    session.request.return_value = _resp(429, {}, headers={"Retry-After": "7"})
    c, _, sleeps = _client(session)
    with pytest.raises(ZohoRetryableError) as exc:
        c.add_record("Items_Data", {}, priority=0)
    assert exc.value.retry_after == 7.0
    assert sleeps == []


def test_creator_2955_code_raises_retryable():
    session = MagicMock()
    session.request.return_value = _resp(200, {"code": 2955, "message": "throttle"})
    c, _, _ = _client(session)
    with pytest.raises(ZohoRetryableError):
        c.add_record("Items_Data", {}, priority=0)


def test_creator_4000_code_is_daily_quota_retryable():
    session = MagicMock()
    session.request.return_value = _resp(200, {"code": 4000, "message": "quota"})
    c, _, _ = _client(session)
    with pytest.raises(ZohoRetryableError) as exc:
        c.add_record("Items_Data", {}, priority=0)
    assert exc.value.retry_after == 3600.0


@patch("sync.connectivity.ping_internet", return_value=True)
def test_5xx_retries_then_succeeds(_mock_ping):
    session = MagicMock()
    session.request.side_effect = [
        _resp(503),
        _resp(503),
        _resp(201, {"data": {"ID": "1"}}),
    ]
    c, _, _ = _client(session)
    assert c.add_record("Items_Data", {}, priority=0) == "1"


@patch("sync.connectivity.ping_internet", return_value=True)
def test_network_error_retries_then_succeeds(_mock_ping):
    """Network errors now retry infinitely; here we simulate recovery."""
    session = MagicMock()
    session.request.side_effect = [
        requests.ConnectionError("nope"),
        requests.ConnectionError("nope"),
        _resp(201, {"data": {"ID": "recovered"}}),
    ]
    c, _, _ = _client(session)
    assert c.add_record("Items_Data", {}, priority=0) == "recovered"


def test_update_record_uses_patch_to_report_url():
    session = MagicMock()
    session.request.return_value = _resp(200, {"data": {}})
    c, _, _ = _client(session)
    c.update_record("Items_Report", "ID-7", {"x": 1}, priority=0)
    method, url = session.request.call_args.args
    assert method == "PATCH"
    assert url.endswith("/report/Items_Report/ID-7")


def test_delete_record_uses_delete_to_report_url():
    session = MagicMock()
    session.request.return_value = _resp(200, {"data": {}})
    c, _, _ = _client(session)
    c.delete_record("Items_Report", "ID-7", priority=0)
    method, url = session.request.call_args.args
    assert method == "DELETE"
    assert url.endswith("/report/Items_Report/ID-7")


def test_delete_404_treated_as_idempotent_success():
    session = MagicMock()
    session.request.return_value = _resp(404, {"error": "missing"})
    c, _, _ = _client(session)
    c.delete_record("Items_Report", "ID-404", priority=0)


def test_find_record_id_by_external_key_uses_report_criteria_and_returns_id():
    session = MagicMock()
    session.request.return_value = _resp(200, {"data": [{"ID": "RID-1", "External_Key": "K"}]})
    c, _, _ = _client(session)
    rid = c.find_record_id_by_external_key("Items_Report", "External_Key", "K", priority=0)
    assert rid == "RID-1"
    method, url = session.request.call_args.args
    assert method == "GET"
    assert "criteria=(External_Key == \"K\")" in url
