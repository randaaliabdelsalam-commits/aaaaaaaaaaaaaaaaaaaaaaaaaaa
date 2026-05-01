from unittest.mock import patch

import requests

from sync.connectivity import check_connectivity, wait_for_internet


def test_check_connectivity_falls_back_in_order():
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        if "accounts.zoho.com" in url:
            raise requests.ConnectionError("down")
        if "creator.zoho.com" in url:
            raise requests.Timeout("slow")
        return object()

    with patch("sync.connectivity.requests.get", side_effect=fake_get):
        ok, failure = check_connectivity()

    assert ok is True
    assert failure is None
    assert calls == [
        "https://accounts.zoho.com",
        "https://creator.zoho.com",
        "https://google.com",
    ]


def test_check_connectivity_partial_reachability_custom_endpoints():
    def fake_get(url, **kwargs):
        if url == "https://one.example":
            raise requests.ConnectionError("one down")
        return object()

    with patch("sync.connectivity.requests.get", side_effect=fake_get):
        ok, failure = check_connectivity(endpoints=("https://one.example", "https://two.example"))

    assert ok is True
    assert failure is None


def test_wait_for_internet_honors_stop_event_with_endpoint_failure_logging():
    class StopEvent:
        def wait(self, _delay):
            return True

    with patch("sync.connectivity.check_connectivity", return_value=(False, ("https://accounts.zoho.com", "boom"))):
        try:
            wait_for_internet(stop_event=StopEvent(), label="test")
        except InterruptedError as exc:
            assert "test: shutdown during connectivity wait" in str(exc)
        else:
            raise AssertionError("Expected InterruptedError")
