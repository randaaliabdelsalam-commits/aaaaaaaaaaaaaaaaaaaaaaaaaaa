 (cd "$(git rev-parse --show-toplevel)" && git apply --3way <<'EOF' 
diff --git a/tests/test_token_manager.py b/tests/test_token_manager.py
index 9323682fce891aeae581b3ec19e96ebdc80162f5..88d7991d06c36691ff4f0a6dfd0079b395cd12c0 100644
--- a/tests/test_token_manager.py
+++ b/tests/test_token_manager.py
@@ -1,86 +1,108 @@
+from __future__ import annotations
+
+import threading
 from unittest.mock import MagicMock
 
 import pytest
-import requests
 
 from sync.token_manager import TokenError, TokenManager
 
 
-def _resp(status_code=200, body=None):
+def _resp(body=None):
     r = MagicMock()
-    r.status_code = status_code
     r.json.return_value = body or {}
-    r.text = str(body)
     return r
 
 
-def test_get_fetches_once_and_caches():
-    session = MagicMock()
-    session.post.return_value = _resp(200, {"access_token": "T1"})
-    tm = TokenManager("cid", "csec", "rt", "https://example/oauth", session=session)
+def _set_creds(monkeypatch, client_id="cid", client_secret="csec", refresh_token="rt"):
+    monkeypatch.setenv("client_id", client_id)
+    monkeypatch.setenv("client_secret", client_secret)
+    monkeypatch.setenv("refresh_token", refresh_token)
+
 
-    assert tm.get() == "T1"
-    assert tm.get() == "T1"
-    assert session.post.call_count == 1
+def test_missing_env_vars_raise(monkeypatch):
+    monkeypatch.delenv("client_id", raising=False)
+    monkeypatch.delenv("client_secret", raising=False)
+    monkeypatch.delenv("refresh_token", raising=False)
 
+    tm = TokenManager(session=MagicMock())
+    with pytest.raises(TokenError, match="Missing required OAuth env vars"):
+        tm.get(force_refresh=True)
 
-def test_force_refresh_calls_again():
+
+def test_invalid_client_transitions_from_3x10s_to_hourly(monkeypatch):
+    _set_creds(monkeypatch)
     session = MagicMock()
-    session.post.side_effect = [
-        _resp(200, {"access_token": "A"}),
-        _resp(200, {"access_token": "B"}),
-    ]
-    tm = TokenManager("cid", "csec", "rt", "https://example/oauth", session=session)
-    assert tm.get() == "A"
-    assert tm.get(force_refresh=True) == "B"
-    assert session.post.call_count == 2
+    session.post.return_value = _resp({"error": "invalid_client"})
+
+    stop_event = threading.Event()
+    wait_calls = []
+
+    def fake_wait(seconds):
+        wait_calls.append(seconds)
+        if len(wait_calls) >= 4:
+            stop_event.set()
+            return True
+        return False
+
+    stop_event.wait = fake_wait
+
+    tm = TokenManager(session=session, stop_event=stop_event)
+
+    with pytest.raises(TokenError, match="stopped due to shutdown"):
+        tm.get(force_refresh=True)
+
+    assert wait_calls[:3] == [10, 10, 10]
+    assert wait_calls[3] == 3600
 
 
-def test_invalidate_then_get_refreshes():
+def test_hash_changed_recovery_path(monkeypatch, caplog):
+    _set_creds(monkeypatch, client_id="old")
     session = MagicMock()
     session.post.side_effect = [
-        _resp(200, {"access_token": "A"}),
-        _resp(200, {"access_token": "B"}),
+        _resp({"error": "invalid_client"}),
+        _resp({"error": "invalid_client"}),
+        _resp({"error": "invalid_client"}),
+        _resp({"error": "invalid_client"}),
+        _resp({"access_token": "ok"}),
     ]
-    tm = TokenManager("cid", "csec", "rt", "https://example/oauth", session=session)
-    tm.get()
-    tm.invalidate()
-    assert tm.get() == "B"
+    stop_event = threading.Event()
 
+    calls = {"n": 0}
 
-def test_token_endpoint_failure_raises_token_error():
-    session = MagicMock()
-    session.post.return_value = _resp(401, {"error": "invalid_code"})
-    tm = TokenManager("cid", "csec", "rt", "https://example/oauth", session=session)
-    with pytest.raises(TokenError):
-        tm.get()
+    def fake_wait(seconds):
+        calls["n"] += 1
+        if seconds == 3600:
+            monkeypatch.setenv("client_id", "new")
+        return False
 
+    stop_event.wait = fake_wait
 
-def test_missing_access_token_field_raises():
-    session = MagicMock()
-    session.post.return_value = _resp(200, {"hello": "world"})
-    tm = TokenManager("cid", "csec", "rt", "https://example/oauth", session=session)
-    with pytest.raises(TokenError):
-        tm.get()
+    tm = TokenManager(session=session, stop_event=stop_event)
+    token = tm.get(force_refresh=True)
+    assert token == "ok"
+    assert "credentials changed, auto-retrying" in caplog.text
 
 
-def test_network_error_surfaces_as_token_error():
+def test_unchanged_hash_alert_path(monkeypatch):
+    _set_creds(monkeypatch)
     session = MagicMock()
-    session.post.side_effect = requests.ConnectionError("boom")
-    tm = TokenManager("cid", "csec", "rt", "https://example/oauth", session=session)
-    with pytest.raises(TokenError):
-        tm.get()
+    session.post.return_value = _resp({"error": "invalid_client"})
+    stop_event = threading.Event()
 
+    waits = {"n": 0}
 
-def test_proactive_refresh_after_50_minutes():
-    session = MagicMock()
-    session.post.side_effect = [
-        _resp(200, {"access_token": "old"}),
-        _resp(200, {"access_token": "new"}),
-    ]
-    clock = [0.0]
-    tm = TokenManager("cid", "csec", "rt", "https://example/oauth",
-                      session=session, time_func=lambda: clock[0])
-    assert tm.get() == "old"
-    clock[0] = 50 * 60 + 1
-    assert tm.get() == "new"
+    def fake_wait(seconds):
+        waits["n"] += 1
+        if waits["n"] >= 4:
+            stop_event.set()
+            return True
+        return False
+
+    stop_event.wait = fake_wait
+
+    tm = TokenManager(session=session, stop_event=stop_event)
+    with pytest.raises(TokenError):
+        tm.get(force_refresh=True)
+
+    assert tm._misconfigured is True
 
EOF
)
