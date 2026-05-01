"""Thread-safe OAuth2 token manager with refresh support."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from pathlib import Path


from sync.alerts import play_alert_sound

log = logging.getLogger(__name__)


class TokenError(RuntimeError):
    pass


class TokenManager:
    REFRESH_MARGIN = 300  # refresh 5 minutes before expiry
    FAST_RETRY_ATTEMPTS = 3
    FAST_RETRY_SECONDS = 10
    HOURLY_RETRY_SECONDS = 3600

    def __init__(
        self,
        env_path: str | Path | None = None,
        session=None,
        time_func=time.monotonic,
        stop_event: threading.Event | None = None,
        state_path: str | Path | None = None,
    ):
        self._env_path = Path(env_path) if env_path else None
        self._session = session
        self._time = time_func
        self._stop_event = stop_event or threading.Event()
        self._state_path = Path(state_path) if state_path else None
        self._lock = threading.Lock()

        self._token: str | None = None
        self._expires_at: float = 0.0
        self._last_credential_hash: str | None = None
        self._misconfigured = False

        self._load_state()
        self._load_from_env()

    def get(self, *, force_refresh: bool = False) -> str:
        with self._lock:
            now = self._time()

            if (
                not force_refresh
                and self._token
                and now < self._expires_at - self.REFRESH_MARGIN
            ):
                return self._token

            return self._refresh_with_retries()

    def _refresh_with_retries(self) -> str:
        log.info("refreshing Zoho access token...")
        fast_attempt = 0
        in_hourly_mode = False

        while not self._stop_event.is_set():
            creds = self._validated_credentials()
            current_hash = self._credential_fingerprint(*creds)
            if in_hourly_mode and self._last_credential_hash and self._last_credential_hash != current_hash:
                log.warning("credentials changed, auto-retrying")
                self._misconfigured = False

            try:
                token = self._perform_refresh(*creds)
                self._last_credential_hash = current_hash
                self._misconfigured = False
                self._save_state()
                return token
            except TokenError as exc:
                fast_attempt += 1
                if fast_attempt <= self.FAST_RETRY_ATTEMPTS:
                    log.warning(
                        "token refresh failed, retrying shortly",
                        extra={"attempt": fast_attempt, "wait_seconds": self.FAST_RETRY_SECONDS},
                    )
                    if self._stop_event.wait(self.FAST_RETRY_SECONDS):
                        break
                    continue

                if self._last_credential_hash and self._last_credential_hash != current_hash:
                    self._misconfigured = False
                else:
                    self._misconfigured = True
                    play_alert_sound()
                    log.error("token_refresh_misconfiguration", extra={"error": str(exc)})

                self._last_credential_hash = current_hash
                self._save_state()

                in_hourly_mode = True
                if self._stop_event.wait(self.HOURLY_RETRY_SECONDS):
                    break

        raise TokenError("token refresh stopped due to shutdown")

    def _validated_credentials(self) -> tuple[str, str, str]:
        client_id = os.environ.get("client_id")
        client_secret = os.environ.get("client_secret")
        refresh_token = os.environ.get("refresh_token")

        missing = [
            name
            for name, value in (
                ("client_id", client_id),
                ("client_secret", client_secret),
                ("refresh_token", refresh_token),
            )
            if not value
        ]
        if missing:
            raise TokenError(f"Missing required OAuth env vars: {', '.join(missing)}")
        return client_id, client_secret, refresh_token

    def _perform_refresh(self, client_id: str, client_secret: str, refresh_token: str) -> str:
        url = "https://accounts.zoho.com/oauth/v2/token"
        params = {
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "refresh_token",
        }

        if self._session is None:
            raise TokenError("No HTTP session configured for token refresh")

        try:
            resp = self._session.post(url, params=params, timeout=30)
            data = resp.json()
        except Exception as exc:
            raise TokenError(f"Token refresh request failed: {exc}") from exc

        if "access_token" not in data:
            raise TokenError(f"Token refresh failed: {data}")

        token = data["access_token"]
        expires_in = int(data.get("expires_in", 3600))
        self._token = token
        self._expires_at = self._time() + expires_in
        self._save_to_env(token, expires_in)

        log.info("Zoho access token refreshed")
        return token

    def _credential_fingerprint(self, client_id: str, client_secret: str, refresh_token: str) -> str:
        joined = "\x1f".join((client_id, client_secret, refresh_token))
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    def _load_state(self):
        if not self._state_path or not self._state_path.exists():
            return
        try:
            data = json.loads(self._state_path.read_text(encoding="utf-8"))
            self._last_credential_hash = data.get("last_credential_hash")
        except (OSError, json.JSONDecodeError):
            log.exception("failed to load token manager state")

    def _save_state(self):
        if not self._state_path:
            return
        data = {"last_credential_hash": self._last_credential_hash}
        self._state_path.write_text(json.dumps(data), encoding="utf-8")

    def _load_from_env(self):
        if not self._env_path or not self._env_path.exists():
            return

        text = self._env_path.read_text(encoding="utf-8")

        for line in text.splitlines():
            if line.startswith("access_token="):
                self._token = line.split("=", 1)[1].strip()

            if line.startswith("expires_in="):
                try:
                    self._expires_at = self._time() + int(line.split("=")[1])
                except Exception:
                    pass

        if self._token:
            log.info("loaded saved access token from .env")

    def _save_to_env(self, token: str, expires_in: int):
        if not self._env_path:
            return

        lines = []
        found_token = False
        found_exp = False

        if self._env_path.exists():
            lines = self._env_path.read_text().splitlines()

        new_lines = []

        for line in lines:
            if line.startswith("access_token="):
                new_lines.append(f"access_token={token}")
                found_token = True
            elif line.startswith("expires_in="):
                new_lines.append(f"expires_in={expires_in}")
                found_exp = True
            else:
                new_lines.append(line)

        if not found_token:
            new_lines.append(f"access_token={token}")
        if not found_exp:
            new_lines.append(f"expires_in={expires_in}")

        self._env_path.write_text("\n".join(new_lines), encoding="utf-8")
