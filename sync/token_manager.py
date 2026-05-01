"""Thread-safe OAuth2 token manager with refresh support."""

from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path

import requests

log = logging.getLogger(__name__)


class TokenError(RuntimeError):
    pass


class TokenManager:
    REFRESH_MARGIN = 300  # refresh 5 minutes before expiry

    def __init__(
        self,
        env_path: str | Path | None = None,
        session: requests.Session | None = None,
        time_func=time.monotonic,
        stop_event: threading.Event | None = None,
    ):
        self._env_path = Path(env_path) if env_path else None
        self._session = session or requests.Session()
        self._time = time_func
        self._lock = threading.Lock()

        self._token: str | None = None
        self._expires_at: float = 0.0

        self._load_from_env()

    # ---------------------------------------------------------

    def get(self, *, force_refresh: bool = False) -> str:
        with self._lock:
            now = self._time()

            if (
                not force_refresh
                and self._token
                and now < self._expires_at - self.REFRESH_MARGIN
            ):
                return self._token

            return self._refresh()

    # ---------------------------------------------------------

    def _refresh(self) -> str:
        log.info("refreshing Zoho access token...")

        url = "https://accounts.zoho.com/oauth/v2/token"

        params = {
            "refresh_token": os.environ.get("refresh_token"),
            "client_id": os.environ.get("client_id"),
            "client_secret": os.environ.get("client_secret"),
            "grant_type": "refresh_token",
        }

        resp = self._session.post(url, params=params, timeout=30)
        data = resp.json()

        if "access_token" not in data:
            raise TokenError(f"Token refresh failed: {data}")

        token = data["access_token"]
        expires_in = int(data.get("expires_in", 3600))

        self._token = token
        self._expires_at = self._time() + expires_in

        self._save_to_env(token, expires_in)

        log.info("Zoho access token refreshed")

        return token

    # ---------------------------------------------------------

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

    # ---------------------------------------------------------

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

        self._env_path.write_text("\n".join(new_lines))