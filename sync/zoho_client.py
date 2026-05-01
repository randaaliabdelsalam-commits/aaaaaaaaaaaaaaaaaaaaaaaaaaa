"""Zoho Creator API client with retry, backoff and rate limiting."""
from __future__ import annotations

import json
import logging
import random
import time
from typing import Any, Callable

import requests

from .rate_limiter import ZohoTrafficGate
from .token_manager import TokenManager

log = logging.getLogger(__name__)


class ZohoError(RuntimeError):
    def __init__(self, message: str, status_code: int | None = None, body: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class ZohoRetryableError(ZohoError):
    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        body: Any = None,
        retry_after: float | None = None,
    ):
        super().__init__(message, status_code=status_code, body=body)
        self.retry_after = retry_after


class ZohoClient:
    """Zoho Creator API client guarded by a shared traffic gate."""

    SUCCESS_CODES = (200, 201)
    MAX_ATTEMPTS_DEFAULT = 5

    def __init__(
        self,
        account_owner: str,
        app: str,
        api_base: str,
        token_manager: TokenManager,
        limiter: ZohoTrafficGate,
        session: requests.Session | None = None,
        max_attempts: int = MAX_ATTEMPTS_DEFAULT,
        sleep_func: Callable[[float], None] = time.sleep,
    ):
        self._owner = account_owner
        self._app = app
        self._base = api_base.rstrip("/")
        self._tokens = token_manager
        self._limiter = limiter
        self._session = session or requests.Session()
        self._max_attempts = max_attempts
        self._sleep = sleep_func

    # -- form / report URL helpers
    def _form_url(self, form: str) -> str:
        return f"{self._base}/{self._owner}/{self._app}/form/{form}"

    def _report_record_url(self, report: str, record_id: str) -> str:
        return f"{self._base}/{self._owner}/{self._app}/report/{report}/{record_id}"

    # -- public ops
    def add_record(self, form: str, payload: dict, priority: int) -> str:
        body = {"data": payload}
        resp = self._call("POST", self._form_url(form), body, priority=priority)
        return _extract_record_id(resp)

    def update_record(
        self, report: str, record_id: str, payload: dict, priority: int
    ) -> None:
        body = {"data": payload}
        self._call(
            "PATCH",
            self._report_record_url(report, record_id),
            body,
            priority=priority,
        )

    def delete_record(self, report: str, record_id: str, priority: int) -> None:
        self._call(
            "DELETE",
            self._report_record_url(report, record_id),
            None,
            priority=priority,
        )

    # -- core retry loop (infinite for network / 5xx)
    def _call(
        self, method: str, url: str, body: dict | None, priority: int
    ) -> dict:
        from .connectivity import check_connectivity, wait_for_internet, escalating_backoff

        attempt = 0
        while True:
            attempt += 1
            # Fetch the currently-cached access token.  We keep a reference so
            # we can pass it back as `stale_token` if we get a 401, enabling
            # the compare-and-refresh race-free pattern inside TokenManager.
            token = self._tokens.get()
            headers = {
                "Authorization": f"Zoho-oauthtoken {token}",
                "Content-Type": "application/json",
            }
            try:
                with self._limiter.slot(priority=priority):
                    if body is None:
                        resp = self._session.request(
                            method, url, headers=headers, timeout=30
                        )
                    else:
                        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
                        resp = self._session.request(
                            method, url, headers=headers, data=data, timeout=30
                        )
            except requests.RequestException as e:
                # Network error → ping check, wait for internet, infinite retry
                log.warning(
                    "network error on %s %s (attempt %d): %s", method, url, attempt, e,
                )
                if not check_connectivity()[0]:
                    wait_for_internet(label="zoho-client")
                else:
                    delay = escalating_backoff(attempt)
                    log.info("internet ok but request failed, retrying in %.0f s", delay)
                    self._sleep(delay)
                continue

            sc = resp.status_code
            resp_body = _response_body(resp)
            creator_code = _creator_code(resp_body)
            retry_after = _parse_retry_after(resp.headers.get("Retry-After"))

            if sc in self.SUCCESS_CODES:
                if creator_code == 2955:
                    self._limiter.slow_down(factor=2.0, duration=60.0)
                    raise ZohoRetryableError(
                        f"Zoho {method} {url} throttled by Creator code 2955",
                        status_code=sc,
                        body=resp_body,
                        retry_after=retry_after,
                    )
                if creator_code == 4000:
                    raise ZohoRetryableError(
                        f"Zoho {method} {url} daily API quota reached",
                        status_code=sc,
                        body=resp_body,
                        retry_after=retry_after if retry_after is not None else 3600.0,
                    )
                return resp_body if isinstance(resp_body, dict) else {}

            if sc == 401:
                log.warning("401 Unauthorized → refreshing token and retrying")

                # force refresh token
                self._tokens.get(force_refresh=True)

                # retry request immediately
                continue

            if sc == 429 or creator_code == 2955:
                self._limiter.slow_down(factor=2.0, duration=60.0)
                raise ZohoRetryableError(
                    f"Zoho {method} {url} throttled",
                    status_code=sc,
                    body=resp_body,
                    retry_after=retry_after,
                )
            if creator_code == 4000:
                raise ZohoRetryableError(
                    f"Zoho {method} {url} daily API quota reached",
                    status_code=sc,
                    body=resp_body,
                    retry_after=retry_after if retry_after is not None else 3600.0,
                )
            if 500 <= sc < 600:
                # 5xx → ping check, wait for internet, infinite retry
                log.warning(
                    "Zoho %s %s returned %d (attempt %d), retrying…",
                    method, url, sc, attempt,
                )
                if not check_connectivity()[0]:
                    wait_for_internet(label="zoho-client")
                else:
                    delay = escalating_backoff(attempt)
                    self._sleep(delay)
                continue

            # 400 or other client error → immediate failure (bad data)
            raise ZohoError(
                f"Zoho {method} {url} -> {sc}", status_code=sc, body=resp_body
            )

    def _sleep_backoff(self, attempt: int) -> None:
        self._sleep(self._backoff_seconds(attempt))

    @staticmethod
    def _backoff_seconds(attempt: int) -> float:
        base = min(2 ** (attempt - 1), 30)
        return base + random.uniform(0, base * 0.25)


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _response_body(resp) -> Any:
    if not resp.content:
        return {}
    try:
        return resp.json()
    except ValueError:
        return resp.text


def _creator_code(body: Any) -> int | None:
    if not isinstance(body, dict):
        return None
    code = body.get("code")
    if code is None and isinstance(body.get("data"), dict):
        code = body["data"].get("code")
    if code is None and isinstance(body.get("data"), list) and body["data"]:
        first = body["data"][0]
        if isinstance(first, dict):
            code = first.get("code")
    try:
        return int(code) if code is not None else None
    except (TypeError, ValueError):
        return None


def _extract_record_id(resp: dict) -> str:
    """Pulls the new Zoho record id out of the v2 add-record response."""
    if not isinstance(resp, dict):
        raise ZohoError(f"Zoho add-record returned non-dict: {resp!r}")
    data = resp.get("data")
    if isinstance(data, list) and data:
        candidate = data[0]
    elif isinstance(data, dict):
        candidate = data
    else:
        candidate = resp
    rid = (
        candidate.get("ID")
        or candidate.get("id")
        or candidate.get("zc_record_id")
    )
    if not rid:
        raise ZohoError(
            f"Zoho add-record missing record ID in response: {resp!r}"
        )
    return str(rid)


































# """Zoho Creator API client with retry, backoff and rate limiting."""
# from __future__ import annotations

# import json
# import logging
# import random
# import time
# from typing import Any, Callable

# import requests

# from .rate_limiter import ZohoTrafficGate
# from .token_manager import TokenManager

# log = logging.getLogger(__name__)


# class ZohoError(RuntimeError):
#     def __init__(self, message: str, status_code: int | None = None, body: Any = None):
#         super().__init__(message)
#         self.status_code = status_code
#         self.body = body


# class ZohoRetryableError(ZohoError):
#     def __init__(
#         self,
#         message: str,
#         status_code: int | None = None,
#         body: Any = None,
#         retry_after: float | None = None,
#     ):
#         super().__init__(message, status_code=status_code, body=body)
#         self.retry_after = retry_after


# class ZohoClient:
#     """Zoho Creator API client guarded by a shared traffic gate."""

#     SUCCESS_CODES = (200, 201)
#     MAX_ATTEMPTS_DEFAULT = 5

#     def __init__(self, account_owner: str, app: str, api_base: str,
#                  token_manager: TokenManager, limiter: ZohoTrafficGate,
#                  session: requests.Session | None = None,
#                  max_attempts: int = MAX_ATTEMPTS_DEFAULT,
#                  sleep_func: Callable[[float], None] = time.sleep):
#         self._owner = account_owner
#         self._app = app
#         self._base = api_base.rstrip("/")
#         self._tokens = token_manager
#         self._limiter = limiter
#         self._session = session or requests.Session()
#         self._max_attempts = max_attempts
#         self._sleep = sleep_func

#     # -- form / report URL helpers
#     def _form_url(self, form: str) -> str:
#         return f"{self._base}/{self._owner}/{self._app}/form/{form}"

#     def _report_record_url(self, report: str, record_id: str) -> str:
#         return f"{self._base}/{self._owner}/{self._app}/report/{report}/{record_id}"

#     # -- public ops
#     def add_record(self, form: str, payload: dict, priority: int) -> str:
#         body = {"data": payload}
#         resp = self._call("POST", self._form_url(form), body, priority=priority)
#         return _extract_record_id(resp)

#     def update_record(self, report: str, record_id: str, payload: dict, priority: int) -> None:
#         body = {"data": payload}
#         self._call("PATCH", self._report_record_url(report, record_id), body, priority=priority)

#     def delete_record(self, report: str, record_id: str, priority: int) -> None:
#         self._call("DELETE", self._report_record_url(report, record_id), None, priority=priority)

#     # -- core retry loop
#     def _call(self, method: str, url: str, body: dict | None, priority: int) -> dict:
#         last_exc: Exception | None = None
#         for attempt in range(1, self._max_attempts + 1):
#             token = self._tokens.get()
#             headers = {
#                 "Authorization": f"Zoho-oauthtoken {token}",
#                 "Content-Type": "application/json",
#             }
#             try:
#                 with self._limiter.slot(priority=priority):
#                     if body is None:
#                         resp = self._session.request(method, url, headers=headers, timeout=30)
#                     else:
#                         data = json.dumps(body, ensure_ascii=False).encode("utf-8")
#                         resp = self._session.request(method, url, headers=headers,
#                                                      data=data, timeout=30)
#             except requests.RequestException as e:
#                 last_exc = e
#                 self._sleep_backoff(attempt)
#                 continue

#             sc = resp.status_code
#             resp_body = _response_body(resp)
#             creator_code = _creator_code(resp_body)
#             retry_after = _parse_retry_after(resp.headers.get("Retry-After"))

#             if sc in self.SUCCESS_CODES:
#                 if creator_code == 2955:
#                     self._limiter.slow_down(factor=2.0, duration=60.0)
#                     raise ZohoRetryableError(
#                         f"Zoho {method} {url} throttled by Creator code 2955",
#                         status_code=sc, body=resp_body, retry_after=retry_after,
#                     )
#                 if creator_code == 4000:
#                     raise ZohoRetryableError(
#                         f"Zoho {method} {url} daily API quota reached",
#                         status_code=sc, body=resp_body,
#                         retry_after=retry_after if retry_after is not None else 3600.0,
#                     )
#                 return resp_body if isinstance(resp_body, dict) else {}
#             if sc == 401:
#                 self._tokens.invalidate()
#                 self._tokens.get(force_refresh=True)
#                 continue
#             if sc == 429 or creator_code == 2955:
#                 self._limiter.slow_down(factor=2.0, duration=60.0)
#                 raise ZohoRetryableError(
#                     f"Zoho {method} {url} throttled",
#                     status_code=sc, body=resp_body, retry_after=retry_after,
#                 )
#             if creator_code == 4000:
#                 raise ZohoRetryableError(
#                     f"Zoho {method} {url} daily API quota reached",
#                     status_code=sc, body=resp_body,
#                     retry_after=retry_after if retry_after is not None else 3600.0,
#                 )
#             if 500 <= sc < 600:
#                 self._sleep_backoff(attempt)
#                 continue

#             raise ZohoError(f"Zoho {method} {url} -> {sc}", status_code=sc, body=resp_body)

#         raise ZohoError(
#             f"Zoho {method} {url} exhausted {self._max_attempts} attempts: {last_exc}"
#         )

#     def _sleep_backoff(self, attempt: int) -> None:
#         self._sleep(self._backoff_seconds(attempt))

#     @staticmethod
#     def _backoff_seconds(attempt: int) -> float:
#         base = min(2 ** (attempt - 1), 30)
#         return base + random.uniform(0, base * 0.25)


# def _parse_retry_after(value: str | None) -> float | None:
#     if not value:
#         return None
#     try:
#         return float(value)
#     except ValueError:
#         return None


# def _response_body(resp) -> Any:
#     if not resp.content:
#         return {}
#     try:
#         return resp.json()
#     except ValueError:
#         return resp.text


# def _creator_code(body: Any) -> int | None:
#     if not isinstance(body, dict):
#         return None
#     code = body.get("code")
#     if code is None and isinstance(body.get("data"), dict):
#         code = body["data"].get("code")
#     if code is None and isinstance(body.get("data"), list) and body["data"]:
#         first = body["data"][0]
#         if isinstance(first, dict):
#             code = first.get("code")
#     try:
#         return int(code) if code is not None else None
#     except (TypeError, ValueError):
#         return None


# def _extract_record_id(resp: dict) -> str:
#     """Pulls the new Zoho record id out of the v2 add-record response."""
#     if not isinstance(resp, dict):
#         raise ZohoError(f"Zoho add-record returned non-dict: {resp!r}")
#     data = resp.get("data")
#     if isinstance(data, list) and data:
#         candidate = data[0]
#     elif isinstance(data, dict):
#         candidate = data
#     else:
#         candidate = resp
#     rid = candidate.get("ID") or candidate.get("id") or candidate.get("zc_record_id")
#     if not rid:
#         raise ZohoError(f"Zoho add-record missing record ID in response: {resp!r}")
#     return str(rid)
