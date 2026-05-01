"""Shared internet-connectivity check and escalating-backoff helpers.

Used by token_manager, zoho_client, oracle_pool, and workers to implement
the production infinite-retry policy:

    1 s → 3 s → 10 s → 15 s → 1 min → +5 min each step (forever)
"""
from __future__ import annotations

import logging
import os
import threading
import time

import requests

log = logging.getLogger(__name__)

# The fixed portion of the backoff ladder before it switches to
# "+5 minutes each step".
_FIXED_STEPS: tuple[float, ...] = (1.0, 3.0, 10.0, 15.0, 60.0)


def escalating_backoff(attempt: int) -> float:
    """Return the wait time (seconds) for *attempt* (1-based).

    Steps 1-5 follow the fixed ladder.
    From step 6 onward: 1 min + 5 min × (step - 5).
    """
    if attempt <= len(_FIXED_STEPS):
        return _FIXED_STEPS[attempt - 1]
    extra_steps = attempt - len(_FIXED_STEPS)
    return 60.0 + 300.0 * extra_steps  # 60s base + 5 min per extra step


def ping_internet(timeout: float = 5.0) -> bool:
    """Backward-compatible internet check wrapper."""
    ok, _ = check_connectivity(timeout=timeout)
    return ok


def connectivity_endpoints() -> tuple[str, ...]:
    """Return ordered health-check endpoints, configurable via env."""
    value = os.environ.get("CONNECTIVITY_CHECK_URLS", "").strip()
    if value:
        parsed = tuple(item.strip() for item in value.split(",") if item.strip())
        if parsed:
            return parsed
    return (
        "https://accounts.zoho.com",
        "https://creator.zoho.com",
        "https://google.com",
    )


def check_connectivity(
    timeout: float = 5.0,
    endpoints: tuple[str, ...] | None = None,
) -> tuple[bool, tuple[str, str] | None]:
    """Check connectivity using ordered endpoint fallback.

    Returns (is_online, last_failure) where last_failure is `(endpoint, reason)`.
    """
    checks = endpoints or connectivity_endpoints()
    last_failure: tuple[str, str] | None = None
    for endpoint in checks:
        try:
            requests.get(endpoint, timeout=timeout, allow_redirects=True)
            return True, None
        except Exception as exc:  # noqa: BLE001 - broad by design for network checks
            last_failure = (endpoint, str(exc))
    return False, last_failure


def wait_for_internet(
    stop_event: threading.Event | None = None,
    label: str = "internet",
) -> None:
    """Block until :func:`ping_internet` succeeds.

    Uses the escalating backoff schedule (infinite — no max).
    Respects *stop_event* so the service can still shut down cleanly.
    """
    attempt = 0
    while True:
        attempt += 1
        ok, failure = check_connectivity()
        if ok:
            if attempt > 1:
                log.info("%s: connectivity restored after %d attempts", label, attempt)
            return
        delay = escalating_backoff(attempt)
        if failure is None:
            log.warning("%s: no connectivity (attempt %d), retrying in %.0f s …", label, attempt, delay)
        else:
            endpoint, reason = failure
            log.warning(
                "%s: no connectivity via %s (attempt %d): %s; retrying in %.0f s …",
                label,
                endpoint,
                attempt,
                reason,
                delay,
            )
        if stop_event is not None:
            if stop_event.wait(delay):
                raise InterruptedError(f"{label}: shutdown during connectivity wait")
        else:
            time.sleep(delay)
