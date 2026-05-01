"""Always-on worker that retries DEAD events.

Worker D runs as a single background thread from service startup.  It
continuously polls ``SYNC_EVENTS`` for rows with ``status = 'DEAD'``,
resets them to ``'NEW'`` (with ``attempts = 0``), so that the normal
realtime workers can pick them up again.

It never blocks Worker R or Worker B — completely independent.
"""
from __future__ import annotations

import logging
import threading
import hashlib
import json
from typing import Any

from . import zoho_map
from .realtime_worker import BRANCHES_SELECT, ITEMS_SELECT, PS33_SELECT, _row_to_dict

log = logging.getLogger(__name__)

# How often to poll for DEAD events (seconds).
_POLL_INTERVAL = 30.0


class DeadRetryWorker:
    """Single always-on thread that resurrects DEAD events."""

    def __init__(
        self,
        pool,
        poll_interval: float = _POLL_INTERVAL,
        stop_event: threading.Event | None = None,
    ):
        self._pool = pool
        self._poll_interval = poll_interval
        self._stop = stop_event or threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        log.info("dead-retry worker started (poll every %.0f s)", self._poll_interval)
        while not self._stop.is_set():
            try:
                count = self._resurrect_dead()
                if count > 0:
                    log.info("dead-retry: reset %d DEAD event(s) → NEW", count)
            except Exception:
                log.exception("dead-retry: error during poll cycle")
            # Sleep until next poll (or until stop is requested)
            if self._stop.wait(self._poll_interval):
                break
        log.info("dead-retry worker stopped")

    def _resurrect_dead(self) -> int:
        """Reprocess DEAD events with per-row decisions."""
        with self._pool.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, source_table, k_cp, k_yr, k_code, k_bn, last_source_hash "
                "FROM SYNC_EVENTS WHERE status = 'DEAD'"
            )
            cols = [c[0].lower() for c in cursor.description]
            events = [dict(zip(cols, r)) for r in cursor.fetchall()]
            count = 0
            for ev in events:
                count += self._decide_event(cursor, ev)
            conn.commit()
            return count

    def _decide_event(self, cursor, ev: dict[str, Any]) -> int:
        source_hash = self._current_source_hash(cursor, ev)
        existing_id = self._existing_zoho_map(cursor, ev)
        if source_hash is not None:
            if source_hash != ev.get("last_source_hash"):
                cursor.execute(
                    "UPDATE SYNC_EVENTS SET status='NEW', attempts=0, "
                    "next_attempt_at=NULL, last_error=NULL WHERE id=:id",
                    id=ev["id"],
                )
                return 1
            return 0
        if existing_id:
            cursor.execute(
                "UPDATE SYNC_EVENTS SET status='NEW', op='D', attempts=0, "
                "next_attempt_at=NULL, last_error=NULL WHERE id=:id",
                id=ev["id"],
            )
            return 1
        cursor.execute(
            "UPDATE SYNC_EVENTS SET status='DONE', finished_at=SYSTIMESTAMP, "
            "last_error='RESOLVED: source row and zoho map both absent', "
            "next_attempt_at=NULL WHERE id=:id",
            id=ev["id"],
        )
        return 1

    def _existing_zoho_map(self, cursor, ev: dict[str, Any]) -> str | None:
        if ev["source_table"] == "GRBRF":
            return zoho_map.lookup(cursor, "GRBRF", k_cp=ev["k_cp"], k_yr=ev["k_yr"],
                                   k_bn=ev["k_bn"])
        return zoho_map.lookup(cursor, "ITEMS", k_cp=ev["k_cp"], k_yr=ev["k_yr"],
                               k_code=ev["k_code"])

    def _current_source_hash(self, cursor, ev: dict[str, Any]) -> str | None:
        if ev["source_table"] == "GRBRF":
            cursor.execute(BRANCHES_SELECT, cp=ev["k_cp"], yr=ev["k_yr"], bn=ev["k_bn"])
            row = cursor.fetchone()
            if row is None:
                return None
            payload = _row_to_dict(cursor, row)
        else:
            cursor.execute(ITEMS_SELECT, cp=ev["k_cp"], yr=ev["k_yr"], code=ev["k_code"])
            row = cursor.fetchone()
            if row is None:
                return None
            payload = _row_to_dict(cursor, row)
            cursor.execute(PS33_SELECT, cp=ev["k_cp"], yr=ev["k_yr"], code=ev["k_code"])
            ps = cursor.fetchone()
            if ps is None:
                payload["PS33M2"] = None
                payload["PS33M4"] = None
            else:
                payload.update(_row_to_dict(cursor, ps))
        blob = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()
