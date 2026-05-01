"""Worker R: drains SYNC_EVENTS into Zoho. Realtime priority on the limiter."""
from __future__ import annotations

import logging
import threading
import time
from typing import Any

from . import zoho_map
from .rate_limiter import ZohoTrafficGate
from .transform import branches_payload, items_payload
from .zoho_client import ZohoClient, ZohoError, ZohoRetryableError

log = logging.getLogger(__name__)

# Important recovery note for the SK1MF-only insert bug:
#
# The event queue gives us the exact Oracle item key in
# (k_cp, k_yr, k_code).  The SK1MF query must always use all three parts,
# especially s.SK1M1 = :code.  If that item-code predicate is removed, an
# event for ITEM008 can accidentally fetch another item from the same company
# and year, then store that other Zoho record under ITEM008 in ZOHO_MAP.
#
# PS33MF is optional for realtime item creation.  A row can exist in SK1MF
# before any matching PS33MF filter row exists.  To handle that safely, we
# read the SK1MF row by its exact event key first, then read PS33MF in a
# second optional query.  This avoids an inner join hiding valid SK1MF rows
# while still preserving the exact item identity from SYNC_EVENTS.
ITEMS_SELECT = """
SELECT s.SK1MCP, s.SK1MYR, s.SK1M1, s.SK1M2, s.SK1M3, s.SK1M9,
       s.SK1M11, s.SK1M12, s.SK1M13, s.SK1M14, s.SK1M16, s.SK1M17,
       s.SK1M18, s.SK1M19, s.SK1M20, s.SK1M21, s.SK1M22, s.SK1M24,
       s.SK1M29, s.SK1M31, s.SK1M32, s.SK1M33, s.SK1M34, s.SK1M36,
       s.SK1M37, s.SK1M39, s.SK1M40, s.SK1M41, s.SK1M261
FROM   sk1mf s
WHERE  s.SK1MCP = :cp AND s.SK1MYR = :yr AND s.SK1M1 = :code
"""

PS33_SELECT = """
SELECT PS33M2, PS33M4
FROM   ps33mf
WHERE  PS33MCP = :cp AND PS33MYR = :yr AND PS33M1 = :code
FETCH FIRST 1 ROWS ONLY
"""

BRANCHES_SELECT = """
SELECT GRBRCP, GRBRYR, BN, GRBR2, GRBR3
FROM   GRBRF
WHERE  GRBRCP = :cp AND GRBRYR = :yr AND BN = :bn
"""


def _row_to_dict(cursor, row) -> dict[str, Any]:
    cols = [c[0].upper() for c in cursor.description]
    return dict(zip(cols, row))


class RealtimeWorker:
    def __init__(self, pool, zoho: ZohoClient, form_items: str, form_branches: str,
                 max_attempts: int = 5, batch_size: int = 10, idle_sleep: float = 1.0,
                 stop_event: threading.Event | None = None):
        self._pool = pool
        self._zoho = zoho
        self._form_items = form_items
        self._form_branches = form_branches
        self._max_attempts = max_attempts
        self._batch_size = batch_size
        self._idle_sleep = idle_sleep
        self._stop = stop_event or threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        from .connectivity import escalating_backoff
        log.info("realtime worker started")
        oracle_fail_count = 0
        while not self._stop.is_set():
            try:
                processed = self.run_once()
                oracle_fail_count = 0  # reset on success
                if processed == 0:
                    if self._stop.wait(self._idle_sleep):
                        break
            except Exception as e:
                # Oracle connection or unexpected error — retry forever
                oracle_fail_count += 1
                delay = escalating_backoff(oracle_fail_count)
                log.warning(
                    "realtime worker oracle/unexpected error (attempt %d), "
                    "retrying in %.0f s: %s", oracle_fail_count, delay, e,
                )
                if self._stop.wait(delay):
                    break
        log.info("realtime worker stopped")

    def run_once(self) -> int:
        with self._pool.connection() as conn:
            cursor = conn.cursor()
            events = self._pick_events(cursor)
            if not events:
                conn.commit()
                return 0
            for ev in events:
                self._handle_event(conn, ev)
            conn.commit()
            return len(events)

    def _pick_events(self, cursor) -> list[dict[str, Any]]:
        """Claim ready events without making workers block on the same rows.

        Multiple realtime threads can poll at the same time.  A plain SELECT
        followed by UPDATE makes them all see the same first events, then the
        losing threads wait on row locks.  ``FOR UPDATE SKIP LOCKED`` makes
        Oracle skip rows already claimed by another worker, so each thread can
        immediately move on to a different ready event.
        """
        try:
            cursor.arraysize = self._batch_size
            cursor.prefetchrows = self._batch_size
        except Exception:
            pass
        cursor.execute(
            """
            SELECT id, source_table, op, k_cp, k_yr, k_code, k_bn, attempts
            FROM SYNC_EVENTS
            WHERE status = 'NEW'
              AND (next_attempt_at IS NULL OR next_attempt_at <= SYSTIMESTAMP)
            ORDER BY id
            FOR UPDATE SKIP LOCKED
            """,
        )
        cols = [c[0].lower() for c in cursor.description]
        candidates = [dict(zip(cols, r)) for r in cursor.fetchmany(self._batch_size)]
        if not candidates:
            return []
        claimed: list[dict[str, Any]] = []
        for ev in candidates:
            cursor.execute(
                "UPDATE SYNC_EVENTS SET status='INFLIGHT', picked_at=SYSTIMESTAMP, "
                "next_attempt_at=NULL "
                "WHERE id=:id AND status='NEW'",
                id=ev["id"],
            )
            if cursor.rowcount == 1:
                claimed.append(ev)
        return claimed

    def _handle_event(self, conn, ev: dict[str, Any]) -> None:
        cursor = conn.cursor()
        event_id = ev["id"]
        try:
            if ev["source_table"] == "GRBRF":
                self._sync_branches(cursor, ev)
            else:  # SK1MF or PS33MF -> sync the corresponding Items_Data row
                self._sync_items(cursor, ev)
            cursor.execute(
                "UPDATE SYNC_EVENTS SET status='DONE', finished_at=SYSTIMESTAMP, "
                "last_error=NULL, next_attempt_at=NULL WHERE id=:id",
                id=event_id,
            )
        except ZohoRetryableError as e:
            delay = e.retry_after if e.retry_after is not None else 60.0
            cursor.execute(
                "UPDATE SYNC_EVENTS SET status='NEW', last_error=:err, "
                "next_attempt_at=SYSTIMESTAMP + NUMTODSINTERVAL(:delay, 'SECOND') "
                "WHERE id=:id",
                err=str(e)[:3900], delay=max(float(delay), 0.0), id=event_id,
            )
        except Exception as e:
            log.exception("event %s failed", event_id)
            new_attempts = ev["attempts"] + 1
            new_status = "DEAD" if new_attempts >= self._max_attempts else "NEW"
            cursor.execute(
                "UPDATE SYNC_EVENTS SET status=:st, attempts=:a, last_error=:err, "
                "next_attempt_at=NULL "
                "WHERE id=:id",
                st=new_status, a=new_attempts, err=str(e)[:3900], id=event_id,
            )


    def _delete_with_reconciliation(self, cursor, source_table: str, report: str,
                                    key_params: dict[str, Any], external_key: str) -> None:
        existing_id = zoho_map.lookup(cursor, source_table, **key_params)
        if existing_id:
            try:
                self._zoho.delete_record(report, existing_id, priority=ZohoTrafficGate.REALTIME)
            except ZohoError as e:
                if e.status_code != 404:
                    raise
            zoho_map.delete(cursor, source_table, **key_params)
            return

        found_id = self._zoho.find_record_id_by_external_key(
            report, external_key, self._external_value(source_table, key_params),
            priority=ZohoTrafficGate.REALTIME,
        )
        if found_id:
            self._zoho.delete_record(report, found_id, priority=ZohoTrafficGate.REALTIME)
        zoho_map.delete(cursor, source_table, **key_params)

    @staticmethod
    def _external_value(source_table: str, key_params: dict[str, Any]) -> str:
        if source_table == "ITEMS":
            return f"{key_params['k_cp']}-{key_params['k_yr']}-{key_params['k_code']}"
        return f"{key_params['k_cp']}-{key_params['k_yr']}-{key_params['k_bn']}"

    # --- per-form handlers
    def _sync_items(self, cursor, ev: dict[str, Any]) -> None:
        cp, yr, code = ev["k_cp"], ev["k_yr"], ev["k_code"]
        cursor.execute(ITEMS_SELECT, cp=cp, yr=yr, code=code)
        row = cursor.fetchone()
        row_dict = _row_to_dict(cursor, row) if row is not None else None
        existing_id = zoho_map.lookup(cursor, "ITEMS",
                                      k_cp=cp, k_yr=yr, k_code=code)

        if ev["op"] == "D" and ev["source_table"] == "SK1MF":
            self._delete_with_reconciliation(
                cursor, "ITEMS", self._form_items,
                {"k_cp": cp, "k_yr": yr, "k_code": code}, "External_Key"
            )
            return

        if row_dict is None:
            # No SK1MF row means the source item is gone or the event is
            # orphaned.  In that case we mirror deletion only if Zoho already
            # has a mapped record for this exact key.
            self._delete_with_reconciliation(
                cursor, "ITEMS", self._form_items,
                {"k_cp": cp, "k_yr": yr, "k_code": code}, "External_Key"
            )
            return

        # PS33MF only supplies optional filter fields for Items_Data.  If it is
        # missing, we still send the SK1MF item to Zoho and leave those filter
        # fields as NULL/None.  A PS33MF delete therefore updates the item with
        # blank filter fields instead of deleting the whole SK1MF item.
        cursor.execute(PS33_SELECT, cp=cp, yr=yr, code=code)
        ps33_row = cursor.fetchone()
        if ps33_row is None:
            row_dict["PS33M2"] = None
            row_dict["PS33M4"] = None
        else:
            row_dict.update(_row_to_dict(cursor, ps33_row))

        payload = items_payload(row_dict)

        if existing_id:
            try:
                self._zoho.update_record(self._form_items, existing_id, payload,
                                         priority=ZohoTrafficGate.REALTIME)
                return
            except ZohoError as e:
                if e.status_code != 404:
                    raise
                zoho_map.delete(cursor, "ITEMS", k_cp=cp, k_yr=yr, k_code=code)

        new_id = self._zoho.add_record(self._form_items, payload,
                                       priority=ZohoTrafficGate.REALTIME)
        zoho_map.upsert(cursor, "ITEMS", new_id,
                        k_cp=cp, k_yr=yr, k_code=code)

    def _sync_branches(self, cursor, ev: dict[str, Any]) -> None:
        cp, yr, bn = ev["k_cp"], ev["k_yr"], ev["k_bn"]
        existing_id = zoho_map.lookup(cursor, "GRBRF",
                                      k_cp=cp, k_yr=yr, k_bn=bn)
        if ev["op"] == "D":
            self._delete_with_reconciliation(
                cursor, "GRBRF", self._form_branches,
                {"k_cp": cp, "k_yr": yr, "k_bn": bn}, "External_Key"
            )
            return

        cursor.execute(BRANCHES_SELECT, cp=cp, yr=yr, bn=bn)
        row = cursor.fetchone()
        if row is None:
            self._delete_with_reconciliation(
                cursor, "GRBRF", self._form_branches,
                {"k_cp": cp, "k_yr": yr, "k_bn": bn}, "External_Key"
            )
            return

        payload = branches_payload(_row_to_dict(cursor, row))
        if existing_id:
            try:
                self._zoho.update_record(self._form_branches, existing_id, payload,
                                         priority=ZohoTrafficGate.REALTIME)
                return
            except ZohoError as e:
                if e.status_code != 404:
                    raise
                zoho_map.delete(cursor, "GRBRF", k_cp=cp, k_yr=yr, k_bn=bn)

        new_id = self._zoho.add_record(self._form_branches, payload,
                                       priority=ZohoTrafficGate.REALTIME)
        zoho_map.upsert(cursor, "GRBRF", new_id,
                        k_cp=cp, k_yr=yr, k_bn=bn)
