"""Worker B: walks existing SK1MF / GRBRF rows and pushes them to Zoho.

Resumable via BACKFILL_CHECKPOINT + dedup via ZOHO_MAP.
Runs through the shared ZohoTrafficGate as the backfill lane.
"""
from __future__ import annotations

import logging
import threading
from typing import Any

from . import zoho_map
from .rate_limiter import ZohoTrafficGate
from .transform import branches_payload, items_payload
from .zoho_client import ZohoClient, ZohoError, ZohoRetryableError

log = logging.getLogger(__name__)

PAGE_SIZE = 200

ITEMS_PAGE_SQL = """
SELECT s.SK1MCP, s.SK1MYR, s.SK1M1, s.SK1M2, s.SK1M3, s.SK1M9,
       s.SK1M11, s.SK1M12, s.SK1M13, s.SK1M14, s.SK1M16, s.SK1M17,
       s.SK1M18, s.SK1M19, s.SK1M20, s.SK1M21, s.SK1M22, s.SK1M24,
       s.SK1M29, s.SK1M31, s.SK1M32, s.SK1M33, s.SK1M34, s.SK1M36,
       s.SK1M37, s.SK1M39, s.SK1M40, s.SK1M41, s.SK1M261,
       p.PS33M2, p.PS33M4
FROM   sk1mf s, ps33mf p
WHERE  ps33mcp = sk1mcp
  AND  ps33myr = sk1myr
  AND  ps33m1  = sk1m1
  AND ( s.SK1MCP > :cp
     OR (s.SK1MCP = :cp AND s.SK1MYR > :yr)
     OR (s.SK1MCP = :cp AND s.SK1MYR = :yr AND s.SK1M1 > :code) )
ORDER BY s.SK1MCP, s.SK1MYR, s.SK1M1
FETCH FIRST :lim ROWS ONLY
"""

BRANCHES_PAGE_SQL = """
SELECT GRBRCP, GRBRYR, BN, GRBR2, GRBR3
FROM   GRBRF
WHERE  ( GRBRCP > :cp
      OR (GRBRCP = :cp AND GRBRYR > :yr)
      OR (GRBRCP = :cp AND GRBRYR = :yr AND BN > :bn) )
ORDER BY GRBRCP, GRBRYR, BN
FETCH FIRST :lim ROWS ONLY
"""


def _row_to_dict(cursor, row) -> dict[str, Any]:
    cols = [c[0].upper() for c in cursor.description]
    return dict(zip(cols, row))


class BackfillWorker:
    def __init__(self, pool, zoho: ZohoClient, form_items: str, form_branches: str,
                 stop_event: threading.Event | None = None,
                 page_size: int = PAGE_SIZE):
        self._pool = pool
        self._zoho = zoho
        self._form_items = form_items
        self._form_branches = form_branches
        self._stop = stop_event or threading.Event()
        self._page_size = page_size

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        from .connectivity import escalating_backoff
        log.info("backfill worker started")
        oracle_fail_count = 0
        while not self._stop.is_set():
            try:
                self._run_items()
                self._run_branches()
                break  # finished successfully
            except ZohoRetryableError:
                raise
            except Exception as e:
                oracle_fail_count += 1
                delay = escalating_backoff(oracle_fail_count)
                log.warning(
                    "backfill worker oracle/unexpected error (attempt %d), "
                    "retrying in %.0f s: %s", oracle_fail_count, delay, e,
                )
                if self._stop.wait(delay):
                    break
        log.info("backfill worker stopped")

    # ----- items
    def _run_items(self) -> None:
        cp, yr, code = self._read_checkpoint("SK1MF",
                                             ("last_cp", "last_yr", "last_code"))
        cp = -1 if cp is None else cp
        yr = -1 if yr is None else yr
        code = " " if code is None else code  # space < any printable char

        while not self._stop.is_set():
            with self._pool.connection() as conn:
                cur = conn.cursor()
                cur.execute(ITEMS_PAGE_SQL, cp=cp, yr=yr, code=code,
                            lim=self._page_size)
                rows = cur.fetchall()
                if not rows:
                    self._mark_finished(conn, "SK1MF")
                    conn.commit()
                    return
                cols = [c[0].upper() for c in cur.description]
                for r in rows:
                    if self._stop.is_set():
                        conn.commit()
                        return
                    rd = dict(zip(cols, r))
                    cp, yr, code = rd["SK1MCP"], rd["SK1MYR"], rd["SK1M1"]
                    self._sync_one(conn, "ITEMS", self._form_items,
                                   items_payload(rd),
                                   k_cp=cp, k_yr=yr, k_code=code)
                    self._update_checkpoint(conn, "SK1MF",
                                            last_cp=cp, last_yr=yr,
                                            last_code=code)
                conn.commit()

    # ----- branches
    def _run_branches(self) -> None:
        cp, yr, bn = self._read_checkpoint("GRBRF",
                                           ("last_cp", "last_yr", "last_bn"))
        cp = -1 if cp is None else cp
        yr = -1 if yr is None else yr
        bn = -1 if bn is None else bn

        while not self._stop.is_set():
            with self._pool.connection() as conn:
                cur = conn.cursor()
                cur.execute(BRANCHES_PAGE_SQL, cp=cp, yr=yr, bn=bn,
                            lim=self._page_size)
                rows = cur.fetchall()
                if not rows:
                    self._mark_finished(conn, "GRBRF")
                    conn.commit()
                    return
                cols = [c[0].upper() for c in cur.description]
                for r in rows:
                    if self._stop.is_set():
                        conn.commit()
                        return
                    rd = dict(zip(cols, r))
                    cp, yr, bn = rd["GRBRCP"], rd["GRBRYR"], rd["BN"]
                    self._sync_one(conn, "GRBRF", self._form_branches,
                                   branches_payload(rd),
                                   k_cp=cp, k_yr=yr, k_bn=bn)
                    self._update_checkpoint(conn, "GRBRF",
                                            last_cp=cp, last_yr=yr,
                                            last_bn=bn)
                conn.commit()

    # ----- shared helpers
    def _sync_one(self, conn, source_key: str, form: str, payload: dict,
                  **keys) -> None:
        cur = conn.cursor()
        existing = zoho_map.lookup(cur, source_key, **keys)
        if existing is not None:
            self._zoho.update_record(form, existing, payload,
                                     priority=ZohoTrafficGate.BACKFILL)
            return
        if not zoho_map.claim(cur, source_key, **keys):
            return
        try:
            resolved = self._resolve_existing(form, source_key, keys,
                                              ZohoTrafficGate.BACKFILL)
            if resolved is not None:
                self._zoho.update_record(form, resolved, payload,
                                         priority=ZohoTrafficGate.BACKFILL)
                zoho_map.upsert(cur, source_key, resolved, **keys)
                return
            zoho_id = self._zoho.add_record(form, payload, priority=ZohoTrafficGate.BACKFILL)
            zoho_map.upsert(cur, source_key, zoho_id, **keys)
        except ZohoRetryableError:
            raise
        except ZohoError as exc:
            log.warning("backfill skip %s %s: %s", source_key, keys, exc)
            return
        finally:
            zoho_map.release_claim(cur, source_key, **keys)

    def _resolve_existing(self, form: str, source_key: str, keys: dict[str, Any], priority: int) -> str | None:
        if source_key == "ITEMS":
            crit = f"(Company_Number == {keys['k_cp']}) && (Year == {keys['k_yr']}) && (Item_Code == \"{keys['k_code']}\")"
        else:
            crit = f"(Company_Number == {keys['k_cp']}) && (Year == {keys['k_yr']}) && (Branch_Number == {keys['k_bn']})"
        return self._zoho.find_record_by_criteria(form, crit, priority=priority)

    def _read_checkpoint(self, table: str, fields: tuple[str, ...]) -> tuple:
        with self._pool.connection() as conn:
            cur = conn.cursor()
            cur.execute(
                f"SELECT {', '.join(fields)} FROM BACKFILL_CHECKPOINT "
                f"WHERE source_table = :t",
                t=table,
            )
            row = cur.fetchone()
            if not row:
                cur.execute(
                    "INSERT INTO BACKFILL_CHECKPOINT(source_table) VALUES(:t)",
                    t=table,
                )
                conn.commit()
                return tuple(None for _ in fields)
            return row

    def _update_checkpoint(self, conn, table: str, **fields) -> None:
        sets = ", ".join(f"{k} = :{k}" for k in fields)
        conn.cursor().execute(
            f"UPDATE BACKFILL_CHECKPOINT SET {sets}, rows_done = rows_done + 1 "
            f"WHERE source_table = :t",
            t=table, **fields,
        )

    def _mark_finished(self, conn, table: str) -> None:
        conn.cursor().execute(
            "UPDATE BACKFILL_CHECKPOINT SET finished_at = SYSTIMESTAMP "
            "WHERE source_table = :t AND finished_at IS NULL",
            t=table,
        )
