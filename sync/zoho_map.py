"""ZOHO_MAP: composite Oracle key <-> Zoho record id."""
from __future__ import annotations

from typing import Any


def _params(source_table: str, k_cp, k_yr, k_code, k_bn) -> dict[str, Any]:
    return {"src": source_table, "cp": k_cp, "yr": k_yr,
            "code": k_code, "bn": k_bn}


def _where_clause() -> str:
    # NULL parts must compare equal too — emulate via NVL on both sides.
    return (
        "source_table = :src "
        "AND NVL(k_cp, -1)   = NVL(:cp, -1) "
        "AND NVL(k_yr, -1)   = NVL(:yr, -1) "
        "AND NVL(k_code,'~') = NVL(:code,'~') "
        "AND NVL(k_bn, -1)   = NVL(:bn, -1)"
    )


def lookup(cursor, source_table: str, k_cp=None, k_yr=None,
           k_code=None, k_bn=None) -> str | None:
    cursor.execute(
        f"SELECT zoho_record_id FROM ZOHO_MAP WHERE {_where_clause()}",
        _params(source_table, k_cp, k_yr, k_code, k_bn),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def upsert(cursor, source_table: str, zoho_record_id: str, k_cp=None,
           k_yr=None, k_code=None, k_bn=None) -> None:
    cursor.execute(
        """
        MERGE INTO ZOHO_MAP t
        USING (SELECT :src AS source_table, :cp AS k_cp, :yr AS k_yr,
                      :code AS k_code, :bn AS k_bn FROM dual) s
        ON (    t.source_table = s.source_table
            AND NVL(t.k_cp, -1)   = NVL(s.k_cp, -1)
            AND NVL(t.k_yr, -1)   = NVL(s.k_yr, -1)
            AND NVL(t.k_code,'~') = NVL(s.k_code,'~')
            AND NVL(t.k_bn, -1)   = NVL(s.k_bn, -1))
        WHEN MATCHED THEN UPDATE
            SET zoho_record_id = :rid, last_synced_at = SYSTIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (source_table, k_cp, k_yr, k_code, k_bn,
                    zoho_record_id, last_synced_at)
            VALUES (s.source_table, s.k_cp, s.k_yr, s.k_code, s.k_bn,
                    :rid, SYSTIMESTAMP)
        """,
        {**_params(source_table, k_cp, k_yr, k_code, k_bn), "rid": zoho_record_id},
    )


def delete(cursor, source_table: str, k_cp=None, k_yr=None,
           k_code=None, k_bn=None) -> None:
    cursor.execute(
        f"DELETE FROM ZOHO_MAP WHERE {_where_clause()}",
        _params(source_table, k_cp, k_yr, k_code, k_bn),
    )


def claim(cursor, source_table: str, k_cp=None, k_yr=None,
          k_code=None, k_bn=None) -> bool:
    """Try to claim key for create-path idempotency. Returns True if claimed."""
    params = _params(source_table, k_cp, k_yr, k_code, k_bn)
    cursor.execute(
        """
        MERGE INTO ZOHO_SYNC_CLAIMS t
        USING (SELECT :src AS source_table, :cp AS k_cp, :yr AS k_yr,
                      :code AS k_code, :bn AS k_bn FROM dual) s
        ON (    t.source_table = s.source_table
            AND NVL(t.k_cp, -1)   = NVL(s.k_cp, -1)
            AND NVL(t.k_yr, -1)   = NVL(s.k_yr, -1)
            AND NVL(t.k_code,'~') = NVL(s.k_code,'~')
            AND NVL(t.k_bn, -1)   = NVL(s.k_bn, -1))
        WHEN NOT MATCHED THEN
            INSERT (source_table, k_cp, k_yr, k_code, k_bn, claimed_at)
            VALUES (s.source_table, s.k_cp, s.k_yr, s.k_code, s.k_bn, SYSTIMESTAMP)
        """,
        params,
    )
    return cursor.rowcount == 1


def release_claim(cursor, source_table: str, k_cp=None, k_yr=None,
                  k_code=None, k_bn=None) -> None:
    cursor.execute(
        f"DELETE FROM ZOHO_SYNC_CLAIMS WHERE {_where_clause()}",
        _params(source_table, k_cp, k_yr, k_code, k_bn),
    )
