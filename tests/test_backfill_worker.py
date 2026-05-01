from sync.backfill_worker import BackfillWorker

from .fakes import FakeConnection, FakePool, FakeZoho


ITEMS_COLS = (
    "SK1MCP SK1MYR SK1M1 SK1M2 SK1M3 SK1M9 SK1M11 SK1M12 SK1M13 SK1M14 "
    "SK1M16 SK1M17 SK1M18 SK1M19 SK1M20 SK1M21 SK1M22 SK1M24 SK1M29 "
    "SK1M31 SK1M32 SK1M33 SK1M34 SK1M36 SK1M37 SK1M39 SK1M40 SK1M41 "
    "SK1M261 PS33M2 PS33M4"
).split()


def _items_row(cp, yr, code):
    base = (cp, yr, code, "ع", "E", 1,
            "Y", "N", "Y", 14, 16, 17, 18, 19, 20, 21, 22,
            "AC", "PAR", "Y", "N", "Y", 34, "KG", 37, 39, "N", "Y",
            261, 5, 6)
    return base


def test_items_backfill_paginates_until_empty(monkeypatch):
    monkeypatch.setattr("sync.backfill_worker.PAGE_SIZE", 2, raising=False)
    conn = FakeConnection()
    # reading checkpoint
    conn.register("FROM BACKFILL_CHECKPOINT",
                  lambda *_: (["last_cp", "last_yr", "last_code"],
                              [(None, None, None)]))
    # first items page
    conn.register(
        "FROM   sk1mf",
        lambda *_: (ITEMS_COLS,
                    [_items_row(1, 2025, "A"), _items_row(1, 2025, "B")]),
    )
    # zoho_map lookup x2 (not present), upsert x2, checkpoint update x2
    for _ in range(2):
        conn.register("FROM ZOHO_MAP", lambda *_: ([], []))
        conn.register("MERGE INTO ZOHO_SYNC_CLAIMS", lambda *_: None)
        conn.register("/report/", lambda *_: (["data"], [()]))
        conn.register("MERGE INTO ZOHO_MAP", lambda *_: None)
        conn.register("DELETE FROM ZOHO_SYNC_CLAIMS", lambda *_: None)
        conn.register("UPDATE BACKFILL_CHECKPOINT", lambda *_: None)
    # second items page (empty) -> finished
    conn.register("FROM   sk1mf", lambda *_: (ITEMS_COLS, []))
    conn.register("UPDATE BACKFILL_CHECKPOINT", lambda *_: None)
    # branches checkpoint and empty page
    conn.register("FROM BACKFILL_CHECKPOINT",
                  lambda *_: (["last_cp", "last_yr", "last_bn"],
                              [(None, None, None)]))
    conn.register("FROM   GRBRF", lambda *_: (["GRBRCP", "GRBRYR", "BN",
                                               "GRBR2", "GRBR3"], []))
    conn.register("UPDATE BACKFILL_CHECKPOINT", lambda *_: None)

    zoho = FakeZoho()
    w = BackfillWorker(FakePool(conn), zoho, "Items_Data", "Branches_Codes",
                       page_size=2)
    w.run()
    assert sum(1 for c in zoho.calls if c[0] == "add") == 2


def test_mapped_row_updates_instead_of_add():
    conn = FakeConnection()
    conn.register("FROM BACKFILL_CHECKPOINT",
                  lambda *_: (["last_cp", "last_yr", "last_code"],
                              [(None, None, None)]))
    conn.register("FROM   sk1mf",
                  lambda *_: (ITEMS_COLS, [_items_row(1, 2025, "A")]))
    # already mapped -> update existing id
    conn.register("FROM ZOHO_MAP",
                  lambda *_: (["zoho_record_id"], [("REC-1",)]))
    conn.register("UPDATE BACKFILL_CHECKPOINT", lambda *_: None)
    # next page empty
    conn.register("FROM   sk1mf", lambda *_: (ITEMS_COLS, []))
    conn.register("UPDATE BACKFILL_CHECKPOINT", lambda *_: None)
    # branches: empty
    conn.register("FROM BACKFILL_CHECKPOINT",
                  lambda *_: (["last_cp", "last_yr", "last_bn"],
                              [(None, None, None)]))
    conn.register("FROM   GRBRF",
                  lambda *_: (["GRBRCP", "GRBRYR", "BN", "GRBR2", "GRBR3"], []))
    conn.register("UPDATE BACKFILL_CHECKPOINT", lambda *_: None)

    zoho = FakeZoho()
    w = BackfillWorker(FakePool(conn), zoho, "Items_Data", "Branches_Codes",
                       page_size=10)
    w.run()
    assert zoho.calls[0][0] == "update"
