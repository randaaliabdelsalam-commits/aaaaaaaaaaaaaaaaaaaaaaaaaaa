from sync.dead_worker import DeadRetryWorker

from .fakes import FakeConnection, FakePool
from .test_realtime_worker import SK1MF_COLS, SAMPLE_SK1MF_ROW, PS33_COLS


def _dead_events(*rows):
    cols = ["id", "source_table", "k_cp", "k_yr", "k_code", "k_bn", "last_source_hash"]
    return lambda *_: (cols, list(rows))


def test_dead_with_changed_source_hash_reactivates_new():
    conn = FakeConnection()
    conn.register("FROM SYNC_EVENTS WHERE status = 'DEAD'", _dead_events((1, "SK1MF", 1, 2025, "ITM-1", None, "old")))
    conn.register("FROM   sk1mf", lambda *_: (SK1MF_COLS, [SAMPLE_SK1MF_ROW]))
    conn.register("FROM   ps33mf", lambda *_: (PS33_COLS, [(5, 6)]))
    conn.register("UPDATE SYNC_EVENTS SET status='NEW'", lambda *_: None)

    w = DeadRetryWorker(FakePool(conn), poll_interval=0.0)
    assert w._resurrect_dead() == 1


def test_dead_missing_source_with_zoho_map_enqueues_delete():
    conn = FakeConnection()
    conn.register("FROM SYNC_EVENTS WHERE status = 'DEAD'", _dead_events((2, "SK1MF", 1, 2025, "ITM-1", None, "any")))
    conn.register("FROM   sk1mf", lambda *_: (SK1MF_COLS, []))
    conn.register("FROM ZOHO_MAP", lambda *_: (["zoho_record_id"], [("REC-1",)]))
    conn.register("UPDATE SYNC_EVENTS SET status='NEW', op='D'", lambda *_: None)

    w = DeadRetryWorker(FakePool(conn), poll_interval=0.0)
    assert w._resurrect_dead() == 1


def test_dead_missing_source_and_zoho_map_marks_done_resolved():
    conn = FakeConnection()
    conn.register("FROM SYNC_EVENTS WHERE status = 'DEAD'", _dead_events((3, "SK1MF", 1, 2025, "ITM-1", None, "any")))
    conn.register("FROM   sk1mf", lambda *_: (SK1MF_COLS, []))
    conn.register("FROM ZOHO_MAP", lambda *_: ([], []))
    conn.register("UPDATE SYNC_EVENTS SET status='DONE'", lambda *_: None)

    w = DeadRetryWorker(FakePool(conn), poll_interval=0.0)
    assert w._resurrect_dead() == 1
