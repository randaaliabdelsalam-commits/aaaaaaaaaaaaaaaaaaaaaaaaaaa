"""Lightweight fakes for unit testing the workers without Oracle / Zoho."""
from __future__ import annotations

import contextlib
from typing import Any


class FakeCursor:
    def __init__(self, owner: "FakeConnection"):
        self._owner = owner
        self.description = None
        self._rows: list[tuple] = []
        self._fetched = 0
        self.last_sql: str | None = None
        self.last_params: dict | tuple | None = None
        self.rowcount = 1  # default: claim succeeds in tests

    def execute(self, sql, params=None, **kwargs):
        self.last_sql = sql
        self.last_params = kwargs if kwargs else params
        self._owner.execute_calls.append((sql, self.last_params))
        # Use the registered handler queue, in order, that matches by SQL keyword.
        handler = self._owner.match_handler(sql)
        if handler is None:
            self._rows = []
            self.description = None
            return
        result = handler(sql, self.last_params)
        if result is None:
            self._rows = []
            self.description = None
        else:
            description, rows = result
            self.description = [(c, None) for c in description]
            self._rows = list(rows)
        self._fetched = 0

    def executemany(self, sql, seq):
        for params in seq:
            self.execute(sql, params)

    def fetchone(self):
        if self._fetched >= len(self._rows):
            return None
        row = self._rows[self._fetched]
        self._fetched += 1
        return row

    def fetchall(self):
        rest = self._rows[self._fetched:]
        self._fetched = len(self._rows)
        return rest

    def fetchmany(self, size=None):
        size = len(self._rows) if size is None else size
        end = min(len(self._rows), self._fetched + size)
        rows = self._rows[self._fetched:end]
        self._fetched = end
        return rows

    def close(self):
        pass


class FakeConnection:
    def __init__(self):
        self.execute_calls: list[tuple[str, Any]] = []
        self.commit_count = 0
        self.handlers: list[tuple[str, Any]] = []  # (sql_substring, callable)

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commit_count += 1

    def register(self, sql_substring: str, handler) -> None:
        """One-shot handler: pop after first match."""
        self.handlers.append((sql_substring, handler))

    def match_handler(self, sql: str):
        for i, (needle, h) in enumerate(self.handlers):
            if needle in sql:
                self.handlers.pop(i)
                return h
        return None


class FakePool:
    def __init__(self, conn: FakeConnection | None = None):
        self.conn = conn or FakeConnection()

    @contextlib.contextmanager
    def connection(self):
        yield self.conn

    def close(self):
        pass


class FakeZoho:
    def __init__(self):
        self.calls: list[tuple[str, ...]] = []
        self.next_id = 1
        self.fail_on: list[str] = []

    def add_record(self, form, payload, priority):
        self.calls.append(("add", form, dict(payload), priority))
        rid = f"REC-{self.next_id}"
        self.next_id += 1
        return rid

    def update_record(self, report, record_id, payload, priority):
        self.calls.append(("update", report, record_id, dict(payload), priority))

    def delete_record(self, report, record_id, priority):
        self.calls.append(("delete", report, record_id, priority))

    def find_record_by_criteria(self, report, criteria, priority):
        self.calls.append(("find", report, criteria, priority))
        return None
