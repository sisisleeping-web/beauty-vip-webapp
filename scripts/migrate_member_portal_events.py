#!/usr/bin/env python3
"""Idempotent migration for aggregate member-portal analytics.

Run on PythonAnywhere from the project directory.  Rollback intentionally only
works while no events exist, preventing accidental loss of analytics evidence.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app


REQUIRED_COLUMNS = {"id", "member_id", "event_type", "metadata_json", "event_key", "created_at"}


def verify() -> None:
    conn = sqlite3.connect(app.DB_PATH)
    try:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(member_portal_events)")}
        indexes = {row[1] for row in conn.execute("PRAGMA index_list(member_portal_events)")}
    finally:
        conn.close()
    missing = REQUIRED_COLUMNS - columns
    required_indexes = {"idx_member_portal_events_created_at", "idx_member_portal_events_type_created", "idx_member_portal_events_member_created"}
    if missing or required_indexes - indexes:
        raise SystemExit(f"migration validation failed; missing columns={sorted(missing)}, indexes={sorted(required_indexes - indexes)}")


def rollback_if_empty() -> None:
    conn = sqlite3.connect(app.DB_PATH)
    try:
        count = conn.execute("SELECT COUNT(*) FROM member_portal_events").fetchone()[0]
        if count:
            raise SystemExit("refusing rollback: member_portal_events contains event data")
        conn.execute("DROP TABLE member_portal_events")
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--rollback-if-empty", action="store_true")
    args = parser.parse_args()
    if args.rollback_if_empty:
        rollback_if_empty()
        print("rollback complete (empty table only)")
    elif args.check:
        verify()
        print("member_portal_events schema is valid")
    else:
        app.init_db()
        verify()
        print("member_portal_events migration applied and verified")
