#!/usr/bin/env python3
"""B1-01: opt-in, additive anniversary-anchor migration. Never backfill legacy rows."""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import app


COLUMN = "tier_anniversary_anchor"


def verify(db: sqlite3.Connection) -> None:
    rows = {r[1]: r for r in db.execute("PRAGMA table_info(customers)")}
    if COLUMN not in rows or rows[COLUMN][2].upper() != "TEXT" or rows[COLUMN][3] != 0:
        raise RuntimeError("B1-01 schema validation failed")


def apply(db: sqlite3.Connection) -> None:
    db.execute("BEGIN IMMEDIATE")
    try:
        rows = {r[1]: r for r in db.execute("PRAGMA table_info(customers)")}
        if not rows:
            raise RuntimeError("customers table missing; refusing implicit initialization")
        if COLUMN not in rows:
            db.execute(f"ALTER TABLE customers ADD COLUMN {COLUMN} TEXT")
        verify(db)
        db.commit()
    except Exception:
        db.rollback()
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    path = Path(app.DB_PATH)
    if not path.is_file():
        parser.error("database file does not exist")
    uri = f"{path.resolve().as_uri()}?mode={'rw' if args.apply else 'ro'}"
    with sqlite3.connect(uri, uri=True) as db:
        if args.apply:
            apply(db)
        else:
            verify(db)
    print("B1-01 anniversary anchor schema OK")


if __name__ == "__main__":
    main()
