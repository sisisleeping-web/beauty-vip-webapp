#!/usr/bin/env python3
"""spa_bookings.updated_at 獨立 migration（冪等）。

正式環境 WSGI 不會呼叫 init_db()，所以此欄位須明確執行本腳本補上：
  python3 scripts/migrate_spa_bookings_updated_at.py --check   # 唯讀，缺欄位 exit 1
  python3 scripts/migrate_spa_bookings_updated_at.py --apply   # 先備份，再 ADD COLUMN

回滾：以 --apply 產生的 <db>.bak_YYYYMMDD_HHMMSS 還原（停服務後覆蓋）。
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

log = logging.getLogger("migrate_spa_bookings_updated_at")

DEFAULT_DB = Path(__file__).resolve().parents[1] / "data" / "beauty_vip.db"
TABLE, COLUMN = "spa_bookings", "updated_at"


def _columns(conn: sqlite3.Connection) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({TABLE})")}


def check(db_path: Path) -> bool:
    """唯讀檢查；資料表不存在視為錯誤（不是缺欄位）。"""
    if not db_path.is_file():
        raise SystemExit(f"DB 不存在：{db_path}")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        cols = _columns(conn)
    finally:
        conn.close()
    if not cols:
        raise SystemExit(f"資料表 {TABLE} 不存在，請先確認 DB 路徑")
    return COLUMN in cols


def backup(db_path: Path) -> Path:
    target = db_path.with_name(f"{db_path.name}.bak_{datetime.now():%Y%m%d_%H%M%S}")
    src = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    dst = sqlite3.connect(target)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    return target


def apply(db_path: Path) -> str:
    """回傳 'already' 或 'applied'；已存在時不備份、不寫入。"""
    if check(db_path):
        return "already"
    bak = backup(db_path)
    log.info("備份完成：%s", bak)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(f"ALTER TABLE {TABLE} ADD COLUMN {COLUMN} TEXT DEFAULT NULL")
        conn.commit()
    finally:
        conn.close()
    if not check(db_path):
        raise SystemExit("migration 後驗證失敗，請以備份還原")
    return "applied"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="唯讀檢查，缺欄位 exit 1")
    mode.add_argument("--apply", action="store_true", help="明確執行（先備份）")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="DB 路徑（預設 data/beauty_vip.db）")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)

    if args.check:
        ok = check(args.db)
        log.info("%s.%s %s", TABLE, COLUMN, "存在" if ok else "缺少")
        return 0 if ok else 1
    log.info("結果：%s", apply(args.db))
    return 0


if __name__ == "__main__":
    sys.exit(main())
