#!/usr/bin/env python3
"""spa_bookings.updated_at 獨立 migration（冪等、fail-closed）。

正式環境 WSGI 不會呼叫 init_db()，所以此欄位須明確執行本腳本補上：
  python3 scripts/migrate_spa_bookings_updated_at.py --check   # 唯讀，缺欄位 exit 1
  python3 scripts/migrate_spa_bookings_updated_at.py --apply   # 鎖定→一致性備份→驗證→ADD COLUMN→驗證

--apply 流程：取得寫入鎖（BEGIN IMMEDIATE，取不到即放棄、不改任何東西）→ 以 sqlite backup API
產生一致性快照（WAL 亦正確）→ 驗證備份 integrity_check 與筆數 → 才 ALTER → 驗證欄位、
integrity_check、筆數不變。任何一步失敗都不會留下半套 schema；備份失敗則移除殘檔。

回滾：新增的是 nullable 欄位，優先「保留欄位」；若必須還原，停寫入後用 <db>.bak_* 還原並先評估
備份之後的新預約（見部署計畫）。
"""
from __future__ import annotations

import argparse
import logging
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

log = logging.getLogger("migrate_spa_bookings_updated_at")

DEFAULT_DB = Path(__file__).resolve().parents[1] / "data" / "beauty_vip.db"
TABLE, COLUMN = "spa_bookings", "updated_at"
LOCK_TIMEOUT_SEC = 30
DISK_SAFETY_FACTOR = 2


def _columns(conn: sqlite3.Connection) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({TABLE})")}


def _require_table(db_path: Path, cols: set[str]) -> None:
    if not cols:
        raise SystemExit(f"資料表 {TABLE} 不存在，請先確認 DB 路徑：{db_path}")


def check(db_path: Path) -> bool:
    """唯讀檢查；DB 或資料表不存在視為錯誤（不是缺欄位）。"""
    if not db_path.is_file():
        raise SystemExit(f"DB 不存在：{db_path}")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        cols = _columns(conn)
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    finally:
        conn.close()
    _require_table(db_path, cols)
    log.info("journal_mode=%s", mode)
    return COLUMN in cols


def _verify_snapshot(path: Path, expected_rows: int) -> None:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        result = conn.execute("PRAGMA integrity_check").fetchone()[0]
        rows = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    finally:
        conn.close()
    if result != "ok" or rows != expected_rows:
        raise SystemExit(f"備份驗證失敗（integrity={result}, rows={rows}, 預期 {expected_rows}）")


def _make_backup(db_path: Path, expected_rows: int) -> Path:
    free = shutil.disk_usage(db_path.parent).free
    if free < db_path.stat().st_size * DISK_SAFETY_FACTOR:
        raise SystemExit(f"磁碟空間不足（可用 {free} bytes），中止、未修改 DB")
    target = db_path.with_name(f"{db_path.name}.bak_{datetime.now():%Y%m%d_%H%M%S}")
    if target.exists():
        raise SystemExit(f"備份檔已存在，拒絕覆蓋：{target}")
    src = sqlite3.connect(db_path, timeout=LOCK_TIMEOUT_SEC)
    dst = sqlite3.connect(target)
    try:
        src.backup(dst)
        dst.close()
        _verify_snapshot(target, expected_rows)
    except BaseException:
        dst.close()
        target.unlink(missing_ok=True)
        raise
    finally:
        src.close()
    return target


def _post_verify(db_path: Path, expected_rows: int) -> None:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        cols = _columns(conn)
        result = conn.execute("PRAGMA integrity_check").fetchone()[0]
        rows = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    finally:
        conn.close()
    if COLUMN not in cols or result != "ok" or rows != expected_rows:
        raise SystemExit(f"migration 後驗證失敗（col={COLUMN in cols}, integrity={result}, rows={rows}）")


def apply(db_path: Path) -> str:
    """回傳 'already' 或 'applied'。全程持有寫入鎖，備份與 ALTER 之間不會有新預約插入。"""
    check(db_path)
    lock = sqlite3.connect(db_path, timeout=LOCK_TIMEOUT_SEC, isolation_level=None)
    try:
        try:
            lock.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            raise SystemExit(f"無法取得資料庫寫入鎖（{exc}），未修改任何東西") from exc
        if COLUMN in _columns(lock):
            lock.execute("ROLLBACK")
            return "already"
        rows = lock.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
        backup_path = _make_backup(db_path, rows)
        log.info("備份完成：%s（%d bytes，integrity ok，%d 筆）", backup_path, backup_path.stat().st_size, rows)
        lock.execute(f"ALTER TABLE {TABLE} ADD COLUMN {COLUMN} TEXT DEFAULT NULL")
        lock.execute("COMMIT")
    except BaseException:
        if lock.in_transaction:
            lock.execute("ROLLBACK")
        raise
    finally:
        lock.close()
    _post_verify(db_path, rows)
    return "applied"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="唯讀檢查，缺欄位 exit 1")
    mode.add_argument("--apply", action="store_true", help="明確執行（鎖定→備份→驗證→ALTER）")
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
