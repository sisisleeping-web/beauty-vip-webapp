#!/usr/bin/env python3
"""
sync_from_cloud.py — 從 PythonAnywhere 下載最新 DB 備份並同步到本機

使用方式：
  python3 scripts/sync_from_cloud.py
  python3 scripts/sync_from_cloud.py --pin 1225   # 指定 PIN（不建議，留 shell history）

流程：
  1. 登入 /manager/unlock 取得 session
  2. 下載 /manager/backup（最新雲端快照）
  3. 比對雲端 vs 本機筆數、大小
  4. 跳確認（需輸入 y）
  5. 備份本機舊 DB → data/backups/
  6. 覆蓋本機 DB
"""

import argparse
import getpass
import http.cookiejar
import shutil
import sqlite3
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

BASE_URL = "https://sisisleeping.pythonanywhere.com"
BASE_DIR = Path(__file__).resolve().parent.parent
LOCAL_DB  = BASE_DIR / "data" / "beauty_vip.db"
BACKUP_DIR = BASE_DIR / "data" / "backups"


def db_summary(db_path: Path) -> dict:
    """讀取 DB 基本統計"""
    if not db_path.exists():
        return {"exists": False}
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        txn_count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        cust_count = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
        latest = conn.execute(
            "SELECT MAX(txn_date) FROM transactions"
        ).fetchone()[0] or "無資料"
        total_rev = conn.execute(
            "SELECT COALESCE(SUM(final_amount),0) FROM transactions WHERE entry_mode='normal'"
        ).fetchone()[0]
        disc_check = conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE birthday_discount_applied=1"
        ).fetchone()[0]
        conn.close()
        return {
            "exists": True,
            "size_kb": db_path.stat().st_size // 1024,
            "customers": cust_count,
            "transactions": txn_count,
            "latest_txn": latest,
            "total_revenue": total_rev,
            "discount_anomaly": disc_check,
        }
    except Exception as e:
        return {"exists": True, "error": str(e)}


def print_summary(label: str, s: dict):
    if not s.get("exists"):
        print(f"  {label}：（不存在）")
        return
    if "error" in s:
        print(f"  {label}：讀取失敗 — {s['error']}")
        return
    print(f"  {label}：")
    print(f"    大小        {s['size_kb']:,} KB")
    print(f"    顧客數      {s['customers']:,}")
    print(f"    交易筆數    {s['transactions']:,}")
    print(f"    最新交易    {s['latest_txn']}")
    print(f"    消費總額    {s['total_revenue']:,.0f} 元")
    if s.get("discount_anomaly", 0) > 0:
        print(f"    ⚠ 折扣異常  {s['discount_anomaly']} 筆（應為 0）")


def main():
    parser = argparse.ArgumentParser(description="從雲端下載最新 DB 備份同步到本機")
    parser.add_argument("--pin", help="管理後台 PIN（不指定則互動輸入）")
    parser.add_argument("--yes", "-y", action="store_true", help="跳過確認直接覆蓋")
    args = parser.parse_args()

    print("=" * 55)
    print("  美咖美容 VIP — 雲端 DB 同步到本機")
    print("=" * 55)

    # ── Step 1: 取得 PIN ───────────────────────────────────────
    pin = args.pin or getpass.getpass("管理後台 PIN：")
    if not pin:
        print("❌ PIN 不可為空")
        sys.exit(1)

    # ── Step 2: 登入取 session cookie ─────────────────────────
    print("\n[1/4] 登入管理後台...")
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

    login_data = urllib.parse.urlencode({"pin": pin}).encode()
    try:
        resp = opener.open(f"{BASE_URL}/manager/unlock", login_data, timeout=15)
        # 成功登入後會跳轉到 /manager，確認 session 有效
        final_url = resp.geturl()
        if "login" in final_url.lower() or "unlock" in final_url.lower():
            print("❌ PIN 錯誤，登入失敗")
            sys.exit(1)
    except urllib.error.URLError as e:
        print(f"❌ 連線失敗：{e}")
        sys.exit(1)
    print("   ✓ 登入成功")

    # ── Step 3: 下載備份 ───────────────────────────────────────
    print("[2/4] 下載雲端 DB 備份...")
    tmp_file = Path(tempfile.mktemp(suffix=".db", prefix="beauty_cloud_"))
    try:
        resp = opener.open(f"{BASE_URL}/manager/backup", timeout=30)
        content_disp = resp.headers.get("Content-Disposition", "")
        cloud_filename = "beauty_vip_cloud.db"
        if "filename=" in content_disp:
            cloud_filename = content_disp.split("filename=")[-1].strip().strip('"')
        with open(tmp_file, "wb") as f:
            f.write(resp.read())
    except urllib.error.URLError as e:
        print(f"❌ 下載失敗：{e}")
        sys.exit(1)
    print(f"   ✓ 下載完成：{cloud_filename}")

    # ── Step 4: 比對雲端 vs 本機 ──────────────────────────────
    print("\n[3/4] 比對資料...")
    cloud_summary = db_summary(tmp_file)
    local_summary = db_summary(LOCAL_DB)

    print()
    print_summary("雲端（最新）", cloud_summary)
    print()
    print_summary("本機（現有）", local_summary)
    print()

    # 差異提示
    if local_summary.get("exists") and not local_summary.get("error"):
        c_txn = cloud_summary.get("transactions", 0)
        l_txn = local_summary.get("transactions", 0)
        diff = c_txn - l_txn
        if diff > 0:
            print(f"  → 雲端比本機多 {diff} 筆交易")
        elif diff < 0:
            print(f"  ⚠ 本機比雲端多 {abs(diff)} 筆（不正常，請確認）")
        else:
            print(f"  → 筆數相同（{c_txn} 筆）")

    # ── Step 5: 確認 ──────────────────────────────────────────
    print("-" * 55)
    if not args.yes:
        answer = input("確定要用雲端資料覆蓋本機 DB？(y/N) ").strip().lower()
        if answer != "y":
            print("❌ 已取消，本機 DB 未變動")
            tmp_file.unlink(missing_ok=True)
            sys.exit(0)

    # ── Step 6: 備份舊 DB + 覆蓋 ─────────────────────────────
    print("\n[4/4] 備份舊 DB 並覆蓋...")
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    if LOCAL_DB.exists():
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dest = BACKUP_DIR / f"beauty_vip_pre_sync_{ts}.db"
        shutil.copy2(LOCAL_DB, backup_dest)
        print(f"   ✓ 舊 DB 備份至：{backup_dest}")

    # Keep only last 10 backups
    backups = sorted(BACKUP_DIR.glob("beauty_vip_*.db"))
    for old in backups[:-10]:
        old.unlink()

    shutil.move(str(tmp_file), LOCAL_DB)
    print(f"   ✓ 本機 DB 已更新：{LOCAL_DB}")

    # ── 驗算 ──────────────────────────────────────────────────
    after = db_summary(LOCAL_DB)
    print(f"\n✓ 同步完成 — 本機現有 {after.get('customers',0)} 位顧客、"
          f"{after.get('transactions',0)} 筆交易")
    if after.get("discount_anomaly", 0) > 0:
        print(f"⚠ 注意：DB 內有 {after['discount_anomaly']} 筆折扣異常，"
              f"請執行 python3 scripts/db_check.py 確認")


if __name__ == "__main__":
    main()
