"""2026美咖會員制度V3 Phase 3：既有點數餘額遷移成分筆效期帳本的一筆 legacy 批次。

背景：Phase 3 之前，customers.coin_balance 是單一數字，沒有效期追蹤。這支腳本幫每位
coin_balance > 0 的顧客建一筆 is_legacy=1 的 coin_batches 紀錄：credit_date 設成
執行當天、expires_date 設成 +1年——不回溯回原本賺點的歷史日期，因為沒辦法還原每一筆
的真實入帳日，硬回溯只會讓真實存在的餘額憑空提前過期，遷移日當基準是唯一不會造成
顧客損失的做法。

已經有 is_legacy=1 批次的顧客會被跳過，重複執行是安全的。

Usage（照 CLAUDE.md 既有流程：上傳到 PA → console 執行 → 驗算 → 刪腳本）：
    python3 scripts/backfill_coin_batches.py            # 實際寫入
    python3 scripts/backfill_coin_batches.py --dry-run  # 只印出會怎麼改，不寫入
"""

import sqlite3
import sys
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
import app as beautyapp  # noqa: E402


def main():
    dry_run = "--dry-run" in sys.argv
    today = date.today()
    today_str = today.isoformat()
    expires = beautyapp._add_years(today_str, 1)

    db = sqlite3.connect(beautyapp.DB_PATH)
    db.row_factory = sqlite3.Row

    customers = db.execute("SELECT id, name, coin_balance FROM customers WHERE coin_balance > 0").fetchall()
    print(f"共 {len(customers)} 位 coin_balance > 0 的顧客，{'（僅預覽，不寫入）' if dry_run else ''}")

    migrated = 0
    skipped_existing = 0
    now_str = beautyapp.datetime.now().isoformat(timespec="seconds")
    for c in customers:
        cid, cname, balance = c["id"], c["name"], int(c["coin_balance"])
        existing = db.execute(
            "SELECT id FROM coin_batches WHERE customer_id=? AND is_legacy=1", (cid,)
        ).fetchone()
        if existing:
            skipped_existing += 1
            continue
        print(f"  {cname}: 建立 legacy 批次 {balance} 點，效期 {today_str} ~ {expires}")
        if not dry_run:
            db.execute(
                "INSERT INTO coin_batches(customer_id, source_txn_id, source_adjustment_id, earned_amount, "
                "remaining_amount, credit_date, expires_date, status, is_legacy, created_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (cid, None, None, balance, balance, today_str, expires, "active", 1, now_str),
            )
        migrated += 1

    if not dry_run:
        db.commit()
        # 遷移完立刻用批次重算一次餘額，跟原本的 coin_balance 應該完全一致（因為
        # 這支腳本就是照原本的 coin_balance 建批次，只是多了效期追蹤）
        beautyapp.sync_all_coin_balances(db, today)
        db.commit()

    print(f"\n{'（dry-run，未寫入）' if dry_run else '完成'}：{migrated} 位已建立 legacy 批次，{skipped_existing} 位已有 legacy 批次（跳過）")
    db.close()


if __name__ == "__main__":
    main()
