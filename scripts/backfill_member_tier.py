"""2026美咖會員制度V3 Phase 1：既有顧客的會員年度制初始化（一次性）。

現有顧客沒有「正式升等日」的歷史紀錄，沒辦法還原每個人真正的升等日期。這支腳本
用目前的判定邏輯（all-time 最高單筆 + 目前日曆年至今累計）算出當下應該是什麼等級，
把 tier_effective_date 設為執行當天、tier_expires_date 設為 +1 年——代表所有現有
S/P/A 會員的「會員年度」都會從執行當天重新起算。這是唯一誠實的做法（見
CLAUDE.md / memory 的說明）。

已經有 tier_effective_date 的顧客會被跳過，重複執行是安全的，不會覆蓋已經生效的
會員年度視窗。

Usage（照 CLAUDE.md 既有流程：上傳到 PA → console 執行 → 驗算 → 刪腳本）：
    python3 scripts/backfill_member_tier.py            # 實際寫入
    python3 scripts/backfill_member_tier.py --dry-run   # 只印出會怎麼改，不寫入
"""

import sqlite3
import sys
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "beauty_vip.db"


def tier_name_from_totals(max_single: float, year_total: float) -> str:
    if max_single >= 30000 or year_total >= 60000:
        return "A級美咖"
    if max_single >= 12000 or year_total >= 24000:
        return "P級美咖"
    if max_single >= 8000 or year_total >= 15000:
        return "S級美咖"
    return "一般會員"


def add_years(d: date, n: int) -> date:
    try:
        return d.replace(year=d.year + n)
    except ValueError:
        return d.replace(year=d.year + n, day=28)


def main():
    dry_run = "--dry-run" in sys.argv
    today = date.today()
    today_str = today.isoformat()
    year_str = today.strftime("%Y")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    customers = cur.execute(
        "SELECT id, name, tier_effective_date FROM customers"
    ).fetchall()
    print(f"共 {len(customers)} 位顧客，{'（僅預覽，不寫入）' if dry_run else ''}")

    updated = 0
    skipped_existing = 0
    for c in customers:
        cid, cname = c["id"], c["name"]
        if c["tier_effective_date"]:
            skipped_existing += 1
            continue

        max_single = float(
            cur.execute(
                "SELECT COALESCE(MAX(final_amount),0) FROM transactions "
                "WHERE customer_id=? AND voided_at IS NULL",
                (cid,),
            ).fetchone()[0] or 0
        )
        year_total = float(
            cur.execute(
                "SELECT COALESCE(SUM(final_amount),0) FROM transactions "
                "WHERE customer_id=? AND substr(txn_date,1,4)=? AND final_amount>=1000 AND voided_at IS NULL",
                (cid, year_str),
            ).fetchone()[0] or 0
        )
        tier = tier_name_from_totals(max_single, year_total)

        if tier == "一般會員":
            print(f"  {cname}: 一般會員（無會員年度視窗）")
            if not dry_run:
                cur.execute(
                    "UPDATE customers SET member_tier=? WHERE id=?",
                    (tier, cid),
                )
        else:
            expires = add_years(today, 1).isoformat()
            print(f"  {cname}: {tier}，會員年度 {today_str} ~ {expires}（單筆最高 {max_single:,.0f}／今年累計 {year_total:,.0f}）")
            if not dry_run:
                cur.execute(
                    "UPDATE customers SET member_tier=?, tier_effective_date=?, tier_expires_date=? WHERE id=?",
                    (tier, today_str, expires, cid),
                )
        updated += 1

    if not dry_run:
        conn.commit()
    conn.close()
    print(f"\n{'（dry-run，未寫入）' if dry_run else '✅ 完成'}：{updated} 位已初始化，{skipped_existing} 位已有會員年度視窗（跳過）")


if __name__ == "__main__":
    main()
