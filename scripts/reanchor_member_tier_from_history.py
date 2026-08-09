"""2026美咖會員制度V3 Phase 1 修正：既有顧客的會員年度改用真實歷史升等日期錨定。

背景：backfill_member_tier.py 第一次跑的時候，因為系統從沒追蹤過「正式升等日」，
只能把所有現有 S/P/A 會員的會員年度起算日設成遷移執行當天（2026-08-09）。但實際上
tier_upgrades 表裡已經有 360 筆真實的歷史升等紀錄（upgrade_date 有實際日期），這支
腳本改用「這位顧客升到目前等級的那一筆歷史紀錄」當錨點，比全部都用遷移日準確多了。

找不到對應歷史紀錄的顧客（理論上少數，例如系統上線前就已經是老客戶但升等紀錄沒被
追蹤到），維持原本遷移日起算，不強行猜測。

重新錨定後會立刻呼叫 app.py 正式跑的 reevaluate_and_persist_tier() 幫每位顧客追趕到
今天（如果新錨點算出來的效期已經過去，會依 V3 §6 規則重新判定續會/降級/回一般會員，
這是正確行為，不是 bug）。

Usage（照 CLAUDE.md 既有流程：上傳到 PA → console 執行 → 驗算 → 刪腳本）：
    python3 scripts/reanchor_member_tier_from_history.py            # 實際寫入
    python3 scripts/reanchor_member_tier_from_history.py --dry-run  # 只印出會怎麼改，不寫入
"""

import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
import app as beautyapp  # noqa: E402


def main():
    dry_run = "--dry-run" in sys.argv

    db = sqlite3.connect(beautyapp.DB_PATH)
    db.row_factory = sqlite3.Row

    customers = db.execute(
        "SELECT id, name, member_tier FROM customers WHERE tier_effective_date IS NOT NULL"
    ).fetchall()
    print(f"共 {len(customers)} 位目前有會員年度視窗的顧客，{'（僅預覽，不寫入）' if dry_run else ''}")

    reanchored = 0
    kept_migration_day = 0
    for c in customers:
        cid, cname, cur_tier = c["id"], c["name"], c["member_tier"]
        row = db.execute(
            "SELECT upgrade_date FROM tier_upgrades WHERE customer_id=? AND tier_after=? "
            "ORDER BY upgrade_date DESC, id DESC LIMIT 1",
            (cid, cur_tier),
        ).fetchone()
        if row:
            effective = (date.fromisoformat(row["upgrade_date"]) + timedelta(days=1)).isoformat()
            expires = beautyapp._add_years(effective, 1)
            print(f"  {cname}: {cur_tier} 錨點改成 {effective}（依歷史升等紀錄 {row['upgrade_date']} 達標），效期 {expires}")
            if not dry_run:
                db.execute(
                    "UPDATE customers SET tier_effective_date=?, tier_expires_date=?, "
                    "pending_tier=NULL, pending_effective_date=NULL WHERE id=?",
                    (effective, expires, cid),
                )
            reanchored += 1
        else:
            print(f"  {cname}: {cur_tier} 找不到對應歷史升等紀錄，維持遷移日起算")
            kept_migration_day += 1

    if not dry_run:
        db.commit()
    print(f"\n{'（dry-run，未寫入）' if dry_run else ''}錨點修正：{reanchored} 位改用歷史日期，{kept_migration_day} 位維持遷移日")

    if not dry_run:
        today = date.today()
        caught_up = 0
        for c in customers:
            beautyapp.reevaluate_and_persist_tier(db, int(c["id"]), today)
            caught_up += 1
        db.commit()
        print(f"已對 {caught_up} 位顧客套用最新到期重判（依 Phase 1 邏輯，可能觸發續會/降級）")

    db.close()


if __name__ == "__main__":
    main()
