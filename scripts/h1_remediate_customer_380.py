#!/usr/bin/env python3
"""
H-1 Historical Under-Credit Remediation — customer_id=380（林秋蘭），一次性、單一顧客。

背景：docs/audit/H1-positive-only-entitlement-verification.md
唯一 HIGH-confidence under-credit candidate。交易 id=678（2026-05-02，9600 元）
當時顧客已是 S 級美咖（tier_effective_date=2026-03-15），應以 3% 費率計算
（288 點），正式歷史記錄卻用了一般會員 2%（192 點），短少 96 點。

Owner 已核准 Positive-Only Remediation（+96，company absorbs 70 位過度發放，
不追回；7 位 HISTORICAL_SOURCE_INCOMPLETE 不自動補發——這支腳本只處理 380 這
唯一一位）。

這支腳本：
  - 只對 customer_id=380 寫入，不碰任何其他顧客、任何歷史 transaction、
    任何既有 coin_batches／point_adjustments。
  - 走正式 source-of-truth：INSERT 一筆 point_adjustments（可追溯
    customer/points/operator/timestamp/reason），再呼叫正式
    create_coin_batch()（跟 /api/customers/<id>/add_points 同一支函式，
    immediate=True、expires_date 用正式 _add_years() 算，不手寫日期邏輯）。
  - 不直接 UPDATE customers.coin_balance——最後呼叫正式 sync_coin_balance()
    讓快取自然跟 batch 對齊。
  - Idempotent：執行前檢查是否已經有標記 H-1 remediation 的 point_adjustments，
    若已存在就直接跳出，不會重複加點。

Usage（照 CLAUDE.md 既有流程：上傳到 PA → console 執行 → 驗算 → 可保留或刪）：
    python3 scripts/h1_remediate_customer_380.py            # 實際寫入
    python3 scripts/h1_remediate_customer_380.py --dry-run  # 只印出會怎麼改，不寫入
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
import app as beauty  # noqa: E402

CUSTOMER_ID = 380
CORRECTION_POINTS = 96
REMEDIATION_MARKER = "H-1 歷史少發修正"
REASON = (
    f"{REMEDIATION_MARKER}：交易 id=678（2026-05-02，9600 元）當時顧客已生效 "
    "S 級美咖（tier_effective_date=2026-03-15），應以 3% 費率計算應得 288 點，"
    "正式歷史記錄誤用一般會員 2% 算成 192 點，短少 96 點。獨立重算三次交叉驗證"
    "（Exposure Scan／Legacy Opening Balance／Positive-Only Entitlement v3）"
    "結果一致。稽核：docs/audit/H1-positive-only-entitlement-verification.md"
)


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    db = beauty.sqlite3.connect(beauty.DB_PATH)
    db.row_factory = beauty.sqlite3.Row

    cust = db.execute("SELECT id, name, coin_balance FROM customers WHERE id=?", (CUSTOMER_ID,)).fetchone()
    if not cust:
        print(f"REFUSED: 找不到 customer_id={CUSTOMER_ID}")
        db.close()
        sys.exit(1)

    existing = db.execute(
        "SELECT id, created_at FROM point_adjustments WHERE customer_id=? AND reason LIKE ?",
        (CUSTOMER_ID, f"%{REMEDIATION_MARKER}%"),
    ).fetchone()
    if existing:
        print(
            f"REFUSED（idempotency guard）：customer_id={CUSTOMER_ID} 已經有一筆 H-1 remediation "
            f"point_adjustments（id={existing['id']}，建立於 {existing['created_at']}），不重複加點。"
        )
        db.close()
        sys.exit(1)

    before_balance = int(cust["coin_balance"])
    print(f"顧客：{cust['name']}（id={CUSTOMER_ID}）")
    print(f"補點前餘額：{before_balance}")
    print(f"本次修正：+{CORRECTION_POINTS}")
    print(f"預期補點後餘額：{before_balance + CORRECTION_POINTS}")

    if dry_run:
        print("\n（dry-run，未寫入）")
        db.close()
        return

    today = date.today()
    now_str = beauty.datetime.now().isoformat(timespec="seconds")
    db.execute(
        "INSERT INTO point_adjustments(customer_id, points, reason, operator, created_at) VALUES(?,?,?,?,?)",
        (CUSTOMER_ID, CORRECTION_POINTS, REASON, "manager", now_str),
    )
    adj_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    beauty.create_coin_batch(
        db, CUSTOMER_ID, CORRECTION_POINTS, today, source_adjustment_id=adj_id, immediate=True,
    )
    beauty.sync_coin_balance(db, CUSTOMER_ID, today)
    db.commit()

    batch = db.execute(
        "SELECT id, earned_amount, remaining_amount, credit_date, expires_date, status "
        "FROM coin_batches WHERE source_adjustment_id=?",
        (adj_id,),
    ).fetchone()
    after = db.execute("SELECT coin_balance FROM customers WHERE id=?", (CUSTOMER_ID,)).fetchone()
    integrity = db.execute("PRAGMA integrity_check").fetchone()[0]

    print("\n=== 完成 ===")
    print(f"POINT_ADJUSTMENT_ID={adj_id}")
    print(f"CORRECTION_BATCH_ID={batch['id']}")
    print(f"BATCH_EARNED={batch['earned_amount']}")
    print(f"BATCH_REMAINING={batch['remaining_amount']}")
    print(f"CREDIT_DATE={batch['credit_date']}")
    print(f"EXPIRES_DATE={batch['expires_date']}")
    print(f"BATCH_STATUS={batch['status']}")
    print(f"BALANCE_BEFORE={before_balance}")
    print(f"BALANCE_AFTER={after['coin_balance']}")
    print(f"BALANCE_DELTA={int(after['coin_balance']) - before_balance}")
    print(f"INTEGRITY_CHECK={integrity}")
    db.close()


if __name__ == "__main__":
    main()
