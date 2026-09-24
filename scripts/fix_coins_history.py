#!/usr/bin/env python3
"""
⚠️ DEPRECATED — 已知與正式會員年度制（V3）不一致，禁止在未經人工覆核前執行 ⚠️

DO NOT RUN without explicit review. 見 docs/audit/H1-root-cause-and-exposure-scan.md
與 docs/audit/H1-remediation-plan-dry-run.md：這支腳本的 `get_tier()` 用的是跟
`update_transaction`（H-1 bug）完全同一套「歷史單筆最高金額 / 日曆年累計」門檻公式，
跟 `app.py` 現行的 V3 會員年度制（`effective_tier_for_transaction` /
`reevaluate_and_persist_tier`，會處理 pending 升等延遲生效、會員年度到期重判／降級）
完全脫鉤。唯讀重放比對顯示：Production 現有 98% 的 normal 交易 `coins_earned` 已經
跟這支腳本「現在重跑一次」會算出的數字一致——強烈證據顯示這支腳本已經對正式歷史帳本
執行過，且正是 H-1 Historical Exposure Scan 找到 36 筆 mismatch（15 位顧客、淨影響
7446 點）的主要根因之一。

再次執行只會把同一種錯誤公式重新蓋一次到正式資料上，讓已知問題更難追溯，不會修好
任何東西。若要處理已知的歷史 mismatch，要用 Owner 已核准的 remediation plan
（逐筆/逐戶更正 `coin_batches`／`review_flags`，有留痕、有稽核紀錄），不是重跑這支
盲目全庫覆寫的腳本。

需要真的執行（例如要驗證某個假設）才把下面這行 os.environ 檢查繞過，且僅限本機唯讀
副本測試，絕不對正式 DB_PATH 執行。

Recalculate ALL normal transactions using the CORRECT business rule:
  - Tier is determined solely by PAST transactions (txn_date < current txn)
  - A big single purchase earns points at the CURRENT tier (before upgrade)
  - Tier upgrade takes effect from the NEXT transaction

Uses txn_date ordering (not id) to handle data imported out of DB-id order.

This script corrects the previous over-crediting caused by fix_coins_history.py
which incorrectly included current_amount in tier calculation.
"""

import os
import sqlite3
import sys
from pathlib import Path

if os.environ.get("ALLOW_DEPRECATED_RAW_THRESHOLD_SCRIPT") != "i-understand-this-is-deprecated-see-H1-audit":
    sys.exit(
        "REFUSED: 這支腳本已因 H-1 稽核發現的問題被停用（見檔案頂端說明與 "
        "docs/audit/H1-root-cause-and-exposure-scan.md）。\n"
        "需要明確設定 ALLOW_DEPRECATED_RAW_THRESHOLD_SCRIPT="
        "i-understand-this-is-deprecated-see-H1-audit 才會執行，而且只該對本機唯讀"
        "副本測試用，不該對正式 DB_PATH 執行。"
    )

DB_PATH = Path.home() / "beauty-vip-webapp" / "data" / "beauty_vip.db"

RATE_MAP = {
    "A級美咖": 0.08,
    "P級美咖": 0.05,
    "S級美咖": 0.03,
    "一般會員": 0.02,
}


def get_tier(past_max_single: float, year_total_before: float) -> str:
    """Tier based on PAST transactions only — no current_amount included."""
    if past_max_single >= 30000 or year_total_before >= 60000:
        return "A級美咖"
    if past_max_single >= 12000 or year_total_before >= 24000:
        return "P級美咖"
    if past_max_single >= 8000 or year_total_before >= 15000:
        return "S級美咖"
    return "一般會員"


conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# Fetch all normal transactions ordered by customer + txn_date
rows = cur.execute(
    """
    SELECT t.id, t.customer_id, t.final_amount, t.coins_earned, t.cashback, t.txn_date
    FROM transactions t
    WHERE t.entry_mode = 'normal' AND t.final_amount > 0
    ORDER BY t.customer_id, t.txn_date ASC, t.id ASC
    """
).fetchall()

updates = []
coin_deltas: dict[int, int] = {}

for r in rows:
    txn_id = r["id"]
    cust_id = r["customer_id"]
    amount = float(r["final_amount"])
    old_coins = int(r["coins_earned"] or 0)
    old_cashback = float(r["cashback"] or 0)
    txn_date = r["txn_date"]
    year_str = txn_date[:4]

    # Use txn_date ordering — tier determined by PAST txns only
    past_max = float(
        cur.execute(
            """SELECT COALESCE(MAX(final_amount),0) FROM transactions
               WHERE customer_id=? AND txn_date < ? AND entry_mode='normal'""",
            (cust_id, txn_date),
        ).fetchone()[0]
        or 0
    )
    year_before = float(
        cur.execute(
            """SELECT COALESCE(SUM(final_amount),0) FROM transactions
               WHERE customer_id=? AND substr(txn_date,1,4)=?
               AND txn_date < ? AND entry_mode='normal'""",
            (cust_id, year_str, txn_date),
        ).fetchone()[0]
        or 0
    )

    tier = get_tier(past_max, year_before)
    rate = RATE_MAP[tier]
    new_coins = int(amount * rate)
    new_cashback = round(amount * rate, 2)

    if new_coins != old_coins or abs(new_cashback - old_cashback) > 0.01:
        delta = new_coins - old_coins
        updates.append((new_coins, new_cashback, txn_id, cust_id, tier, old_coins, delta))
        coin_deltas[cust_id] = coin_deltas.get(cust_id, 0) + delta
        cur.execute(
            "UPDATE transactions SET coins_earned=?, cashback=? WHERE id=?",
            (new_coins, new_cashback, txn_id),
        )

print(f"Found {len(updates)} transactions needing correction:\n")
print(f"{'TxnID':>6} {'等級':<8} {'舊點數':>8} {'新點數':>8} {'差額':>7}")
print("-" * 50)
for new_coins, new_cashback, txn_id, cust_id, tier, old_coins, delta in updates:
    cname = cur.execute("SELECT name FROM customers WHERE id=?", (cust_id,)).fetchone()["name"]
    print(f"{txn_id:>6} {cname:<10} {tier:<8} {old_coins:>8} {new_coins:>8} {delta:>+7}")

print(f"\nUpdating {len(coin_deltas)} customer coin balances...")
for cust_id, delta in coin_deltas.items():
    if delta != 0:
        # Recalculate from scratch for safety
        true_balance = cur.execute(
            """SELECT
                 COALESCE(SUM(CASE WHEN entry_mode IN ('normal','birthday_recharge') THEN coins_earned ELSE 0 END), 0)
                 - COALESCE(SUM(coins_redeemed), 0) AS net
               FROM transactions WHERE customer_id=?""",
            (cust_id,)
        ).fetchone()["net"]
        row = cur.execute("SELECT name, coin_balance FROM customers WHERE id=?", (cust_id,)).fetchone()
        print(f"  {row['name']}: {row['coin_balance']} → {true_balance} (delta: {delta:+d})")
        cur.execute("UPDATE customers SET coin_balance=? WHERE id=?", (true_balance, cust_id))

conn.commit()
conn.close()
print("\n✅ Done! All corrections committed to DB.")
