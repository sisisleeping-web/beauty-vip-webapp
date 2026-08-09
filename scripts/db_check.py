#!/usr/bin/env python3
"""
db_check.py — 美咖美容 VIP 資料完整性驗算

執行方式：
  python3 scripts/db_check.py          # 本機 DB
  python3 scripts/db_check.py [path]   # 指定 DB 路徑
"""
import sqlite3, json, sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
DB_PATH = Path(sys.argv[1]) if len(sys.argv) > 1 else BASE / "data" / "beauty_vip.db"
RULES_PATH = BASE / "rules.json"

if not DB_PATH.exists():
    print(f"DB not found: {DB_PATH}")
    sys.exit(1)

conn = sqlite3.connect(str(DB_PATH))
conn.row_factory = sqlite3.Row

errors = []
warnings = []

# ─── Check 1: birthday_discount_applied 應全為 0 ──────────────────────────
disc = conn.execute("SELECT COUNT(*) FROM transactions WHERE birthday_discount_applied=1 AND voided_at IS NULL").fetchone()[0]
if disc > 0:
    errors.append(f"[ERROR] {disc} 筆 birthday_discount_applied=1（應為 0，折扣機制已移除）")
else:
    print(f"[OK] birthday_discount_applied: 全為 0")

# ─── Check 2: normal 交易 final_amount 應等於 amount ─────────────────────
mismatch = conn.execute("""
    SELECT COUNT(*) FROM transactions
    WHERE entry_mode='normal' AND ABS(final_amount - amount) > 0.01 AND voided_at IS NULL
""").fetchone()[0]
if mismatch > 0:
    errors.append(f"[ERROR] {mismatch} 筆 normal 交易 final_amount ≠ amount（不應有折扣）")
else:
    print(f"[OK] normal 交易 final_amount == amount: 全部一致")

# ─── Check 3（資訊性，Phase 3 起改由 Check 7 做真正的一致性驗證）──────────
# 這個總和只是「歷史累計賺點/扣點」，Phase 3 之後 coins_earned 在交易當下就寫入
# transactions 了，但點數要到 credit_date（次月）才真的入帳可用，兩者本來就會
# 暫時對不上（正常現象），所以這裡不再當硬性一致性檢查，只印出來當參考數字。
customers = conn.execute("SELECT id, name, coin_balance FROM customers").fetchall()
lifetime_earned = conn.execute("""
    SELECT COALESCE(SUM(CASE WHEN entry_mode IN ('normal','birthday_recharge') THEN coins_earned ELSE 0 END),0)
    FROM transactions WHERE voided_at IS NULL
""").fetchone()[0]
print(f"[INFO] 全體歷史累計賺點（含尚未入帳生效的批次）：{int(lifetime_earned):,}")

# ─── Check 4: coin_balance 不應為負 ──────────────────────────────────────
neg = conn.execute("SELECT COUNT(*) FROM customers WHERE coin_balance < 0").fetchone()[0]
if neg > 0:
    errors.append(f"[ERROR] {neg} 位顧客 coin_balance < 0")
else:
    print(f"[OK] coin_balance: 無負值")

# ─── Check 5: 壽星充值僅限當月 ───────────────────────────────────────────
invalid_recharge = conn.execute("""
    SELECT t.id, c.name, c.birthday, t.txn_date FROM transactions t
    JOIN customers c ON c.id = t.customer_id
    WHERE t.entry_mode='birthday_recharge'
    AND substr(c.birthday,6,2) != substr(t.txn_date,6,2)
""").fetchall()
if invalid_recharge:
    warnings.append(f"[WARN] {len(invalid_recharge)} 筆壽星充值日期非生日月：" +
                    ", ".join(f"ID={r['id']} {r['name']}" for r in invalid_recharge))
else:
    print(f"[OK] 壽星充值日期: 全部在生日月份")

# ─── Check 6: 未滿 1000 元交易（2026 V3 制度：可正常建檔賺點，只是不列入會員年度累計）───
under_min = conn.execute("""
    SELECT COUNT(*) FROM transactions WHERE entry_mode='normal' AND amount < 1000 AND voided_at IS NULL
""").fetchone()[0]
print(f"[INFO] {under_min} 筆 normal 交易金額 < 1000（正常，僅提示：這些交易不列入會員年度累計）")

# ─── Check 7: coin_batches 分筆帳本總和應等於 customers.coin_balance ──────
# customers.coin_balance 是從 coin_batches 算出來的快取值，兩邊要隨時一致
# （Phase 3 起 coin_balance 不再是唯一真相來源，coin_batches 才是）。
today = conn.execute("SELECT date('now')").fetchone()[0]
batch_mismatch = []
for c in customers:
    usable = conn.execute("""
        SELECT COALESCE(SUM(remaining_amount),0) FROM coin_batches
        WHERE customer_id=? AND status='active' AND credit_date<=? AND expires_date>=?
    """, (c["id"], today, today)).fetchone()[0]
    if int(usable) != c["coin_balance"]:
        batch_mismatch.append(f"  {c['name']}: coin_balance={c['coin_balance']} 批次可用總和={usable}")
if batch_mismatch:
    errors.append(f"[ERROR] {len(batch_mismatch)} 位顧客 coin_balance 跟 coin_batches 對不上：\n" + "\n".join(batch_mismatch))
else:
    print(f"[OK] coin_batches 分筆帳本：{len(customers)} 位顧客跟 coin_balance 快取值一致")

# ─── Check 8: coin_batches 不應出現負的 remaining_amount ─────────────────
neg_batches = conn.execute("SELECT COUNT(*) FROM coin_batches WHERE remaining_amount < 0").fetchone()[0]
if neg_batches > 0:
    errors.append(f"[ERROR] {neg_batches} 筆點數批次 remaining_amount < 0")
else:
    print(f"[OK] coin_batches: 無負的 remaining_amount")

# ─── Summary（不含已作廢交易）─────────────────────────────────────────────
total = conn.execute("SELECT COUNT(*) FROM transactions WHERE voided_at IS NULL").fetchone()[0]
voided_total = conn.execute("SELECT COUNT(*) FROM transactions WHERE voided_at IS NOT NULL").fetchone()[0]
cust_count = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
total_rev = conn.execute(
    "SELECT COALESCE(SUM(final_amount),0) FROM transactions WHERE entry_mode='normal' AND voided_at IS NULL"
).fetchone()[0]

print(f"\n=== 統計摘要 ===")
print(f"顧客數：{cust_count}")
print(f"交易筆數：{total}（另有 {voided_total} 筆已作廢，不計入）")
print(f"正常消費總額：{total_rev:,.0f}")

if warnings:
    print("\n" + "\n".join(warnings))
if errors:
    print("\n" + "\n".join(errors))
    sys.exit(1)
else:
    print("\n✓ 所有檢查通過")

conn.close()
