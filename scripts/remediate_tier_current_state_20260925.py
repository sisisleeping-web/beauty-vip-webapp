#!/usr/bin/env python3
"""
Tier Current-State Remediation — 7 位顧客，一次性、hard-coded allow-list。

背景（不得改寫的既有稽核鏈，只讀）：
  docs/audit/tier-timeline-audit-2026-09-25.md（全庫 Tier Timeline Correctness Audit）
  docs/audit/tier-remediation-candidates-2026-09-25.md（28 位 mismatch 候選表）
  docs/audit/tier-remediation-fact-sheets-2026-09-25.md（本腳本唯一依據的 7 位詳解，commit 20f5b6c）

範圍鎖定在這 7 位（CURRENT_STATE_IMPACT=YES，全部 28 位 mismatch 裡唯一「現在
顯示的等級本身就是錯的」子集）：246,301,336,352,354,392,402。不接受任何動態
population、不接受 command-line 指定別的顧客 id、不會因為之後又找到別的
mismatch 就自動擴大範圍——那些一律 REPORT_AS_FOLLOW_UP，不在這支腳本處理。

Root-cause 分群（沿用既有稽核文件既定分類，不在此重新推導）：
  Group A（246,301,354,392,402）：疑似 backfill_tier_upgrades.py 型的「日曆年
    全部交易累計、換級不歸零視窗」樣式，高度吻合但無 execution log，不宣稱
    proven cause。
  Group B（336）：疑似 backfill_member_tier.py 型的「直接覆寫 member_tier／
    tier_effective_date 欄位，繞過 tier_upgrades 稽核紀錄」樣式。
  Group C（352）：補登交易（backdated entry）本不該參與升等偵測，已由正式
    /entry route／Calculation Core replay 確認排除後她從未合法跨過門檻。

安全模型：
  1. CAS（compare-and-swap）：每一位的「完整 BEFORE 狀態」
     （member_tier / tier_effective_date / tier_expires_date / pending_tier /
     pending_effective_date）必須跟下面寫死的 EXPECTED_BEFORE 逐欄位完全相符，
     7 位都符合才會繼續；任何一位對不上 → 整批拒絕，0 writes。
  2. Idempotency：7 位若已經全部是 EXPECTED_AFTER 狀態 → 直接回報
     ALREADY_REMEDIATED / NO_WRITE，0 writes。
  3. Partial 狀態（部分是 BEFORE、部分是 AFTER，或任何跟兩者都對不上的狀態）
     → PARTIAL_OR_UNEXPECTED_STATE / REFUSE，0 writes，需要人工複核。
  4. All-or-nothing：全部驗證 → 全部套用 → 全部再驗證 → COMMIT，單一 DB
     transaction；任何一步失敗就 ROLLBACK，不允許部分 commit。
  5. 只允許寫入：customers 表的 member_tier / tier_effective_date /
     tier_expires_date / pending_tier / pending_effective_date（Gate C 確認
     為唯一 runtime 依賴的 authoritative 欄位，tier_upgrades 純稽核用途，讀取
     路徑不依賴它），以及每人一筆新增的 tier_upgrades 稽核列（event_type=
     'remediation'，記錄修正本身，不刪除／不修改任何既有 tier_upgrades 列）。
     絕不寫 transactions／coin_batches／coin_redemptions／point_adjustments／
     review_flags／任何點數欄位／任何非這 7 位以外的 customers 列。
  6. Financial fingerprint guard：對 transactions／coin_batches／
     coin_redemptions／point_adjustments 四張表算 before/after 指紋
     （COUNT + SUM），指紋不同就視為異常，報告會明確標示。

Usage:
    python3 scripts/remediate_tier_current_state_20260925.py --dry-run   # 只驗證、印出，不寫入（永遠 ROLLBACK）
    python3 scripts/remediate_tier_current_state_20260925.py             # 實際寫入（唯一 all-or-nothing transaction）
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
import app as beauty  # noqa: E402

REMEDIATION_MARKER = "TIER-REMEDIATION-20260925"
EVIDENCE_REF = "docs/audit/tier-remediation-fact-sheets-2026-09-25.md（commit 20f5b6c）"

# ── 固定 target population（不接受動態輸入） ────────────────────────────────
TARGET_IDS = (246, 301, 336, 352, 354, 392, 402)

# ── 每位的 BEFORE（Gate A 2026-09-25 讀到的 Production 現況，CAS 基準）
#    AFTER（Fact Sheet 的 EXPECTED，Gate B 對同一份 Gate A 快照重放已再次確認一致）
#    以及要新增的稽核列內容 ─────────────────────────────────────────────────
CUSTOMERS: dict[int, dict] = {
    246: {
        "name": "黃意玲",
        "group": "A",
        "before": {
            "member_tier": "A級美咖", "tier_effective_date": "2026-08-01",
            "tier_expires_date": "2027-08-01", "pending_tier": None, "pending_effective_date": None,
        },
        "after": {
            "member_tier": "P級美咖", "tier_effective_date": "2026-05-30",
            "tier_expires_date": "2027-05-30", "pending_tier": None, "pending_effective_date": None,
        },
        "source_transaction": "id=806, 2026-05-29, NT$4,100（視窗制累計自2026-03-21起達NT$25,429，跨過P級年度24,000門檻）",
        "why": (
            "STORED 軌跡（S@03-20正確→P@04-28→A@07-31）對應「日曆年全部交易累計、"
            "不分視窗歸零」的單一算法；正確視窗制從S生效日(2026-03-21)起算，只在"
            "05-29(txn806)跨過24,000一次，之後未再跨過60,000。疑似backfill_tier_upgrades.py"
            "型樣式，無execution log佐證，不宣稱proven cause。"
        ),
        "superseded_tier_upgrades_ids": [216, 353],
    },
    301: {
        "name": "唐怡芳",
        "group": "A",
        "before": {
            "member_tier": "P級美咖", "tier_effective_date": "2026-07-05",
            "tier_expires_date": "2027-07-05", "pending_tier": None, "pending_effective_date": None,
        },
        "after": {
            "member_tier": "S級美咖", "tier_effective_date": "2026-06-21",
            "tier_expires_date": "2027-06-21", "pending_tier": None, "pending_effective_date": None,
        },
        "source_transaction": "id=857, 2026-06-20, NT$4,999（日曆年累計達NT$19,648，跨過S級年度15,000門檻）",
        "why": (
            "STORED P級（生效07-05）對應「日曆年全部交易累計」在txn891(07-04)跨過"
            "24,000；視窗制從她真正的S級生效日(2026-06-21)起算，txn891單筆僅NT$8,700，"
            "未達P級任一門檻。另有一筆2026-08-10 void觸發的P→S降級事件"
            "（tier_upgrades id=364，trigger_txn_id=1013）其記錄的累計數字（NT$3,500）"
            "跟她目前完整交易史累計不吻合，時間點跟customer 302已知的同日void事件"
            "（本次任務Scope Out，見docs/audit/backdated-entry-review-2026-09-25.md）"
            "同型——列為FOLLOW_UP，不影響本次EXPECTED（已由完整正式replay獨立確認）。"
        ),
        "superseded_tier_upgrades_ids": [309, 364],
    },
    336: {
        "name": "吳豫函",
        "group": "B",
        "before": {
            "member_tier": "A級美咖", "tier_effective_date": "2026-09-09",
            "tier_expires_date": "2027-09-09", "pending_tier": None, "pending_effective_date": None,
        },
        "after": {
            "member_tier": "P級美咖", "tier_effective_date": "2026-03-19",
            "tier_expires_date": "2027-03-19", "pending_tier": None, "pending_effective_date": None,
        },
        "source_transaction": "id=502, 2026-03-18, NT$16,500（單筆一次跨過S的8,000與P的12,000兩個門檻）",
        "why": (
            "STORED A級美咖/2026-09-09沒有對應的tier_upgrades紀錄，且累計至"
            "2026-09-08僅NT$44,495，未達A級60,000門檻——欄位疑似被直接覆寫，"
            "繞過升等偵測與稽核紀錄，疑似backfill_member_tier.py型樣式。"
        ),
        "superseded_tier_upgrades_ids": [],  # 無對應紀錄可列（本身就是問題所在）
    },
    352: {
        "name": "游馥瑋",
        "group": "C",
        "before": {
            "member_tier": "S級美咖", "tier_effective_date": "2026-03-12",
            "tier_expires_date": "2027-03-12", "pending_tier": None, "pending_effective_date": None,
        },
        "after": {
            "member_tier": "一般會員", "tier_effective_date": None,
            "tier_expires_date": None, "pending_tier": None, "pending_effective_date": None,
        },
        "source_transaction": "無——本該觸發的txn578（2026-03-11, NT$6,750）因2026-04-13才補登建立而被規則排除",
        "why": (
            "STORED S級掛在txn578（txn_date=2026-03-11）跨過15,000門檻，但txn578是"
            "2026-04-13才補登建立（建立當下已存在txn_date=2026-04-07的交易，比它晚），"
            "依V3規則補登交易不做升等偵測（本次重放已正確觸發對應review_flags）。"
            "依即時判定，她從未合法跨過門檻，正確目前等級為一般會員。"
        ),
        "superseded_tier_upgrades_ids": [104],
    },
    354: {
        "name": "馮曼婷",
        "group": "A",
        "before": {
            "member_tier": "P級美咖", "tier_effective_date": "2026-04-24",
            "tier_expires_date": "2027-04-24", "pending_tier": None, "pending_effective_date": None,
        },
        "after": {
            "member_tier": "S級美咖", "tier_effective_date": "2026-03-08",
            "tier_expires_date": "2027-03-08", "pending_tier": None, "pending_effective_date": None,
        },
        "source_transaction": "id=492, 2026-03-07, NT$1,620（日曆年累計達NT$15,486，跨過S級年度15,000門檻）",
        "why": (
            "STORED P級（生效04-24）對應「日曆年全部交易累計」在txn631(04-23)跨過"
            "24,000；視窗制從她真正的S級生效日(2026-03-08)起算，txn631單筆僅NT$9,599，"
            "未達P級任一門檻。跟246/301/392/402同一根因樣式。"
        ),
        "superseded_tier_upgrades_ids": [191],
    },
    392: {
        "name": "李佩持",
        "group": "A",
        "before": {
            "member_tier": "P級美咖", "tier_effective_date": "2026-07-23",
            "tier_expires_date": "2027-07-23", "pending_tier": None, "pending_effective_date": None,
        },
        "after": {
            "member_tier": "S級美咖", "tier_effective_date": "2026-04-23",
            "tier_expires_date": "2027-04-23", "pending_tier": None, "pending_effective_date": None,
        },
        "source_transaction": "id=630, 2026-04-22, NT$10,298（單筆直接跨過S級8,000門檻）",
        "why": (
            "STORED P級（生效07-23）對應「日曆年全部交易累計」在txn960(07-22)跨過"
            "24,000；視窗制從她真正的S級生效日(2026-04-23)起算，txn960單筆僅"
            "NT$11,440，未達P級任一門檻。跟246/301/354/402同一根因樣式。"
        ),
        "superseded_tier_upgrades_ids": [341],
    },
    402: {
        "name": "莊惠如",
        "group": "A",
        "before": {
            "member_tier": "P級美咖", "tier_effective_date": "2026-08-11",
            "tier_expires_date": "2027-08-11", "pending_tier": None, "pending_effective_date": None,
        },
        "after": {
            "member_tier": "S級美咖", "tier_effective_date": "2026-06-09",
            "tier_expires_date": "2027-06-09", "pending_tier": None, "pending_effective_date": None,
        },
        "source_transaction": "id=817, 2026-06-08, NT$3,000（日曆年累計達NT$17,696，跨過S級年度15,000門檻）",
        "why": (
            "STORED P級（生效08-11）對應「日曆年全部交易累計」在txn1020(08-10)跨過"
            "24,000；視窗制從她真正的S級生效日(2026-06-09)起算，累計只有NT$7,698"
            "（txn840+899+1020），未達P級任一門檻。跟246/301/354/392同一根因樣式；"
            "跟336相同，她的STORED P級也沒有對應tier_upgrades紀錄。"
        ),
        "superseded_tier_upgrades_ids": [],  # 無對應紀錄可列
    },
}

FINANCIAL_TABLES = ("transactions", "coin_batches", "coin_redemptions", "point_adjustments")


def _financial_fingerprint(db) -> dict[str, tuple]:
    fp = {}
    fp["transactions"] = db.execute(
        "SELECT COUNT(*), COALESCE(SUM(final_amount),0), COALESCE(SUM(coins_earned),0), "
        "COALESCE(SUM(coins_redeemed),0) FROM transactions"
    ).fetchone()
    fp["coin_batches"] = db.execute(
        "SELECT COUNT(*), COALESCE(SUM(earned_amount),0), COALESCE(SUM(remaining_amount),0) FROM coin_batches"
    ).fetchone()
    fp["coin_redemptions"] = db.execute(
        "SELECT COUNT(*), COALESCE(SUM(amount),0) FROM coin_redemptions"
    ).fetchone()
    fp["point_adjustments"] = db.execute(
        "SELECT COUNT(*), COALESCE(SUM(points),0) FROM point_adjustments"
    ).fetchone()
    return {k: tuple(v) for k, v in fp.items()}


def _customer_state(db, cid: int) -> dict:
    row = db.execute(
        "SELECT member_tier, tier_effective_date, tier_expires_date, pending_tier, pending_effective_date "
        "FROM customers WHERE id=?",
        (cid,),
    ).fetchone()
    return dict(row) if row else {}


def _other_customers_fingerprint(db) -> tuple:
    """316 位不在 scope 內的顧客，tier 相關欄位整體指紋（Gate: non-target untouched 用）。"""
    placeholders = ",".join("?" for _ in TARGET_IDS)
    row = db.execute(
        f"SELECT COUNT(*), COALESCE(SUM(length(COALESCE(member_tier,''))),0), "
        f"COALESCE(SUM(length(COALESCE(tier_effective_date,''))),0), "
        f"COALESCE(SUM(length(COALESCE(tier_expires_date,''))),0) "
        f"FROM customers WHERE id NOT IN ({placeholders})",
        TARGET_IDS,
    ).fetchone()
    return tuple(row)


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    db = beauty.sqlite3.connect(beauty.DB_PATH)
    db.row_factory = beauty.sqlite3.Row

    print(f"TARGET_IDS={list(TARGET_IDS)}")
    print(f"MODE={'DRY_RUN' if dry_run else 'LIVE_WRITE'}")

    before_states = {cid: _customer_state(db, cid) for cid in TARGET_IDS}
    for cid in TARGET_IDS:
        if not before_states[cid]:
            print(f"REFUSED: 找不到 customer_id={cid}，整批拒絕，0 writes。")
            db.close()
            sys.exit(1)

    fp_before = _financial_fingerprint(db)
    other_fp_before = _other_customers_fingerprint(db)

    # 既有 remediation 稽核列（idempotency 判斷依據）
    existing_remediation = {
        r["customer_id"]
        for r in db.execute(
            "SELECT DISTINCT customer_id FROM tier_upgrades WHERE trigger_reason LIKE ?",
            (f"%{REMEDIATION_MARKER}%",),
        ).fetchall()
    }

    matches_before = {cid: before_states[cid] == CUSTOMERS[cid]["before"] for cid in TARGET_IDS}
    matches_after = {cid: before_states[cid] == CUSTOMERS[cid]["after"] for cid in TARGET_IDS}

    print("\n=== 逐戶 BEFORE/AFTER/PRECONDITION ===")
    for cid in TARGET_IDS:
        info = CUSTOMERS[cid]
        print(f"\n--- {cid} {info['name']}（Group {info['group']}）---")
        print(f"CURRENT (live)   : {before_states[cid]}")
        print(f"EXPECTED_BEFORE  : {info['before']}")
        print(f"EXPECTED_AFTER   : {info['after']}")
        print(f"ROOT_CAUSE       : {info['why']}")
        print(f"SOURCE_TRANSACTION: {info['source_transaction']}")
        print(f"FIELDS_TO_CHANGE : member_tier/tier_effective_date/tier_expires_date"
              f"（{info['before']['member_tier']} -> {info['after']['member_tier']}）")
        print(f"LEGACY_EVENT_RETAINED: tier_upgrades ids {info['superseded_tier_upgrades_ids']} "
              f"保留不刪，新增一筆 event_type='remediation' 稽核列標記 superseded")
        precond = "MATCH_BEFORE" if matches_before[cid] else (
            "MATCH_AFTER" if matches_after[cid] else "MISMATCH"
        )
        print(f"PRECONDITION     : {precond}")

    all_match_before = all(matches_before.values())
    all_match_after = all(matches_after.values())
    any_match_after = any(matches_after.values())
    any_match_before = any(matches_before.values())

    print("\n=== Summary ===")
    print(f"7/7 MATCH_BEFORE = {all_match_before}")
    print(f"7/7 MATCH_AFTER  = {all_match_after}")

    if all_match_after:
        print("RESULT=ALREADY_REMEDIATED / NO_WRITE")
        print(f"PRODUCTION_TIER_METADATA_WRITES=0")
        print(f"PRODUCTION_FINANCIAL_WRITES=0")
        db.close()
        return

    if not all_match_before:
        # 混合狀態，或有人完全不在 before/after 兩個已知狀態集合裡
        if any_match_after and any_match_before:
            print("RESULT=PARTIAL_OR_UNEXPECTED_STATE / REFUSE")
        else:
            print("RESULT=CAS_PRECONDITION_FAILED / REFUSE")
        print("0 writes（整批拒絕，需人工複核後才能重跑）。")
        print(f"PRODUCTION_TIER_METADATA_WRITES=0")
        print(f"PRODUCTION_FINANCIAL_WRITES=0")
        db.close()
        sys.exit(1)

    if existing_remediation & set(TARGET_IDS):
        print(
            f"REFUSED（idempotency guard）：{sorted(existing_remediation & set(TARGET_IDS))} "
            f"已有 {REMEDIATION_MARKER} 稽核列，但 BEFORE 狀態仍是舊值——狀態不一致，人工複核。"
        )
        db.close()
        sys.exit(1)

    if dry_run:
        print("\nDRY_RUN=PASS（7/7 precondition 通過，未寫入，即將 ROLLBACK）")
        print(f"PRODUCTION_TIER_METADATA_WRITES=0")
        print(f"PRODUCTION_FINANCIAL_WRITES=0")
        db.close()  # 沒有任何 execute 過 UPDATE/INSERT，close 即可，無需額外 rollback
        return

    # ── 實際寫入：單一 transaction，all-or-nothing ──────────────────────────
    now_str = datetime.now().isoformat(timespec="seconds")
    try:
        for cid in TARGET_IDS:
            info = CUSTOMERS[cid]
            after = info["after"]
            db.execute(
                "UPDATE customers SET member_tier=?, tier_effective_date=?, tier_expires_date=?, "
                "pending_tier=?, pending_effective_date=? WHERE id=?",
                (
                    after["member_tier"], after["tier_effective_date"], after["tier_expires_date"],
                    after["pending_tier"], after["pending_effective_date"], cid,
                ),
            )
            reason = (
                f"{REMEDIATION_MARKER}：修正 CURRENT_STATE tier 由 "
                f"{info['before']['member_tier']}（{info['before']['tier_effective_date']}）"
                f"改為 {after['member_tier']}（{after['tier_effective_date']}）。"
                f"根因：{info['why']} Superseded tier_upgrades ids（保留不刪）："
                f"{info['superseded_tier_upgrades_ids']}。依據：{EVIDENCE_REF}"
            )
            db.execute(
                """
                INSERT INTO tier_upgrades(
                    customer_id, upgrade_date, tier_before, tier_after,
                    trigger_txn_id, trigger_reason, gift_name,
                    gift_status, event_type, created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    cid, after["tier_effective_date"] or before_states[cid]["tier_effective_date"],
                    info["before"]["member_tier"], after["member_tier"],
                    None, reason, "", "skipped", "remediation", now_str,
                ),
            )

        # 再驗證 7/7
        after_states = {cid: _customer_state(db, cid) for cid in TARGET_IDS}
        verify_ok = all(after_states[cid] == CUSTOMERS[cid]["after"] for cid in TARGET_IDS)
        fp_after = _financial_fingerprint(db)
        other_fp_after = _other_customers_fingerprint(db)
        financial_untouched = fp_before == fp_after
        others_untouched = other_fp_before == other_fp_after

        if not (verify_ok and financial_untouched and others_untouched):
            db.rollback()
            print("REFUSED: 寫入後再驗證失敗，已 ROLLBACK，0 writes 生效。")
            print(f"verify_ok={verify_ok} financial_untouched={financial_untouched} "
                  f"others_untouched={others_untouched}")
            db.close()
            sys.exit(1)

        db.commit()
    except Exception as exc:  # noqa: BLE001 — 任何例外都要 rollback，不允許 partial commit
        db.rollback()
        print(f"REFUSED: 寫入過程發生例外，已 ROLLBACK，0 writes 生效。錯誤：{exc}")
        db.close()
        raise

    integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
    print("\n=== 完成（LIVE WRITE COMMITTED） ===")
    for cid in TARGET_IDS:
        print(f"{cid} {CUSTOMERS[cid]['name']}: AFTER={after_states[cid]}")
    print(f"PRODUCTION_TIER_METADATA_WRITES=7（7 位 customers 列 + 7 筆 tier_upgrades 稽核列）")
    print(f"PRODUCTION_FINANCIAL_WRITES=0（FINANCIAL_DIFF=0，四張財務表指紋不變）")
    print(f"INTEGRITY_CHECK={integrity}")
    db.close()


if __name__ == "__main__":
    main()
