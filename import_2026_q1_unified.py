#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import shutil
import sqlite3
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from app import app, get_db, get_or_create_customer, current_month_key, is_birthday_month, birthday_discount_used_this_month, customer_year_total, get_past_max_single, calc_tier, load_rules

BASE = Path('/Users/openclaw/Projects/beauty-vip-webapp')
A_XLSX = Path('/Users/openclaw/Downloads/A.xlsx')
B_XLSX = Path('/Users/openclaw/Downloads/B.xlsx')
DB = BASE / 'data' / 'beauty_vip.db'
REVIEW_MD = BASE / 'data_quality_review_2026Q1.md'
ASSUMPTIONS_MD = BASE / 'import_assumptions_2026Q1.md'

CN_NUM = {"一":1,"二":2,"三":3,"四":4,"五":5,"六":6,"七":7,"八":8,"九":9,"十":10,"十一":11,"十二":12}


def normalize_birthday(value) -> tuple[str, str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return '2000-01-01', 'missing_default'

    if isinstance(value, (datetime, date, pd.Timestamp)):
        dt = pd.to_datetime(value)
        # B.xlsx uses fake year 2026 for birthday; normalize to year-agnostic anchor year 2000
        return f"2000-{dt.month:02d}-{dt.day:02d}", 'timestamp_normalized'

    s = str(value).strip()
    if not s:
        return '2000-01-01', 'blank_default'

    # yyyy-mm-dd / yyyy/mm/dd
    for fmt in ('%Y-%m-%d', '%Y/%m/%d', '%m/%d', '%m-%d'):
        try:
            dt = datetime.strptime(s, fmt)
            if fmt in ('%m/%d', '%m-%d'):
                return f"2000-{dt.month:02d}-{dt.day:02d}", 'month_day_normalized'
            return f"2000-{dt.month:02d}-{dt.day:02d}", 'date_normalized'
        except Exception:
            pass

    # Chinese month only e.g. 3月 / 十月
    m = re.match(r'^\s*(\d{1,2})\s*月\s*$', s)
    if m:
        mm = max(1, min(12, int(m.group(1))))
        return f'2000-{mm:02d}-01', 'month_only_numeric'

    if s.endswith('月'):
        k = s.replace('月', '').strip()
        if k in CN_NUM:
            mm = CN_NUM[k]
            return f'2000-{mm:02d}-01', 'month_only_chinese'

    return '2000-01-01', f'unparsed:{s}'


def parse_txn_date(v, sheet_name: str) -> date:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        m = int(sheet_name.replace('月', '').strip())
        return date(2026, m, 1)
    try:
        dt = pd.to_datetime(v)
        if dt.year != 2026:
            return date(2026, dt.month, dt.day)
        return dt.date()
    except Exception:
        m = int(sheet_name.replace('月', '').strip())
        return date(2026, m, 1)


def clear_q1_data(db: sqlite3.Connection):
    db.execute("DELETE FROM transactions WHERE month_key IN ('2026-01','2026-02','2026-03')")
    db.execute("DELETE FROM customers WHERE id NOT IN (SELECT DISTINCT customer_id FROM transactions)")


def import_one_store(db, excel_path: Path, store_id: str, rules: dict, review_rows: list[dict]):
    xls = pd.ExcelFile(excel_path)
    imported = 0
    for sheet in ('1月', '2月', '3月'):
        if sheet not in xls.sheet_names:
            continue
        df = pd.read_excel(xls, sheet_name=sheet)
        if '姓名' not in df.columns:
            continue
        df = df.dropna(subset=['姓名'])
        for _, row in df.iterrows():
            name = str(row.get('姓名', '')).strip()
            if not name or name == 'nan':
                continue
            amount = row.get('消費金額')
            if pd.isna(amount):
                continue
            try:
                amount = float(amount)
            except Exception:
                continue
            if amount <= 0:
                continue

            bday_raw = row.get('生日')
            birthday, bflag = normalize_birthday(bday_raw)
            if bflag.startswith('unparsed') or bflag.endswith('default'):
                review_rows.append({'type':'birthday_issue','store':store_id,'name':name,'raw_birthday':str(bday_raw),'normalized':birthday,'flag':bflag})

            txn_day = parse_txn_date(row.get('日期'), sheet)
            cid = get_or_create_customer(db, name, birthday, '')
            month_key = current_month_key(txn_day)
            year_str = txn_day.strftime('%Y')

            birthday_discount_rate = float(rules['birthday_offer']['discount_rate'])
            once_per_month = bool(rules['birthday_offer'].get('once_per_month', True))
            in_birthday_month = is_birthday_month(birthday, txn_day)
            already_used = once_per_month and birthday_discount_used_this_month(db, cid, month_key)
            discount_applied = in_birthday_month and not already_used
            final_amount = amount * (1 - birthday_discount_rate) if discount_applied else amount

            year_total_so_far = customer_year_total(db, cid, year_str)
            past_max_single = get_past_max_single(db, cid)
            tier_before = calc_tier(past_max_single, year_total_so_far, rules)
            cashback = round(final_amount * tier_before.cashback_rate, 2)

            db.execute(
                '''INSERT INTO transactions(customer_id, store_id, txn_date, month_key, amount, birthday_discount_applied, final_amount, cashback, created_at)
                   VALUES(?,?,?,?,?,?,?,?,?)''',
                (cid, store_id, txn_day.isoformat(), month_key, amount, 1 if discount_applied else 0, round(final_amount,2), cashback, datetime.now().isoformat(timespec='seconds'))
            )
            imported += 1
    return imported


def generate_review(db: sqlite3.Connection, review_rows: list[dict]):
    # same-name multi-birthday candidates
    dup = db.execute('''
        SELECT name, COUNT(DISTINCT birthday) AS n_bday, GROUP_CONCAT(DISTINCT birthday) AS birthdays
        FROM customers
        GROUP BY name
        HAVING COUNT(DISTINCT birthday) > 1
        ORDER BY n_bday DESC, name
    ''').fetchall()

    lines = ["# Data Quality Review (2026 Q1)", "", f"- generated_at: {datetime.now().isoformat(timespec='seconds')}", f"- birthday_issues: {len(review_rows)}", f"- same_name_multi_birthday: {len(dup)}", "", "## 1) 生日格式問題", ""]
    for r in review_rows[:300]:
        lines.append(f"- [{r['store']}] {r['name']} | raw={r['raw_birthday']} | normalized={r['normalized']} | {r['flag']}")

    lines += ["", "## 2) 同名多生日（人工判斷，暫不合併）", ""]
    for d in dup:
        lines.append(f"- {d['name']} | birthdays={d['birthdays']}")

    REVIEW_MD.write_text('\n'.join(lines), encoding='utf-8')


def main():
    if not A_XLSX.exists() or not B_XLSX.exists():
        raise SystemExit('A.xlsx/B.xlsx not found in Downloads')

    backup = DB.with_name(f"beauty_vip.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")
    if DB.exists():
        shutil.copy2(DB, backup)

    rules = load_rules()
    review_rows = []

    with app.app_context():
        db = get_db()
        clear_q1_data(db)
        a_count = import_one_store(db, A_XLSX, 'store_a', rules, review_rows)
        b_count = import_one_store(db, B_XLSX, 'store_b', rules, review_rows)
        db.commit()
        generate_review(db, review_rows)

    ASSUMPTIONS_MD.write_text(
        "\n".join([
            "# Import Assumptions (2026 Q1)",
            "",
            "1. A.xlsx birthday month-only values are normalized to 2000-MM-01.",
            "2. B.xlsx birthday with fake year (2026-*) is normalized to 2000-MM-DD.",
            "3. same name + different birthday are kept as different customers (no auto merge).",
            "4. unparsed/missing birthday falls back to 2000-01-01 and is listed in review report.",
            f"5. import counts: store_a={a_count}, store_b={b_count}",
        ]),
        encoding='utf-8'
    )

    print(json.dumps({'ok': True, 'store_a_imported': a_count, 'store_b_imported': b_count, 'review_report': str(REVIEW_MD), 'backup_db': str(backup)}, ensure_ascii=False))


if __name__ == '__main__':
    main()
