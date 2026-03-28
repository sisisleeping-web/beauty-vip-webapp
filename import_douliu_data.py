import pandas as pd
from datetime import datetime
import sys
import os

# Ensure app can be imported
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app import (
    app, get_db, get_or_create_customer, calc_tier, is_birthday_month,
    birthday_discount_used_this_month, customer_year_total,
    get_past_max_single, current_month_key, load_rules
)

def main():
    rules = load_rules()
    excel_path = "/Users/openclaw/Downloads/A.xlsx"
    store_id = "store_a"
    
    with app.app_context():
        db = get_db()
        
        # Prevent double import for the same months
        # Let's delete existing store_a transactions for 2026-01, 2026-02, 2026-03 to be safe if rerun
        db.execute("DELETE FROM transactions WHERE store_id=? AND month_key IN ('2026-01', '2026-02', '2026-03')", (store_id,))
        
        xls = pd.ExcelFile(excel_path)
        
        target_sheets = ["1月", "2月", "3月"]
        for sheet in target_sheets:
            if sheet not in xls.sheet_names:
                continue
                
            print(f"Importing sheet {sheet}...")
            df = pd.read_excel(xls, sheet_name=sheet)
            df = df.dropna(subset=['姓名'])
            
            for _, row in df.iterrows():
                name = str(row['姓名']).strip()
                if not name or name == "nan":
                    continue
                    
                birthday_val = row['生日']
                if pd.isna(birthday_val):
                    birthday_str = "2000-01-01"
                else:
                    if isinstance(birthday_val, datetime):
                        birthday_str = birthday_val.strftime("%Y-%m-%d")
                    else:
                        birthday_str = str(birthday_val).strip()
                        # simple fix for common excel date formats
                        if " " in birthday_str:
                            birthday_str = birthday_str.split(" ")[0]
                
                date_val = row['日期']
                if pd.isna(date_val):
                    month_num = int(sheet.replace("月", ""))
                    txn_day = datetime(2026, month_num, 1).date()
                else:
                    try:
                        txn_day = pd.to_datetime(date_val).date()
                    except:
                        month_num = int(sheet.replace("月", ""))
                        txn_day = datetime(2026, month_num, 1).date()
                        
                amount_val = row['消費金額']
                if pd.isna(amount_val):
                    continue
                amount = float(amount_val)
                
                # force 2026 for this specific assignment
                if txn_day.year != 2026:
                    txn_day = txn_day.replace(year=2026)
                
                customer_id = get_or_create_customer(db, name, birthday_str, "")
                month_key = current_month_key(txn_day)
                year_str = txn_day.strftime("%Y")
                
                birthday_discount_rate = float(rules["birthday_offer"]["discount_rate"])
                once_per_month = bool(rules["birthday_offer"].get("once_per_month", True))
                in_birthday_month = is_birthday_month(birthday_str, txn_day)
                already_used = once_per_month and birthday_discount_used_this_month(db, customer_id, month_key)
                discount_applied = in_birthday_month and not already_used
                final_amount = amount * (1 - birthday_discount_rate) if discount_applied else amount
                
                year_total_so_far = customer_year_total(db, customer_id, year_str)
                past_max_single = get_past_max_single(db, customer_id)
                
                tier_before = calc_tier(past_max_single, year_total_so_far, rules)
                cashback = round(final_amount * tier_before.cashback_rate, 2)
                points = int(final_amount * tier_before.points_rate)
                
                db.execute(
                    '''
                    INSERT INTO transactions(
                        customer_id, store_id, txn_date, month_key, amount,
                        birthday_discount_applied, final_amount, cashback, created_at
                    ) VALUES(?,?,?,?,?,?,?,?,?)
                    ''',
                    (
                        customer_id,
                        store_id,
                        txn_day.isoformat(),
                        month_key,
                        amount,
                        1 if discount_applied else 0,
                        round(final_amount, 2),
                        cashback,
                        datetime.now().isoformat(timespec="seconds")
                    )
                )
        db.commit()
    print("Import completed for Douliu 2026 Jan-Mar.")

if __name__ == "__main__":
    main()
