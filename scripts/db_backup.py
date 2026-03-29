#!/usr/bin/env python3
"""
db_backup.py — 備份本機 DB 到 data/backups/

執行方式：python3 scripts/db_backup.py
"""
import shutil, sys
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
DB_PATH = BASE / "data" / "beauty_vip.db"
BACKUP_DIR = BASE / "data" / "backups"

if not DB_PATH.exists():
    print(f"DB not found: {DB_PATH}")
    sys.exit(1)

BACKUP_DIR.mkdir(parents=True, exist_ok=True)
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
dest = BACKUP_DIR / f"beauty_vip_{ts}.db"
shutil.copy2(DB_PATH, dest)
print(f"✓ Backup: {dest}")

# Keep only last 10 backups
backups = sorted(BACKUP_DIR.glob("beauty_vip_*.db"))
for old in backups[:-10]:
    old.unlink()
    print(f"  Removed old backup: {old.name}")
