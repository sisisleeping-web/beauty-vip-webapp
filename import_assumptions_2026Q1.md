# Import Assumptions (2026 Q1)

1. A.xlsx birthday month-only values are normalized to 2000-MM-01.
2. B.xlsx birthday with fake year (2026-*) is normalized to 2000-MM-DD.
3. same name + different birthday are kept as different customers (no auto merge).
4. unparsed/missing birthday falls back to 2000-01-01 and is listed in review report.
5. import counts: store_a=126, store_b=131