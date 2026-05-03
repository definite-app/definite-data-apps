#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.10"
# dependencies = ["pyarrow"]
# ///
"""Generate a tiny synthetic dataset for _refined_demo.

Writes data/sample.parquet (100 rows) so CI (and local previews) can verify
the refined template + runtime actually start up against real preview data.

Schema matches what examples/_refined_demo/src/App.tsx queries:
  - id          (int)         row identifier
  - originated  (string)      ISO date used as DATE_COLUMN in App.tsx
  - name        (string)      free-form label for detail views

Dates are spread from 2020-01 through 2029-12 so the default "Last 12 months"
date filter has rows to pick up regardless of when the test runs.

Run: uv run examples/_refined_demo/gen_preview_data.py
"""

import random
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).parent
DATA_DIR = HERE / "data"
DATA_DIR.mkdir(exist_ok=True)
PARQUET_PATH = DATA_DIR / "sample.parquet"

random.seed(42)
START = date(2020, 1, 1)
END = date(2029, 12, 31)
SPAN_DAYS = (END - START).days

NAMES = [
    "Acme", "Globex", "Initech", "Umbrella", "Soylent",
    "Hooli", "Vandelay", "Pied Piper", "Stark", "Wayne",
]

ROWS = 100
ids = list(range(1, ROWS + 1))
originated = [
    (START + timedelta(days=random.randint(0, SPAN_DAYS))).isoformat()
    for _ in range(ROWS)
]
names = [random.choice(NAMES) for _ in range(ROWS)]

table = pa.table({"id": ids, "originated": originated, "name": names})
pq.write_table(table, PARQUET_PATH)
print(f"Wrote {PARQUET_PATH} ({ROWS} rows)")
