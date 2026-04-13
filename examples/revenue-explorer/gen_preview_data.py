#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb", "pyarrow"]
# ///
"""Generate realistic demo transactions and write both a parquet sidecar
(data/transactions.parquet) and an expanded preview-data.json so the starter
data app has non-trivial data to filter.

Run: uv run gen_preview_data.py
"""

from __future__ import annotations

import json
import random
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).parent
DATA_DIR = HERE / "data"
PARQUET_PATH = DATA_DIR / "transactions.parquet"
PREVIEW_PATH = HERE / "preview-data.json"

BRANCHES = [
    ("AUS", "Austin"),
    ("BOS", "Boston"),
    ("CHI", "Chicago"),
    ("DEN", "Denver"),
    ("MIA", "Miami"),
    ("NSH", "Nashville"),
    ("SEA", "Seattle"),
    ("SFO", "San Francisco"),
]

STATUSES = ["Funded", "Pending", "Review"]
STATUS_WEIGHTS = [0.70, 0.20, 0.10]

# ~10k rows spanning 2024-01-01 through 2026-12-31 so every preset has coverage
START = date(2024, 1, 1)
END = date(2026, 12, 31)
N_ROWS = 10000


def gen_rows() -> list[dict]:
    random.seed(42)
    rows: list[dict] = []
    # Give each branch its own baseline so revenue shapes look different
    branch_bias = {code: random.uniform(0.7, 1.4) for code, _ in BRANCHES}
    span = (END - START).days
    for i in range(N_ROWS):
        day_offset = random.randint(0, span)
        d = START + timedelta(days=day_offset)
        code, name = random.choice(BRANCHES)
        status = random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
        base = random.lognormvariate(7.2, 0.55)  # ~$1000–$3000 typical, long tail
        amount = round(base * branch_bias[code], 2)
        rows.append({
            "transactionId": f"TXN-{100000 + i}",
            "transactionDate": d.isoformat(),
            "amount": amount,
            "branchName": name,
            "status": status,
        })
    rows.sort(key=lambda r: r["transactionDate"])
    return rows


def write_parquet(rows: list[dict]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=pa.schema([
        ("transactionId", pa.string()),
        ("transactionDate", pa.string()),
        ("amount", pa.float64()),
        ("branchName", pa.string()),
        ("status", pa.string()),
    ]))
    pq.write_table(table, PARQUET_PATH, compression="zstd")


def write_preview_json(rows: list[dict]) -> None:
    # Reference the parquet file; build.mjs inlines it as base64 at bundle time
    # so the runtime loads it via DuckDB WASM read_parquet, not JSON parsing.
    del rows  # unused — rows live in the parquet now
    payload = {
        "context": {
            "publicMode": False,
            "driveFile": "preview://data-apps/starter",
            "appVersion": "v2",
        },
        "datasets": {
            "transactions": {"file": "data/transactions.parquet", "format": "parquet"},
        },
        "resources": {
            "branches": [
                {"branchId": code, "branchName": name} for code, name in BRANCHES
            ],
        },
    }
    PREVIEW_PATH.write_text(json.dumps(payload, indent=2) + "\n")


def report(rows: list[dict]) -> None:
    con = duckdb.connect()
    con.register("r", pa.Table.from_pylist(rows))
    stats = con.execute(
        "SELECT COUNT(*) AS n, MIN(transactionDate) AS min_d, MAX(transactionDate) AS max_d, "
        "SUM(amount)::DOUBLE AS total, COUNT(DISTINCT branchName) AS branches FROM r"
    ).fetchone()
    print(f"rows={stats[0]}  dates={stats[1]}..{stats[2]}  branches={stats[4]}  total=${stats[3]:,.0f}")
    print(f"parquet bytes: {PARQUET_PATH.stat().st_size:,}")
    print(f"preview-data.json bytes: {PREVIEW_PATH.stat().st_size:,}")


def main() -> None:
    rows = gen_rows()
    write_parquet(rows)
    write_preview_json(rows)
    report(rows)


if __name__ == "__main__":
    main()
