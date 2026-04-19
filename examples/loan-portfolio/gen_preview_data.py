#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb", "pyarrow"]
# ///
"""Generate a synthetic consumer loan book for the Refined SaaS v2 template.

Writes data/loan_book.parquet (2,588 loans) and preview-data.json so the app
has realistic data to render against without hitting the warehouse.

Run: uv run gen_preview_data.py
"""

import json
import math
import random
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).parent
DATA_DIR = HERE / "data"
PARQUET_PATH = DATA_DIR / "loan_book.parquet"
PREVIEW_PATH = HERE / "preview-data.json"

N_ROWS = 2588
# Anchor the book at end of 2026 so the dashboard's default "Last 12 months"
# preset (relative to current date) covers a dense window of recent originations.
# The 48-month span in months_back (see gen_rows) spans back to 2023-01.
END_Y, END_M = 2026, 12

PRODUCTS = [
    ("personal", "Personal loan", 1482),
    ("consolidation", "Debt consolidation", 612),
    ("auto", "Auto-secured", 308),
    ("home", "Home improvement", 186),
]
CHANNELS = [
    ("direct_mail", "Direct mail", 784),
    ("paid_search", "Paid search", 612),
    ("affiliate", "Affiliate", 498),
    ("organic", "Organic", 406),
    ("partner", "Partner API", 288),
]
EMPLOYMENTS = [
    ("w2", "W-2 full-time", 1812),
    ("self_employed", "Self-employed", 412),
    ("contract", "1099 / contract", 218),
    ("retired", "Retired", 86),
    ("other", "Other", 60),
]
TERMS = [24, 36, 48, 60]
PURPOSES = [
    "Debt consolidation", "Home repair", "Medical", "Wedding",
    "Auto repair", "Moving", "Education", "Vacation", "Business", "Other",
]
FIRST_NAMES = list("MRJASDET KLPBCHNOVWYZFGI".replace(" ", ""))
LAST_NAMES = [
    "Chen", "Patel", "Williams", "Kowalski", "Nakamura", "Okafor", "Martinez",
    "Becker", "Thompson", "Garcia", "Rodriguez", "Hernandez", "Singh", "Ahmed",
    "Cohen", "O'Brien", "Walsh", "Andersen", "Muller", "Lopez", "Tran", "Nguyen",
    "Park", "Kim", "Wong", "Liu", "Murphy", "Foster", "Reyes", "Diaz", "Webb",
    "Holt", "Sloan", "Mendez", "Zhao", "Iyer", "Khan", "Bauer", "Roux",
    "Esposito", "Romano", "Bianchi", "Fischer", "Schmidt", "Larsen", "Berg",
    "Costa", "Silva", "Petrov", "Volkov", "Novak", "Jansen", "Yamamoto",
    "Suzuki", "Tanaka", "Mensah", "Adeyemi", "Eze", "Ndiaye", "Cisse", "Saito",
    "Kovac", "Vasquez", "Romero", "Castillo", "Cruz", "Ortiz", "Ramos",
    "Brennan", "Donnelly", "Quinn", "Sweeney", "McKay", "Lindgren", "Hansen",
]
STATE_WEIGHTS = {
    "CA": 412, "TX": 389, "NY": 276, "FL": 268, "IL": 214, "PA": 158, "OH": 142,
    "GA": 131, "AZ": 118, "WA": 104, "NC": 96, "CO": 84, "OR": 71, "MA": 68,
    "VA": 62, "MI": 58, "NJ": 54, "MN": 48, "MD": 42, "IN": 38, "WI": 34,
    "TN": 32, "MO": 28, "SC": 25, "LA": 22, "NV": 21, "KY": 18, "OK": 16,
    "AL": 15, "UT": 14, "CT": 13, "KS": 12, "AR": 11, "IA": 10, "MS": 9,
    "NE": 8, "NM": 7, "WV": 6, "ID": 6, "HI": 5, "ME": 4, "RI": 4, "NH": 4,
    "MT": 3, "DE": 3, "SD": 3, "ND": 2, "VT": 2, "AK": 2, "WY": 1,
}


def weighted_choice(rng, opts):
    total = sum(w for *_, w in opts)
    r = rng.random() * total
    for *ids, w in opts:
        r -= w
        if r <= 0:
            return ids[0]
    return opts[-1][0]


def weighted_state(rng):
    total = sum(STATE_WEIGHTS.values())
    r = rng.random() * total
    for code, w in STATE_WEIGHTS.items():
        r -= w
        if r <= 0:
            return code
    return "CA"


def status_from_fico(rng, fico, mob):
    base_delinq = max(0.01, (760 - fico) / 1000) * (1 + mob / 24)
    r = rng.random()
    if r < 0.005 + base_delinq * 0.10:
        return "charged_off"
    if r < 0.012 + base_delinq * 0.30:
        return "late_90"
    if r < 0.04 + base_delinq * 0.55:
        return "late_60"
    if r < 0.10 + base_delinq * 0.85:
        return "late_30"
    if r > 0.985:
        return "paid_off"
    return "current"


def fico_band(fico):
    if fico >= 750:
        return "A"
    if fico >= 700:
        return "B"
    if fico >= 650:
        return "C"
    if fico >= 600:
        return "D"
    return "E"


def income_band(income):
    if income < 50_000:
        return "lt50"
    if income < 75_000:
        return "50_75"
    if income < 100_000:
        return "75_100"
    if income < 150_000:
        return "100_150"
    if income < 200_000:
        return "150_200"
    return "gt200"


def dti_band(dti):
    pct = dti * 100
    if pct < 20:
        return "lt20"
    if pct < 30:
        return "20_30"
    if pct < 40:
        return "30_40"
    return "gt40"


def gen_rows():
    rng = random.Random(42)
    out = []
    for i in range(N_ROWS):
        months_back = int(48 * math.pow(rng.random(), 1.6))
        oM = END_M - months_back
        year = END_Y + (oM - 1) // 12
        month = ((oM - 1) % 12 + 12) % 12 + 1
        day = 1 + int(rng.random() * 28)
        originated = f"{year:04d}-{month:02d}-{day:02d}"
        mob = months_back

        fico_raw = 695 + (rng.random() + rng.random() + rng.random() - 1.5) * 75
        fico = max(540, min(820, round(fico_raw)))
        band = fico_band(fico)

        amount = round((5000 + math.pow(rng.random(), 1.4) * 70000) / 100) * 100
        base_rate = 14 - (fico - 600) * 0.03
        rate = max(4.5, min(19.9, base_rate + (rng.random() - 0.5) * 1.5))
        income = round((30_000 + (fico - 540) * 200 + rng.random() * 60_000) / 1000) * 1000
        dti = max(0.05, min(0.55, 0.18 + (rng.random() - 0.4) * 0.25))
        term = TERMS[int(rng.random() * len(TERMS))]

        status = status_from_fico(rng, fico, mob)

        monthly_pmt = amount * (rate / 1200) / (1 - (1 + rate / 1200) ** (-term)) if rate > 0 else amount / term
        months_paid = min(term, mob)
        balance = float(amount)
        for _ in range(months_paid):
            interest = balance * rate / 1200
            balance -= max(0, monthly_pmt - interest)
        if status == "paid_off":
            balance = 0
        elif status == "charged_off":
            balance = balance * 0.7

        product = weighted_choice(rng, [(p[0], p[2]) for p in PRODUCTS])
        channel = weighted_choice(rng, [(c[0], c[2]) for c in CHANNELS])
        employment = weighted_choice(rng, [(e[0], e[2]) for e in EMPLOYMENTS])
        purpose = PURPOSES[int(rng.random() * len(PURPOSES))]
        state = weighted_state(rng)
        autopay = rng.random() < 0.78

        if status == "current":
            last_pay = f"{END_Y:04d}-{END_M:02d}-{1 + int(rng.random()*28):02d}"
        elif status == "late_30":
            last_pay = f"{END_Y:04d}-11-{1 + int(rng.random()*28):02d}"
        elif status == "late_60":
            last_pay = f"{END_Y:04d}-10-{1 + int(rng.random()*28):02d}"
        elif status == "late_90":
            last_pay = f"{END_Y:04d}-09-{1 + int(rng.random()*28):02d}"
        elif status == "paid_off":
            last_pay = f"{END_Y:04d}-{END_M:02d}-15"
        else:
            last_pay = f"{END_Y:04d}-08-{1 + int(rng.random()*28):02d}"

        fn = FIRST_NAMES[int(rng.random() * len(FIRST_NAMES))]
        ln = LAST_NAMES[int(rng.random() * len(LAST_NAMES))]
        vintage = f"{year}-Q{(month - 1) // 3 + 1}"

        out.append({
            "loanId": f"LN-{25883 - i}",
            "borrower": f"{fn}. {ln}",
            "amount": int(amount),
            "balance": int(round(balance)),
            "fico": int(fico),
            "ficoBand": band,
            "rate": round(rate, 2),
            "term": int(term),
            "income": int(income),
            "incomeBand": income_band(income),
            "dti": round(dti, 3),
            "dtiBand": dti_band(dti),
            "status": status,
            "state": state,
            "product": product,
            "channel": channel,
            "employment": employment,
            "purpose": purpose,
            "autopay": bool(autopay),
            "payMethod": "ach_auto" if autopay else ("ach_manual" if rng.random() < 0.6 else ("card" if rng.random() < 0.7 else "check")),
            "originated": originated,
            "originatedMonth": f"{year:04d}-{month:02d}",
            "vintage": vintage,
            "mob": int(mob),
            "lastPay": last_pay,
            "collectionsFlag": "none" if status in ("current", "paid_off") else ("soft" if status == "late_30" else ("active" if status == "late_60" else "legal")),
        })
    out.sort(key=lambda r: r["originated"], reverse=True)
    return out


def write_parquet(rows):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([
        ("loanId", pa.string()),
        ("borrower", pa.string()),
        ("amount", pa.int64()),
        ("balance", pa.int64()),
        ("fico", pa.int32()),
        ("ficoBand", pa.string()),
        ("rate", pa.float64()),
        ("term", pa.int32()),
        ("income", pa.int64()),
        ("incomeBand", pa.string()),
        ("dti", pa.float64()),
        ("dtiBand", pa.string()),
        ("status", pa.string()),
        ("state", pa.string()),
        ("product", pa.string()),
        ("channel", pa.string()),
        ("employment", pa.string()),
        ("purpose", pa.string()),
        ("autopay", pa.bool_()),
        ("payMethod", pa.string()),
        ("originated", pa.string()),
        ("originatedMonth", pa.string()),
        ("vintage", pa.string()),
        ("mob", pa.int32()),
        ("lastPay", pa.string()),
        ("collectionsFlag", pa.string()),
    ])
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, PARQUET_PATH, compression="zstd")


FICO_BANDS = [
    {"band": "A", "range": "750+",    "apr": 5.8,  "defaultRate": 0.6,  "color": "#10B981"},
    {"band": "B", "range": "700-749", "apr": 7.2,  "defaultRate": 1.4,  "color": "#84CC16"},
    {"band": "C", "range": "650-699", "apr": 9.5,  "defaultRate": 3.1,  "color": "#F59E0B"},
    {"band": "D", "range": "600-649", "apr": 12.8, "defaultRate": 6.2,  "color": "#F97316"},
    {"band": "E", "range": "<600",    "apr": 17.4, "defaultRate": 11.8, "color": "#EF4444"},
]

STATUS_CATALOG = [
    {"key": "current",     "label": "Current",      "tone": "ok"},
    {"key": "late_30",     "label": "30 days late", "tone": "warn"},
    {"key": "late_60",     "label": "60 days late", "tone": "warn"},
    {"key": "late_90",     "label": "90+ late",     "tone": "bad"},
    {"key": "paid_off",    "label": "Paid off",     "tone": "muted"},
    {"key": "charged_off", "label": "Charged off",  "tone": "bad"},
]


def write_preview_json():
    payload = {
        "context": {
            "publicMode": False,
            "driveFile": "preview://data-apps/loan-portfolio",
            "appVersion": "v2",
        },
        "datasets": {
            "loans": {"file": "data/loan_book.parquet", "format": "parquet"},
        },
        "resources": {
            "riskBands": FICO_BANDS,
            "statusCatalog": STATUS_CATALOG,
        },
    }
    PREVIEW_PATH.write_text(json.dumps(payload, indent=2) + "\n")


def main():
    rows = gen_rows()
    write_parquet(rows)
    write_preview_json()
    print(f"rows={len(rows)}  parquet={PARQUET_PATH.stat().st_size:,} bytes")
    statuses = {}
    for r in rows:
        statuses[r["status"]] = statuses.get(r["status"], 0) + 1
    print(f"statuses={statuses}")


if __name__ == "__main__":
    main()
