"""
Creates a "Crypto Analytics" Superset dashboard from the gold Delta Lake star schema.

Steps:
  1. Authenticate
  2. Find the Trino database
  3. Create a virtual dataset (fact JOIN dim_asset → adds asset_symbol + trade_date)
  4. Create 5 charts
  5. Assemble them into a dashboard

Run:
  python3 scripts/create_superset_dashboard.py
"""

import json
import sys
import requests

SUPERSET = "http://localhost:8088"
USER, PASS = "admin", "admin"

# ── helpers ────────────────────────────────────────────────────────────────

# A single session keeps cookies alive across all calls — required for CSRF
session = requests.Session()

def login():
    r = session.post(f"{SUPERSET}/api/v1/security/login",
                     json={"username": USER, "password": PASS, "provider": "db"})
    r.raise_for_status()
    token = r.json()["access_token"]
    # Fetching the CSRF token also sets the session cookie Superset checks
    csrf_r = session.get(f"{SUPERSET}/api/v1/security/csrf_token/",
                         headers={"Authorization": f"Bearer {token}"})
    csrf_r.raise_for_status()
    csrf = csrf_r.json()["result"]
    return token, csrf

def h(token, csrf=None):
    hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    if csrf:
        hdrs["X-CSRFToken"] = csrf
    return hdrs

def post(token, csrf, path, body):
    r = session.post(f"{SUPERSET}{path}", headers=h(token, csrf), json=body)
    if r.status_code not in (200, 201):
        print(f"  ERROR {r.status_code}: {r.text[:300]}")
        sys.exit(1)
    return r.json()

def get(token, path):
    r = session.get(f"{SUPERSET}{path}", headers=h(token))
    r.raise_for_status()
    return r.json()

def put(token, csrf, path, body):
    r = session.put(f"{SUPERSET}{path}", headers=h(token, csrf), json=body)
    if r.status_code not in (200, 201):
        print(f"  ERROR {r.status_code}: {r.text[:300]}")
        sys.exit(1)
    return r.json()

# ── 1. auth ────────────────────────────────────────────────────────────────

print("Authenticating...")
token, csrf = login()

# ── 2. find Trino DB ────────────────────────────────────────────────────────

print("Finding Trino database...")
dbs = get(token, "/api/v1/database/?q=(page_size:50)")["result"]
trino_db = next((d for d in dbs if "trino" in d.get("sqlalchemy_uri", "").lower()
                 or "trino" in d.get("backend", "").lower()), None)
if not trino_db:
    print("ERROR: Trino database not found. Add it first via Settings → Database Connections.")
    sys.exit(1)
db_id = trino_db["id"]
print(f"  Found: id={db_id}  name={trino_db['database_name']}")

# ── 3. virtual dataset (fact + dim_asset → adds asset_symbol + trade_date) ─

VIRTUAL_SQL = """
SELECT
    f.trade_key,
    a.asset_symbol,
    a.asset_name,
    date_parse(cast(f.date_key AS varchar), '%Y%m%d') AS trade_date,
    f.open_price,
    f.high_price,
    f.low_price,
    f.close_price,
    f.volume,
    f.market_cap,
    f.daily_return,
    f.price_range
FROM delta.gold.fact_daily_ohlc f
JOIN delta.gold.dim_asset a ON f.asset_key = a.asset_key
"""

print("Creating virtual dataset (fact_daily_ohlc ⋈ dim_asset)...")
ds = post(token, csrf, "/api/v1/dataset/", {
    "database": db_id,
    "table_name": "crypto_fact_with_dims",
    "sql": VIRTUAL_SQL.strip(),
    "schema": "gold",
})
ds_id = ds["id"]
print(f"  Dataset id={ds_id}")

datasource = f"{ds_id}__table"

# ── 4. charts ──────────────────────────────────────────────────────────────

def make_metric(col, agg="MAX", label=None):
    return {
        "aggregate": agg,
        "column": {"column_name": col},
        "expressionType": "SIMPLE",
        "label": label or f"{agg}({col})",
    }

charts_cfg = [
    {
        "slice_name": "Close Price Over Time",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "viz_type": "echarts_timeseries_line",
            "datasource": datasource,
            "x_axis": "trade_date",
            "time_grain_sqla": "P1D",
            "metrics": [make_metric("close_price", label="Close Price")],
            "groupby": ["asset_symbol"],
            "row_limit": 50000,
            "time_range": "No filter",
            "rich_tooltip": True,
            "show_legend": True,
        },
    },
    {
        "slice_name": "Daily Return Over Time",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "viz_type": "echarts_timeseries_line",
            "datasource": datasource,
            "x_axis": "trade_date",
            "time_grain_sqla": "P1D",
            "metrics": [make_metric("daily_return", "AVG", "Avg Daily Return")],
            "groupby": ["asset_symbol"],
            "row_limit": 50000,
            "time_range": "No filter",
            "rich_tooltip": True,
            "show_legend": True,
        },
    },
    {
        "slice_name": "Volume Over Time",
        "viz_type": "echarts_timeseries_bar",
        "params": {
            "viz_type": "echarts_timeseries_bar",
            "datasource": datasource,
            "x_axis": "trade_date",
            "time_grain_sqla": "P1M",
            "metrics": [make_metric("volume", "SUM", "Total Volume")],
            "groupby": ["asset_symbol"],
            "row_limit": 10000,
            "time_range": "No filter",
            "show_legend": True,
        },
    },
    {
        "slice_name": "Price Range (High − Low) Over Time",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "viz_type": "echarts_timeseries_line",
            "datasource": datasource,
            "x_axis": "trade_date",
            "time_grain_sqla": "P1W",
            "metrics": [make_metric("price_range", "AVG", "Avg Price Range")],
            "groupby": ["asset_symbol"],
            "row_limit": 10000,
            "time_range": "No filter",
            "show_legend": True,
        },
    },
    {
        "slice_name": "Asset Summary Table",
        "viz_type": "table",
        "params": {
            "viz_type": "table",
            "datasource": datasource,
            "groupby": ["asset_symbol"],
            "metrics": [
                make_metric("close_price", "MAX", "Max Close"),
                make_metric("close_price", "MIN", "Min Close"),
                make_metric("close_price", "AVG", "Avg Close"),
                make_metric("daily_return", "AVG", "Avg Daily Return"),
                make_metric("volume", "SUM", "Total Volume"),
            ],
            "time_range": "No filter",
            "row_limit": 10,
            "table_timestamp_format": "smart_date",
        },
    },
]

chart_ids = []
for cfg in charts_cfg:
    print(f"  Creating chart: {cfg['slice_name']}...")
    resp = post(token, csrf, "/api/v1/chart/", {
        "slice_name": cfg["slice_name"],
        "viz_type": cfg["viz_type"],
        "datasource_id": ds_id,
        "datasource_type": "table",
        "params": json.dumps(cfg["params"]),
    })
    chart_ids.append(resp["id"])
    print(f"    id={resp['id']}")

# ── 5. dashboard + layout ──────────────────────────────────────────────────

print("Creating dashboard...")

def chart_block(uid, chart_id, col, row, w=6, h=50):
    return {
        "type": "CHART",
        "id": uid,
        "children": [],
        "parents": ["ROOT_ID", "GRID_ID", f"ROW-{row}"],
        "meta": {"chartId": chart_id, "width": w, "height": h, "sliceName": ""},
    }

def row_block(row_num, child_uids):
    return {
        "type": "ROW",
        "id": f"ROW-{row_num}",
        "children": child_uids,
        "parents": ["ROOT_ID", "GRID_ID"],
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
    }

c = chart_ids
layout = {
    "DASHBOARD_VERSION_KEY": "v2",
    "ROOT_ID":  {"type": "ROOT",  "id": "ROOT_ID",  "children": ["GRID_ID"]},
    "GRID_ID":  {"type": "GRID",  "id": "GRID_ID",  "children": ["ROW-1", "ROW-2", "ROW-3"],
                 "parents": ["ROOT_ID"]},
    # Row 1: Close price (full width)
    "ROW-1": row_block(1, ["CHART-0"]),
    "CHART-0": chart_block("CHART-0", c[0], 0, 1, w=12, h=55),
    # Row 2: Daily return | Volume
    "ROW-2": row_block(2, ["CHART-1", "CHART-2"]),
    "CHART-1": chart_block("CHART-1", c[1], 0, 2, w=6, h=50),
    "CHART-2": chart_block("CHART-2", c[2], 6, 2, w=6, h=50),
    # Row 3: Price range | Summary table
    "ROW-3": row_block(3, ["CHART-3", "CHART-4"]),
    "CHART-3": chart_block("CHART-3", c[3], 0, 3, w=6, h=50),
    "CHART-4": chart_block("CHART-4", c[4], 6, 3, w=6, h=50),
}

dashboard = post(token, csrf, "/api/v1/dashboard/", {
    "dashboard_title": "Crypto Analytics",
    "published": True,
    "position_json": json.dumps(layout),
    "slug": "crypto-analytics",
})
dash_id = dashboard["id"]
print(f"  Dashboard id={dash_id}")

# Link each chart to the dashboard (position_json alone is not enough in Superset 4.x)
print("Linking charts to dashboard...")
for cid in chart_ids:
    put(token, csrf, f"/api/v1/chart/{cid}", {"dashboards": [dash_id]})
    print(f"  Chart {cid} → dashboard {dash_id}")

print(f"\nDone! Open: {SUPERSET}/superset/dashboard/crypto-analytics/")
