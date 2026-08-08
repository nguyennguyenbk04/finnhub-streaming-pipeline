"""
Creates a "Crypto Long-Term Analytics" dashboard using the gold star schema.

Charts:
  1. Normalized Performance (base-100 from 2018-01-01) — who won?
  2. Annual Return % by Year — YoY bar chart
  3. Annualized Volatility by Year — risk evolution
  4. Monthly Seasonality — which months are historically best/worst
  5. Weekend vs Weekday Returns — dim_date.is_weekend in action
  6. Quarterly High/Low Band — long-run price range per quarter

Run:
  python3 scripts/create_longterm_dashboard.py
"""

import json
import sys
import requests

SUPERSET = "http://localhost:8088"
USER, PASS = "admin", "admin"

session = requests.Session()

def login():
    r = session.post(f"{SUPERSET}/api/v1/security/login",
                     json={"username": USER, "password": PASS, "provider": "db"})
    r.raise_for_status()
    token = r.json()["access_token"]
    csrf_r = session.get(f"{SUPERSET}/api/v1/security/csrf_token/",
                         headers={"Authorization": f"Bearer {token}"})
    csrf_r.raise_for_status()
    return token, csrf_r.json()["result"]

def h(token, csrf=None):
    hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    if csrf:
        hdrs["X-CSRFToken"] = csrf
    return hdrs

def post(token, csrf, path, body):
    r = session.post(f"{SUPERSET}{path}", headers=h(token, csrf), json=body)
    if r.status_code not in (200, 201):
        print(f"  ERROR {r.status_code}: {r.text[:400]}")
        sys.exit(1)
    return r.json()

def get(token, path):
    r = session.get(f"{SUPERSET}{path}", headers=h(token))
    r.raise_for_status()
    return r.json()

def put(token, csrf, path, body):
    r = session.put(f"{SUPERSET}{path}", headers=h(token, csrf), json=body)
    if r.status_code not in (200, 201):
        print(f"  ERROR {r.status_code}: {r.text[:400]}")
        sys.exit(1)
    return r.json()

# ── auth + db ──────────────────────────────────────────────────────────────

print("Authenticating...")
token, csrf = login()

print("Finding Trino database...")
dbs = get(token, "/api/v1/database/?q=(page_size:50)")["result"]
trino = next((d for d in dbs if "trino" in d.get("sqlalchemy_uri", "").lower()
              or "trino" in d.get("backend", "").lower()), None)
if not trino:
    print("ERROR: Trino database not found in Superset.")
    sys.exit(1)
db_id = trino["id"]
print(f"  id={db_id}  name={trino['database_name']}")

# ── virtual datasets ───────────────────────────────────────────────────────

datasets = {

    "lt_normalized": """
        WITH base_price AS (
            SELECT asset_key, close_price AS start_price
            FROM delta.gold.fact_daily_ohlc
            WHERE date_key = (SELECT MIN(date_key) FROM delta.gold.fact_daily_ohlc)
        )
        SELECT
            a.asset_symbol,
            date_parse(cast(f.date_key AS varchar), '%Y%m%d') AS trade_date,
            ROUND(f.close_price / bp.start_price * 100, 4)    AS normalized_price
        FROM delta.gold.fact_daily_ohlc f
        JOIN delta.gold.dim_asset  a  ON f.asset_key  = a.asset_key
        JOIN base_price            bp ON f.asset_key  = bp.asset_key
    """,

    "lt_annual_return": """
        WITH bounds AS (
            SELECT f.asset_key, d.year,
                MIN(f.date_key) AS first_day,
                MAX(f.date_key) AS last_day
            FROM delta.gold.fact_daily_ohlc f
            JOIN delta.gold.dim_date d ON f.date_key = d.date_key
            GROUP BY f.asset_key, d.year
        ),
        prices AS (
            SELECT b.asset_key, b.year,
                f1.close_price AS year_open,
                f2.close_price AS year_close
            FROM bounds b
            JOIN delta.gold.fact_daily_ohlc f1
                ON b.asset_key = f1.asset_key AND b.first_day = f1.date_key
            JOIN delta.gold.fact_daily_ohlc f2
                ON b.asset_key = f2.asset_key AND b.last_day  = f2.date_key
        )
        SELECT
            a.asset_symbol,
            p.year,
            ROUND((p.year_close - p.year_open) / p.year_open * 100, 2) AS annual_return_pct
        FROM prices p
        JOIN delta.gold.dim_asset a ON p.asset_key = a.asset_key
    """,

    "lt_volatility": """
        SELECT
            a.asset_symbol,
            d.year,
            ROUND(stddev_samp(f.daily_return) * sqrt(252) * 100, 2) AS annualized_vol_pct,
            ROUND(avg(f.daily_return) * 252 * 100, 2)               AS approx_annual_return_pct,
            ROUND(
                avg(f.daily_return) / NULLIF(stddev_samp(f.daily_return), 0) * sqrt(252),
                3
            ) AS sharpe_approx
        FROM delta.gold.fact_daily_ohlc f
        JOIN delta.gold.dim_asset a ON f.asset_key = a.asset_key
        JOIN delta.gold.dim_date  d ON f.date_key  = d.date_key
        GROUP BY a.asset_symbol, d.year
    """,

    "lt_seasonality": """
        SELECT
            a.asset_symbol,
            d.month,
            d.month_name,
            ROUND(avg(f.daily_return) * 100, 4) AS avg_daily_return_pct,
            count(*)                             AS trading_days
        FROM delta.gold.fact_daily_ohlc f
        JOIN delta.gold.dim_asset a ON f.asset_key = a.asset_key
        JOIN delta.gold.dim_date  d ON f.date_key  = d.date_key
        GROUP BY a.asset_symbol, d.month, d.month_name
    """,

    "lt_weekday": """
        SELECT
            a.asset_symbol,
            CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
            ROUND(avg(f.daily_return) * 100, 4)  AS avg_daily_return_pct,
            ROUND(avg(f.price_range), 2)          AS avg_price_range,
            count(*)                              AS days
        FROM delta.gold.fact_daily_ohlc f
        JOIN delta.gold.dim_asset a ON f.asset_key = a.asset_key
        JOIN delta.gold.dim_date  d ON f.date_key  = d.date_key
        GROUP BY a.asset_symbol, d.is_weekend
    """,

    "lt_market_cap_share": """
        SELECT
            a.asset_symbol,
            ROUND(AVG(f.market_cap), 2) AS avg_market_cap
        FROM delta.gold.fact_daily_ohlc f
        JOIN delta.gold.dim_asset a ON f.asset_key = a.asset_key
        WHERE f.market_cap IS NOT NULL
        GROUP BY a.asset_symbol
    """,

    "lt_quarterly_band": """
        SELECT
            a.asset_symbol,
            d.year,
            d.quarter,
            cast(d.year AS varchar) || '-Q' || cast(d.quarter AS varchar) AS year_quarter,
            ROUND(max(f.high_price),  2) AS quarterly_high,
            ROUND(min(f.low_price),   2) AS quarterly_low,
            ROUND(avg(f.close_price), 2) AS quarterly_avg_close
        FROM delta.gold.fact_daily_ohlc f
        JOIN delta.gold.dim_asset a ON f.asset_key = a.asset_key
        JOIN delta.gold.dim_date  d ON f.date_key  = d.date_key
        GROUP BY a.asset_symbol, d.year, d.quarter
    """,
}

print("Creating virtual datasets...")
ds_ids = {}
for name, sql in datasets.items():
    ds = post(token, csrf, "/api/v1/dataset/", {
        "database": db_id,
        "table_name": name,
        "sql": sql.strip(),
        "schema": "gold",
    })
    ds_ids[name] = ds["id"]
    print(f"  {name} → id={ds['id']}")

# ── charts ─────────────────────────────────────────────────────────────────

def metric(col, agg="MAX", label=None):
    return {
        "aggregate": agg,
        "column": {"column_name": col},
        "expressionType": "SIMPLE",
        "label": label or f"{agg}({col})",
    }

charts_cfg = [
    # 1. Normalized performance
    {
        "slice_name": "Normalized Performance (Base 100, Jan 2018)",
        "viz_type": "echarts_timeseries_line",
        "ds": "lt_normalized",
        "params": {
            "viz_type": "echarts_timeseries_line",
            "x_axis": "trade_date",
            "time_grain_sqla": "P1W",
            "metrics": [metric("normalized_price", "MAX", "Normalized Price")],
            "groupby": ["asset_symbol"],
            "row_limit": 50000,
            "time_range": "No filter",
            "rich_tooltip": True,
            "show_legend": True,
        },
    },
    # 2. Annual return %
    {
        "slice_name": "Annual Return % by Year",
        "viz_type": "dist_bar",
        "ds": "lt_annual_return",
        "params": {
            "viz_type": "dist_bar",
            "metrics": [metric("annual_return_pct", "MAX", "Annual Return %")],
            "groupby": ["year"],
            "columns": ["asset_symbol"],
            "row_limit": 1000,
            "time_range": "No filter",
            "show_legend": True,
        },
    },
    # 3. Annualized volatility
    {
        "slice_name": "Annualized Volatility by Year",
        "viz_type": "dist_bar",
        "ds": "lt_volatility",
        "params": {
            "viz_type": "dist_bar",
            "metrics": [metric("annualized_vol_pct", "MAX", "Annualized Volatility %")],
            "groupby": ["year"],
            "columns": ["asset_symbol"],
            "row_limit": 1000,
            "time_range": "No filter",
            "show_legend": True,
        },
    },
    # 4. Monthly seasonality — use month (int) so it sorts Jan→Dec correctly
    {
        "slice_name": "Monthly Seasonality (Avg Daily Return %)",
        "viz_type": "dist_bar",
        "ds": "lt_seasonality",
        "params": {
            "viz_type": "dist_bar",
            "metrics": [metric("avg_daily_return_pct", "MAX", "Avg Daily Return %")],
            "groupby": ["month"],
            "columns": ["asset_symbol"],
            "row_limit": 500,
            "time_range": "No filter",
            "show_legend": True,
        },
    },
    # 5. Weekend vs weekday
    {
        "slice_name": "Weekend vs Weekday Avg Daily Return %",
        "viz_type": "dist_bar",
        "ds": "lt_weekday",
        "params": {
            "viz_type": "dist_bar",
            "metrics": [metric("avg_daily_return_pct", "MAX", "Avg Daily Return %")],
            "groupby": ["day_type"],
            "columns": ["asset_symbol"],
            "row_limit": 100,
            "time_range": "No filter",
            "show_legend": True,
        },
    },
    # 6. Market cap share pie
    {
        "slice_name": "Avg Market Cap Share by Asset",
        "viz_type": "pie",
        "ds": "lt_market_cap_share",
        "params": {
            "viz_type": "pie",
            "groupby": ["asset_symbol"],
            "metric": metric("avg_market_cap", "MAX", "Avg Market Cap"),
            "time_range": "No filter",
            "row_limit": 10,
            "show_legend": True,
            "show_labels": True,
            "labels_outside": True,
            "label_type": "key_percent",
        },
    },
    # 7. Quarterly high/low band
    {
        "slice_name": "Quarterly High / Avg / Low (BTC)",
        "viz_type": "table",
        "ds": "lt_quarterly_band",
        "params": {
            "viz_type": "table",
            "groupby": ["asset_symbol", "year_quarter"],
            "metrics": [
                metric("quarterly_high",      "MAX", "Quarterly High"),
                metric("quarterly_avg_close", "MAX", "Quarterly Avg Close"),
                metric("quarterly_low",       "MAX", "Quarterly Low"),
            ],
            "row_limit": 200,
            "time_range": "No filter",
            "table_timestamp_format": "smart_date",
            "order_desc": True,
        },
    },
]

print("Creating charts...")
chart_ids = []
for cfg in charts_cfg:
    ds_id = ds_ids[cfg["ds"]]
    cfg["params"]["datasource"] = f"{ds_id}__table"
    resp = post(token, csrf, "/api/v1/chart/", {
        "slice_name": cfg["slice_name"],
        "viz_type": cfg["viz_type"],
        "datasource_id": ds_id,
        "datasource_type": "table",
        "params": json.dumps(cfg["params"]),
    })
    chart_ids.append(resp["id"])
    print(f"  [{resp['id']}] {cfg['slice_name']}")

# ── dashboard layout ───────────────────────────────────────────────────────

def row(n, children):
    return {
        "type": "ROW", "id": f"ROW-{n}",
        "children": children,
        "parents": ["ROOT_ID", "GRID_ID"],
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
    }

def chart(uid, cid, row_n, w=6, h=52):
    return {
        "type": "CHART", "id": uid, "children": [],
        "parents": ["ROOT_ID", "GRID_ID", f"ROW-{row_n}"],
        "meta": {"chartId": cid, "width": w, "height": h, "sliceName": ""},
    }

c = chart_ids
layout = {
    "DASHBOARD_VERSION_KEY": "v2",
    "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
    "GRID_ID": {"type": "GRID", "id": "GRID_ID",
                "children": ["ROW-1", "ROW-2", "ROW-3", "ROW-4"],
                "parents": ["ROOT_ID"]},
    # Row 1: Normalized performance — full width
    "ROW-1": row(1, ["C0"]),
    "C0": chart("C0", c[0], 1, w=12, h=58),
    # Row 2: Annual return | Volatility
    "ROW-2": row(2, ["C1", "C2"]),
    "C1": chart("C1", c[1], 2),
    "C2": chart("C2", c[2], 2),
    # Row 3: Seasonality | Weekend | Quarterly table
    "ROW-3": row(3, ["C3", "C4", "C5"]),
    "C3": chart("C3", c[3], 3, w=4),
    "C4": chart("C4", c[4], 3, w=4),
    "C5": chart("C5", c[5], 3, w=4),
    # Row 4: Market cap pie (centred at w=6)
    "ROW-4": row(4, ["C6"]),
    "C6": chart("C6", c[6], 4, w=6, h=52),
}

print("Creating dashboard...")
dash = post(token, csrf, "/api/v1/dashboard/", {
    "dashboard_title": "Crypto Long-Term Analytics",
    "published": True,
    "slug": "crypto-longterm",
    "position_json": json.dumps(layout),
})
dash_id = dash["id"]
print(f"  Dashboard id={dash_id}")

print("Linking charts to dashboard...")
for cid in chart_ids:
    put(token, csrf, f"/api/v1/chart/{cid}", {"dashboards": [dash_id]})
    print(f"  Chart {cid} linked")

print(f"\nDone! Open: {SUPERSET}/superset/dashboard/crypto-longterm/")
