import json
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import yfinance as yf

INSTRUMENTS = {
    "ES":  {"report_type": "tff",   "cftc_name": "E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE",        "yahoo": "ES=F"},
    "NQ":  {"report_type": "tff",   "cftc_name": "NASDAQ-100 Consolidated - CHICAGO MERCANTILE EXCHANGE","yahoo": "NQ=F"},
    "6E":  {"report_type": "tff",   "cftc_name": "EURO FX - CHICAGO MERCANTILE EXCHANGE",                "yahoo": "6E=F"},
    "BTC": {"report_type": "tff",   "cftc_name": "BITCOIN - CHICAGO MERCANTILE EXCHANGE",                "yahoo": "BTC=F"},
    "GC":  {"report_type": "disag", "cftc_name": "GOLD - COMMODITY EXCHANGE INC.",                       "yahoo": "GC=F"},
    "CL":  {"report_type": "disag", "cftc_name": "WTI-PHYSICAL - NEW YORK MERCANTILE EXCHANGE",          "yahoo": "CL=F"},
}

# TFF (Traders in Financial Futures) field mappings
TFF_CATS = {
    "dealer":   ("dealer_positions_long_all",  "dealer_positions_short_all",  "change_in_dealer_long_all",  "change_in_dealer_short_all"),
    "assetmgr": ("asset_mgr_positions_long",   "asset_mgr_positions_short",   "change_in_asset_mgr_long",   "change_in_asset_mgr_short"),
    "levfunds": ("lev_money_positions_long",    "lev_money_positions_short",   "change_in_lev_money_long",   "change_in_lev_money_short"),
    "other":    ("other_rept_positions_long",   "other_rept_positions_short",  "change_in_other_rept_long",  "change_in_other_rept_short"),
    "nonrept":  ("nonrept_positions_long_all",  "nonrept_positions_short_all", "change_in_nonrept_long_all", "change_in_nonrept_short_all"),
}

# Disaggregated field mappings
DISAG_CATS = {
    "prodmerc": ("prod_merc_positions_long",   "prod_merc_positions_short",   "change_in_prod_merc_long",   "change_in_prod_merc_short"),
    "swap":     ("swap_positions_long_all",     "swap__positions_short_all",   "change_in_swap_long_all",    "change_in_swap_short_all"),
    "mngdmoney":("m_money_positions_long_all",  "m_money_positions_short_all", "change_in_m_money_long_all", "change_in_m_money_short_all"),
    "other":    ("other_rept_positions_long",   "other_rept_positions_short",  "change_in_other_rept_long",  "change_in_other_rept_short"),
    "nonrept":  ("nonrept_positions_long_all",  "nonrept_positions_short_all", "change_in_nonrept_long_all", "change_in_nonrept_short_all"),
}

TFF_URL   = "https://publicreporting.cftc.gov/resource/gpe5-46if.json"
DISAG_URL = "https://publicreporting.cftc.gov/resource/72hh-3qpy.json"
MAX_WEEKS = 156

FRED_SERIES = {
    "vix":      {"id": "VIXCLS",       "label": "VIX",          "description": "CBOE Volatility Index"},
    "hyspread": {"id": "BAMLH0A0HYM2", "label": "HY Spread",    "description": "ICE BofA US High Yield OAS"},
    "yieldcrv": {"id": "T10Y2Y",       "label": "Yield Curve",  "description": "10Y - 2Y Treasury Spread"},
    "fedbal":   {"id": "WALCL",        "label": "Fed Balance",  "description": "Total Assets, Wednesday Level"},
}
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
FRED_OBSERVATION_START = "2018-01-01"


def to_monday_cot(date_str: str) -> str:
    """COT report dates are Tuesdays; subtract 1 day to get Monday."""
    d = datetime.strptime(date_str, "%Y-%m-%d")
    return (d - timedelta(days=1)).strftime("%Y-%m-%d")


def to_monday_price(d: datetime) -> str:
    """Return Monday of the ISO week for a given date."""
    return (d - timedelta(days=d.weekday())).strftime("%Y-%m-%d")


def _parse_rows(rows: list, cats: dict) -> list:
    result = []
    for row in rows:
        date = row["report_date_as_yyyy_mm_dd"][:10]
        oi = float(row.get("open_interest_all") or 0)
        entry = {"date": date, "time": to_monday_cot(date), "oi": oi}
        for cat, (pl, ps, cl, cs) in cats.items():
            L  = float(row.get(pl) or 0)
            S  = float(row.get(ps) or 0)
            cL = float(row.get(cl) or 0)
            cS = float(row.get(cs) or 0)
            entry[cat] = {
                "longs":  L,
                "shorts": S,
                "net":    L - S,
                "chgNet": cL - cS,
                "chgL":   cL,
                "chgS":   cS,
                "pctNet": round((L - S) / (oi or 1) * 100, 1),
            }
        result.append(entry)
    return sorted(result, key=lambda x: x["time"])


def fetch_tff(cftc_name: str) -> list:
    params = {
        "market_and_exchange_names": cftc_name,
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": MAX_WEEKS,
    }
    resp = requests.get(TFF_URL, params=params, timeout=30)
    resp.raise_for_status()
    rows = resp.json()
    if not rows:
        raise ValueError(f"No TFF data for: {cftc_name}")
    return _parse_rows(rows, TFF_CATS)


def fetch_disaggregated(cftc_name: str) -> list:
    params = {
        "market_and_exchange_names": cftc_name,
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": MAX_WEEKS,
    }
    resp = requests.get(DISAG_URL, params=params, timeout=30)
    resp.raise_for_status()
    rows = resp.json()
    if not rows:
        raise ValueError(f"No Disaggregated data for: {cftc_name}")
    return _parse_rows(rows, DISAG_CATS)


def fetch_fred(series_id: str, api_key: str) -> list:
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": FRED_OBSERVATION_START,
    }
    resp = requests.get(FRED_BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    observations = resp.json().get("observations", [])
    result = []
    for obs in observations:
        if obs.get("value") in (".", None):
            continue
        try:
            result.append({"time": obs["date"], "value": float(obs["value"])})
        except (ValueError, KeyError):
            continue
    return result


def fetch_price(yahoo_ticker: str) -> list:
    start = (datetime.now(timezone.utc) - timedelta(weeks=MAX_WEEKS + 4)).strftime("%Y-%m-%d")
    df = yf.download(yahoo_ticker, start=start, interval="1wk",
                     auto_adjust=True, progress=False)

    if df.empty:
        return []

    # yfinance >= 0.2.x returns MultiIndex columns for single tickers
    if hasattr(df.columns, "levels"):
        df.columns = df.columns.get_level_values(0)

    today = datetime.now(timezone.utc).date()
    by_week: dict = {}

    for ts, row in df.iterrows():
        date = ts.date() if hasattr(ts, "date") else datetime.strptime(str(ts)[:10], "%Y-%m-%d").date()
        if date > today:
            continue

        o = float(row["Open"])
        h = float(row["High"])
        l = float(row["Low"])
        c = float(row["Close"])
        if any(math.isnan(v) for v in (o, h, l, c)):
            continue

        monday = to_monday_price(datetime(date.year, date.month, date.day))

        if monday not in by_week:
            by_week[monday] = {
                "time":  monday,
                "open":  round(o, 4),
                "high":  round(h, 4),
                "low":   round(l, 4),
                "close": round(c, 4),
            }
        else:
            ex = by_week[monday]
            by_week[monday] = {
                "time":  monday,
                "open":  ex["open"],
                "high":  round(max(ex["high"], h), 4),
                "low":   round(min(ex["low"],  l), 4),
                "close": round(c, 4),
            }

    return sorted(by_week.values(), key=lambda x: x["time"])


def main():
    out_dir = Path(__file__).parent.parent / "data"
    out_dir.mkdir(exist_ok=True)

    errors = []
    for inst, cfg in INSTRUMENTS.items():
        rtype = cfg["report_type"]
        print(f"[{inst}] fetching COT ({rtype}) …", flush=True)
        try:
            if rtype == "tff":
                cot = fetch_tff(cfg["cftc_name"])
            else:
                cot = fetch_disaggregated(cfg["cftc_name"])
        except Exception as e:
            print(f"[{inst}] COT FAILED: {e}", file=sys.stderr)
            errors.append(inst)
            continue

        print(f"[{inst}] fetching price ({cfg['yahoo']}) …", flush=True)
        try:
            price = fetch_price(cfg["yahoo"])
        except Exception as e:
            print(f"[{inst}] price FAILED: {e}", file=sys.stderr)
            price = []

        payload = {"cot": cot, "price": price}
        path = out_dir / f"{inst}.json"
        path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        print(f"[{inst}] → {len(cot)} COT rows, {len(price)} price bars → {path.name}")

    # === FRED Macro Data ===
    fred_api_key = os.environ.get("FRED_API_KEY")
    if not fred_api_key:
        print("[WARN] FRED_API_KEY not set — skipping macro data fetch")
    else:
        print("[INFO] Fetching FRED macro data...")
        macro_data = {}
        for key, cfg in FRED_SERIES.items():
            try:
                print(f"  - {cfg['label']} ({cfg['id']})...")
                macro_data[key] = fetch_fred(cfg["id"], fred_api_key)
                print(f"    OK: {len(macro_data[key])} observations")
            except Exception as e:
                print(f"    ERROR fetching {cfg['id']}: {e}", file=sys.stderr)
                macro_data[key] = []
        macro_path = out_dir / "macro.json"
        macro_path.write_text(json.dumps(macro_data, separators=(",", ":")), encoding="utf-8")
        print(f"[INFO] Saved {macro_path.name}")

    if errors:
        print(f"\nFailed instruments: {errors}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
