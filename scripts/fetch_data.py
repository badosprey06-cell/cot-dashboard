import json
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import yfinance as yf

INSTRUMENTS = {
    "ES":  {"cftc_name": "E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE",        "yahoo": "ES=F"},
    "NQ":  {"cftc_name": "NASDAQ-100 Consolidated - CHICAGO MERCANTILE EXCHANGE","yahoo": "NQ=F"},
    "6E":  {"cftc_name": "EURO FX - CHICAGO MERCANTILE EXCHANGE",                "yahoo": "6E=F"},
    "BTC": {"cftc_name": "BITCOIN - CHICAGO MERCANTILE EXCHANGE",                "yahoo": "BTC=F"},
}

# (long_col, short_col, chg_long_col, chg_short_col)
CATS = {
    "dealer":   ("dealer_positions_long_all",  "dealer_positions_short_all",  "change_in_dealer_long_all",  "change_in_dealer_short_all"),
    "assetmgr": ("asset_mgr_positions_long",   "asset_mgr_positions_short",   "change_in_asset_mgr_long",   "change_in_asset_mgr_short"),
    "levfunds": ("lev_money_positions_long",    "lev_money_positions_short",   "change_in_lev_money_long",   "change_in_lev_money_short"),
    "other":    ("other_rept_positions_long",   "other_rept_positions_short",  "change_in_other_rept_long",  "change_in_other_rept_short"),
    "nonrept":  ("nonrept_positions_long_all",  "nonrept_positions_short_all", "change_in_nonrept_long_all", "change_in_nonrept_short_all"),
}

CFTC_URL = "https://publicreporting.cftc.gov/resource/gpe5-46if.json"
MAX_WEEKS = 156


def to_monday_cot(date_str: str) -> str:
    """COT report dates are Tuesdays; subtract 1 day to get Monday.
    Mirrors JS: toMonday(s) { d.setUTCDate(d.getUTCDate()-1) }
    """
    d = datetime.strptime(date_str, "%Y-%m-%d")
    return (d - timedelta(days=1)).strftime("%Y-%m-%d")


def to_monday_price(d: datetime) -> str:
    """Return Monday of the ISO week for a given date.
    Mirrors JS parsePrice logic: dow===0 ? -6 : 1-dow
    Python weekday(): Mon=0 … Sun=6, so subtracting weekday() gives Monday.
    """
    return (d - timedelta(days=d.weekday())).strftime("%Y-%m-%d")


def fetch_cot(cftc_name: str) -> list:
    params = {
        "market_and_exchange_names": cftc_name,
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": MAX_WEEKS,
    }
    resp = requests.get(CFTC_URL, params=params, timeout=30)
    resp.raise_for_status()
    rows = resp.json()
    if not rows:
        raise ValueError(f"No CFTC data for: {cftc_name}")

    result = []
    for row in rows:
        date = row["report_date_as_yyyy_mm_dd"][:10]
        oi = float(row.get("open_interest_all") or 0)
        entry = {"date": date, "time": to_monday_cot(date), "oi": oi}
        for cat, (pl, ps, cl, cs) in CATS.items():
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
                "open":  ex["open"],               # keep first open of the week
                "high":  round(max(ex["high"], h), 4),
                "low":   round(min(ex["low"],  l), 4),
                "close": round(c, 4),               # last close wins
            }

    return sorted(by_week.values(), key=lambda x: x["time"])


def main():
    out_dir = Path(__file__).parent.parent / "data"
    out_dir.mkdir(exist_ok=True)

    errors = []
    for inst, cfg in INSTRUMENTS.items():
        print(f"[{inst}] fetching COT …", flush=True)
        try:
            cot = fetch_cot(cfg["cftc_name"])
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

    if errors:
        print(f"\nFailed instruments: {errors}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
