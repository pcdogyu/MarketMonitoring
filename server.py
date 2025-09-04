"""FastAPI application serving market monitoring data.

The server exposes endpoints for refreshing and retrieving on‑chain holdings
and derivatives metrics.  Data is periodically refreshed based on the
``refresh_interval_sec`` value in ``settings.json`` (falling back to
``settings.example.json`` if the former is absent).
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import time
import hmac
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlencode

from fastapi import FastAPI, File, UploadFile, Request
from fastapi.responses import HTMLResponse, JSONResponse
import httpx

from derivatives import append_history as append_deriv_history
from derivatives import fetch_all as fetch_derivs
from derivatives import backfill_24h as derivs_backfill
from db import (
    init_db,
    save_derivs as db_save_derivs,
    save_holdings as db_save_holdings,
    query_derivs as db_query_derivs,
    query_price as db_query_price,
    save_price as db_save_price,
    save_cex_holdings as db_save_cex_holdings,
    prune_old_data,
)
from holdings import refresh_holdings
from exchange_holdings import refresh_exchange_holdings

app = FastAPI()


# Base directory of the project – ensures paths work regardless of CWD
BASE_DIR = Path(__file__).resolve().parent

# ---------------------------------------------------------------------------
# Utility helpers


def _settings_path() -> Path:
    p = BASE_DIR / "settings.json"
    if not p.exists():
        p = BASE_DIR / "settings.example.json"
    return p


def _symbols() -> list[str]:
    """Return list of monitored trading pairs from settings."""

    try:
        cfg = json.loads(_settings_path().read_text())
        syms = cfg.get(
            "symbols",
            [
                "BTCUSDT",
                "ETHUSDT",
                "SOLUSDT",
                "XLMUSDT",
                "XRPUSDT",
                "DOGEUSDT",
                "SUIUSDT",
                "PEPEUSDT",
                "1000PEPEUSDT",
                "AAVEUSDT",
                "BNBUSDT",
                "PUMPUSDT",
                "FARTCOINUSDT",
                "WLFIUSDT",
            ],
        )
        return [str(s).upper() for s in syms]
    except Exception:
        return [
            "BTCUSDT",
            "ETHUSDT",
            "SOLUSDT",
            "XLMUSDT",
            "XRPUSDT",
            "DOGEUSDT",
            "SUIUSDT",
            "PEPEUSDT",
            "1000PEPEUSDT",
            "AAVEUSDT",
            "BNBUSDT",
            "PUMPUSDT",
            "FARTCOINUSDT",
            "WLFIUSDT",
        ]


def _load_history(path: Path) -> list[Dict[str, Any]]:
    try:
        return json.loads(path.read_text())
    except Exception:
        return []


async def fetch_price(symbol: str) -> float:
    """Fetch latest spot price for ``symbol`` via Binance."""

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://api.binance.com/api/v3/ticker/price",
                params={"symbol": symbol},
            )
            resp.raise_for_status()
            return float(resp.json().get("price", 0))
    except Exception:
        return 0.0


async def _refresh_once() -> Dict[str, Any]:
    """Fetch holdings and derivatives and persist them to disk."""

    snapshot = await refresh_holdings(str(_settings_path()))
    ts = snapshot["time"]
    # persist snapshot to DB as well
    try:
        db_save_holdings(snapshot)
    except Exception:
        pass
    # also refresh centralised exchange holdings
    try:
        global LAST_CEX_SNAPSHOT
        LAST_CEX_SNAPSHOT = await refresh_exchange_holdings(str(_settings_path()))
        try:
            db_save_cex_holdings(LAST_CEX_SNAPSHOT)
        except Exception:
            pass
    except Exception:
        LAST_CEX_SNAPSHOT = {"time": ts, "exchanges": {}}
    cfg = json.loads(_settings_path().read_text())
    interval = int(cfg.get("refresh_interval_sec", 300))
    symbols = _symbols()
    max_points = int(12 * 3600 / interval)
    for sym in symbols:
        deriv = await fetch_derivs(sym)
        deriv["time"] = ts
        # write to DB and JSON history for backward compatibility
        try:
            db_save_derivs(sym, deriv)
        except Exception:
            pass
        append_deriv_history(sym, deriv, BASE_DIR / "data", max_points=max_points)

    # fetch and persist current spot prices
    price_tasks = [fetch_price(s) for s in symbols]
    prices = await asyncio.gather(*price_tasks)
    for sym, price in zip(symbols, prices):
        try:
            db_save_price(sym, ts, price)
        except Exception:
            pass
    return snapshot


# Global state updated on each refresh
LAST_SNAPSHOT: Dict[str, Any] | None = None
LAST_CEX_SNAPSHOT: Dict[str, Any] | None = None


async def _refresh_loop() -> None:
    """Background task that periodically refreshes data."""

    cfg = json.loads(_settings_path().read_text())
    interval = int(cfg.get("refresh_interval_sec", 300))
    while True:
        global LAST_SNAPSHOT
        LAST_SNAPSHOT = await _refresh_once()
        await asyncio.sleep(interval)


async def _cleanup_loop() -> None:
    """Periodically prune old rows from the database."""

    while True:
        now = datetime.now(timezone.utc)
        next_noon = now.replace(hour=12, minute=0, second=0, microsecond=0)
        next_midnight = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        if now < next_noon:
            next_run = next_noon
        elif now < next_midnight:
            next_run = next_midnight
        else:
            next_run = (now + timedelta(days=1)).replace(
                hour=12, minute=0, second=0, microsecond=0
            )
        await asyncio.sleep((next_run - now).total_seconds())
        try:
            prune_old_data()
        except Exception:
            pass


@app.on_event("startup")
async def _startup() -> None:
    global LAST_SNAPSHOT
    # ensure local SQLite exists
    try:
        init_db()
    except Exception:
        pass
    history = _load_history(BASE_DIR / "data" / "holdings_history.json")
    if history:
        LAST_SNAPSHOT = history[-1]
    # Perform a 24h derivatives backfill before starting the periodic refresh.
    #
    # Previously the refresh loop was spawned before the backfill which meant
    # that the first `_refresh_once` call created non-empty history files.  The
    # subsequent backfill then skipped because it detected existing data,
    # leaving charts with only a couple of points.  By awaiting the backfill
    # here we guarantee that the historical data is populated on startup,
    # allowing the front‑end charts to immediately display a full 24‑hour
    # window.
    try:
        await _maybe_backfill_24h()
    except Exception:
        # Ignore backfill errors – regular refresh will still populate data
        pass

    # Start background refresh loop after backfill completes
    asyncio.create_task(_refresh_loop())
    asyncio.create_task(_cleanup_loop())


async def _maybe_backfill_24h() -> None:
    """Backfill last 24h derivatives data if local store is empty.

    Uses Binance endpoints via derivatives.backfill_24h. Runs once at startup.
    """
    symbols = _symbols()
    for s in symbols:
        path = BASE_DIR / "data" / f"derivs_{s}.json"
        needs = True
        try:
            data = json.loads(path.read_text())
            if data.get("timestamps"):
                needs = False
        except Exception:
            needs = True
        if not needs:
            continue
        try:
            series = await derivs_backfill(s)
            for t, f, b, o in zip(series["timestamps"], series["funding"], series["basis"], series["oi"]):
                payload = {"funding": f, "basis": b, "oi": o, "time": t}
                try:
                    db_save_derivs(s, payload)
                except Exception:
                    pass
                append_deriv_history(s, {**payload, "symbol": s}, BASE_DIR / "data")
        except Exception:
            # ignore backfill errors; regular refresh will still populate gradually
            pass


# ---------------------------------------------------------------------------
# HTML front‑end


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    """Serve a very small ECharts based front-end."""
    # Explicitly decode index.html as UTF-8 so that the application works on
    # systems whose default locale uses a different encoding (e.g. GBK on
    # Windows). Without this, reading the file with ``Path.read_text`` can
    # raise ``UnicodeDecodeError`` when the HTML contains non-ASCII characters.
    tpl = (BASE_DIR / "index.html").read_text(encoding="utf-8")
    return tpl.replace("__SYMS__", json.dumps(_symbols()))

# ---------------------------------------------------------------------------
# REST endpoints


@app.post("/mm/refresh")
async def refresh_endpoint() -> Dict[str, Any]:
    """Trigger an immediate refresh of data."""

    global LAST_SNAPSHOT
    LAST_SNAPSHOT = await _refresh_once()
    return LAST_SNAPSHOT


@app.get("/mm/holdings")
def mm_holdings() -> Dict[str, Any]:
    """Return the latest holdings snapshot."""

    return LAST_SNAPSHOT or {"time": None, "totals": {}}


@app.get("/cex/holdings")
def cex_holdings() -> Dict[str, Any]:
    """Return the latest centralised exchange holdings snapshot."""

    if LAST_CEX_SNAPSHOT:
        return LAST_CEX_SNAPSHOT
    hist = _load_history(BASE_DIR / "data" / "exchange_holdings_history.json")
    return hist[-1] if hist else {"time": None, "exchanges": {}}


@app.get("/chart/holdings")
def chart_holdings() -> Dict[str, Any]:
    """Return holdings history as time series."""

    hist = _load_history(BASE_DIR / "data" / "holdings_history.json")
    return {
        "time": [h["time"] for h in hist],
        "BTC": [h["totals"].get("BTC", 0) for h in hist],
        "ETH": [h["totals"].get("ETH", 0) for h in hist],
        "USDT": [h["totals"].get("USDT", 0) for h in hist],
        "USDC": [h["totals"].get("USDC", 0) for h in hist],
    }


@app.get("/chart/cex_holdings")
def chart_cex_holdings() -> Any:
    """Return centralised exchange holdings history."""

    return _load_history(BASE_DIR / "data" / "exchange_holdings_history.json")


@app.get("/predict/{symbol}")
def predict(symbol: str) -> Dict[str, Any]:
    """Return differential prediction score for ``symbol``.

    The score is based on the change in the target asset minus a weighted
    change in stable coins: ``delta_target - 0.8*delta_USDT - 0.4*delta_USDC``.
    A positive score yields a ``bullish`` signal while a negative score is
    ``bearish``.
    """

    hist = _load_history(BASE_DIR / "data" / "holdings_history.json")
    if len(hist) < 2:
        return {
            "symbol": symbol.upper(),
            "score": 0.0,
            "signal": "neutral",
            "note": "insufficient history",
        }

    last, prev = hist[-1], hist[-2]
    sym = symbol.upper()
    target = "BTC" if sym == "BTCUSDT" else "ETH"

    d_target = last["totals"].get(target, 0) - prev["totals"].get(target, 0)
    d_usdt = last["totals"].get("USDT", 0) - prev["totals"].get("USDT", 0)
    d_usdc = last["totals"].get("USDC", 0) - prev["totals"].get("USDC", 0)
    score = d_target - 0.8 * d_usdt - 0.4 * d_usdc

    sig = "bullish" if score > 0 else "bearish" if score < 0 else "neutral"
    return {"symbol": sym, "score": score, "signal": sig, "source": "data/holdings_history.json"}


@app.get("/chart/derivs")
def chart_derivs(symbol: str, window: str | None = None) -> Dict[str, Any]:
    """Return derivatives history for ``symbol``.

    Optional query ``window`` restricts the time range (e.g. "5m", "1h").
    If omitted, the server returns the last 24 hours.  The server prefers
    SQLite when available, falling back to JSON history.
    """

    def _parse_window(w: str | None) -> int:
        if not w:
            return 24 * 3600
        try:
            unit = w[-1].lower()
            num = int(w[:-1])
            if unit == "m":
                return num * 60
            if unit == "h":
                return num * 3600
            if unit == "d":
                return num * 86400
        except Exception:
            pass
        return 24 * 3600

    secs = _parse_window(window)
    # Load price history from DB and map by timestamp
    price_map: Dict[str, float] = {}
    try:
        p = db_query_price(symbol.upper(), secs)
        price_map = dict(zip(p.get("timestamps", []), p.get("price", [])))
    except Exception:
        price_map = {}

    # Try DB first
    try:
        data = db_query_derivs(symbol.upper(), secs)
        if data["timestamps"]:
            data["price"] = [price_map.get(t) for t in data["timestamps"]]
            return data
    except Exception:
        pass

    # Fallback to JSON file
    path = BASE_DIR / "data" / f"derivs_{symbol.upper()}.json"
    try:
        data = json.loads(path.read_text())
        # cut by seconds based on timestamps
        import time
        import calendar

        # Use UTC for parsing ISO timestamps (which are in Zulu/UTC).
        # Previously ``time.mktime`` treated the parsed struct_time as local
        # time, introducing a timezone offset (e.g. -8h in CN) and causing
        # the last 24h window to exclude recent points.  ``calendar.timegm``
        # interprets the struct as UTC and returns the correct epoch seconds.
        cutoff = time.time() - secs
        xs = []
        for t in data.get("timestamps", []):
            try:
                ts = time.strptime(t, "%Y-%m-%dT%H:%M:%SZ")
                if calendar.timegm(ts) >= cutoff:
                    xs.append(True)
                else:
                    xs.append(False)
            except Exception:
                # If parsing fails, keep the point rather than dropping it
                xs.append(True)
        # filter arrays by xs mask
        def filt(arr):
            return [v for v, keep in zip(arr, xs) if keep]

        result = {
            "funding": filt(data.get("funding", [])),
            "basis": filt(data.get("basis", [])),
            "oi": filt(data.get("oi", [])),
            "timestamps": [t for t, keep in zip(data.get("timestamps", []), xs) if keep],
        }
        result["price"] = [price_map.get(t) for t in result["timestamps"]]
        return result
    except Exception:
        return {"funding": [], "basis": [], "oi": [], "price": [], "timestamps": []}


@app.post("/backfill/derivs")
async def backfill_derivs() -> Dict[str, Any]:
    """Force a 24h backfill for all symbols.

    Returns a map of symbol->inserted points.
    """
    symbols = _symbols()
    results: Dict[str, int] = {}
    for s in symbols:
        try:
            series = await derivs_backfill(s)
            n = 0
            for t, f, b, o in zip(series["timestamps"], series["funding"], series["basis"], series["oi"]):
                payload = {"funding": f, "basis": b, "oi": o, "time": t}
                try:
                    db_save_derivs(s, payload)
                except Exception:
                    pass
                append_deriv_history(s, {**payload, "symbol": s}, BASE_DIR / "data")
                n += 1
            results[s] = n
        except Exception:
            results[s] = 0
    return {"status": "ok", "inserted_points": results}


@app.get("/chart/orders")
async def chart_orders(symbol: str) -> Dict[str, Any]:
    """Return aggregated open orders for ``symbol`` binned by price.

    The response contains ``symbol`` together with arrays ``prices``, ``buy``
    and ``sell`` representing the total bid/ask quantities at each price
    bucket.  ``Dict[str, Any]`` is used because the values are heterogeneous.
    """

    from orderbook import fetch

    return await fetch(symbol.upper())


@app.get("/chart/cancels")
async def chart_cancels(symbol: str) -> Dict[str, Any]:
    """Return number of cancelled orders for ``symbol`` on Binance."""

    sym = symbol.upper()
    try:
        cfg = json.loads(_settings_path().read_text())
        ex_cfg = cfg.get("exchanges", {}).get("binance")
        if not ex_cfg:
            raise KeyError("missing binance config")
        ts = int(time.time() * 1000)
        params = {"symbol": sym, "timestamp": ts}
        query = urlencode(params)
        sig = hmac.new(ex_cfg["secret"].encode(), query.encode(), hashlib.sha256).hexdigest()
        headers = {"X-MBX-APIKEY": ex_cfg["api_key"]}
        url = ex_cfg.get("api_base", "https://api.binance.com") + "/api/v3/allOrders"
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params={**params, "signature": sig}, headers=headers)
            resp.raise_for_status()
            data = resp.json()
        count = sum(1 for o in data if o.get("status") == "CANCELED")
    except Exception:
        count = 0
    return {"symbol": sym, "count": count}


@app.get("/chart/trades")
async def chart_trades(symbol: str) -> Dict[str, Any]:
    """Return price series and 70% volume range for ``symbol`` since UTC+8 day start."""

    sym = symbol.upper()
    tz = timezone(timedelta(hours=8))
    now = datetime.now(tz)
    start = datetime(now.year, now.month, now.day, tzinfo=tz)
    start_ms = int(start.timestamp() * 1000)
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": sym, "interval": "5m", "startTime": start_ms, "limit": 1000}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
    except Exception:
        data = []

    times = [int(k[0]) for k in data]
    closes = [float(k[4]) for k in data]
    volumes = [float(k[5]) for k in data]
    total = sum(volumes)
    lower = upper = 0.0
    if total > 0:
        pairs = sorted(zip(closes, volumes))
        cum = 0.0
        lower_bound = total * 0.15
        upper_bound = total * 0.85
        l = None
        u = None
        for price, vol in pairs:
            cum += vol
            if l is None and cum >= lower_bound:
                l = price
            if u is None and cum >= upper_bound:
                u = price
                break
        lower = l if l is not None else pairs[0][0]
        upper = u if u is not None else pairs[-1][0]

    return {
        "symbol": sym,
        "times": times,
        "prices": closes,
        "volumes": volumes,
        "lower": lower,
        "upper": upper,
    }


@app.post("/labels/import")
async def labels_import(file: UploadFile = File(...)) -> Any:
    """Import wallet labels from CSV or JSON and write back to settings."""

    data = await file.read()
    try:
        if file.filename.lower().endswith(".json"):
            labels = json.loads(data.decode())
        else:
            reader = csv.reader(io.StringIO(data.decode()))
            labels = [row for row in reader if row]
    except Exception:  # pragma: no cover - invalid input
        return JSONResponse({"error": "invalid format"}, status_code=400)

    _settings_path().write_text(json.dumps(labels, indent=2))
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Settings endpoints


@app.get("/settings")
def get_settings() -> Dict[str, Any]:
    """Return current system settings."""

    try:
        return json.loads(_settings_path().read_text())
    except Exception:
        return {}


@app.post("/settings")
async def update_settings(req: Request) -> Dict[str, str]:
    """Overwrite settings.json with provided configuration."""

    data = await req.json()
    _settings_path().write_text(json.dumps(data, indent=2))
    return {"status": "ok"}


# End of file

