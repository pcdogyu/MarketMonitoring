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
from pathlib import Path
from typing import Any, Dict

from fastapi import FastAPI, File, UploadFile
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
    save_price as db_save_price,
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
                "AAVEUSDT",
                "BNBUSDT",
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
            "AAVEUSDT",
            "BNBUSDT",
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
    asyncio.create_task(_refresh_loop())
    # Kick off a one-time backfill for derivatives if history is empty
    asyncio.create_task(_maybe_backfill_24h())


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
    """Serve a very small ECharts based front‑end."""

    return (
        "<!doctype html>\n"
        "<html>\n"
        "<head>\n"
        "<meta charset='utf-8'>\n"
        "<title>MarketMonitoring</title>\n"
        "<script src='https://cdn.jsdelivr.net/npm/echarts/dist/echarts.min.js'></script>\n"
        "<style>.menu{padding:4px 8px;border:1px solid #ccc;background:#fff;cursor:pointer;} .menu.active{background:#1890ff;color:#fff;}</style>\n"
        "</head>\n"
        "<body>\n"
        "<h1>Market Monitoring</h1>\n"
        "<div style='display:flex;gap:8px;flex-wrap:wrap'>\n"
        "  <button class='menu active' onclick=\"showTab('holdings',this)\">持仓</button>\n"
        "  <button class='menu' onclick=\"showTab('predict',this)\">预测</button>\n"
        "  <button class='menu' onclick=\"showTab('derivs',this)\">衍生品</button>\n"
        "  <button class='menu' onclick=\"showTab('orders',this)\">挂单</button>\n"
        "</div>\n"
        "<div id='tab-holdings'>\n"
        "  <div id='holdings-bar' style='width:800px;height:320px;border:1px solid #ccc;margin-top:10px'></div>\n"
        "  <div id='holdings-line' style='width:800px;height:320px;border:1px solid #ccc;margin-top:10px'></div>\n"
        "</div>\n"
        "<div id='tab-predict' style='display:none'>\n"
        "  <div id='predict-json' style='margin-top:10px'></div>\n"
        "</div>\n"
        "<div id='tab-derivs' style='display:none'>\n"
        "  <div style='margin:10px 0;color:#555;font-size:13px'>时间已转换为中国时区 (UTC+8)</div>\n"
        "  <div id='derivs-wrap' style='display:grid;grid-template-columns:140px 1fr 1fr 1fr;gap:10px;align-items:start'></div>\n"
        "</div>\n"
        "<div id='tab-orders' style='display:none'>\n"
        "  <div id='orders-wrap' style='display:flex;flex-direction:column;gap:10px;margin-top:10px'></div>\n"
        "</div>\n"
        "<script>\n"
        f"const derivSyms={json.dumps(_symbols())};\n"
        "function showTab(tab,btn){\n"
        "  document.getElementById('tab-holdings').style.display = (tab==='holdings')?'block':'none';\n"
        "  document.getElementById('tab-predict').style.display  = (tab==='predict')?'block':'none';\n"
        "  document.getElementById('tab-derivs').style.display   = (tab==='derivs')?'block':'none';\n"
        "  document.getElementById('tab-orders').style.display   = (tab==='orders')?'block':'none';\n"
        "  document.querySelectorAll('.menu').forEach(b=>b.classList.remove('active'));\n"
        "  if(btn) btn.classList.add('active');\n"
        "}\n"
        "async function load(){\n"
        "  let snap = await fetch('/mm/holdings').then(r=>r.json());\n"
        "  let bar = echarts.init(document.getElementById('holdings-bar'));\n"
        "  bar.setOption({title:{text:'最近快照'},xAxis:{type:'category',data:Object.keys(snap.totals)},yAxis:{type:'value'},series:[{data:Object.values(snap.totals),type:'bar'}]});\n"
        "  let hist = await fetch('/chart/holdings').then(r=>r.json());\n"
        "  let line = echarts.init(document.getElementById('holdings-line'));\n"
        "  line.setOption({tooltip:{trigger:'axis'},legend:{data:['BTC','ETH','USDT','USDC']},xAxis:{type:'category',data:hist.time},yAxis:{type:'value'},series:[{name:'BTC',type:'line',data:hist.BTC},{name:'ETH',type:'line',data:hist.ETH},{name:'USDT',type:'line',data:hist.USDT},{name:'USDC',type:'line',data:hist.USDC}]});\n"
        "  let pred1 = await fetch('/predict/BTCUSDT').then(r=>r.json());\n"
        "  let pred2 = await fetch('/predict/ETHUSDT').then(r=>r.json());\n"
        "  document.getElementById('predict-json').innerHTML='<h3>预测信号</h3><p style=\"color:#555;font-size:14px\">数据来源: data/holdings_history.json, 信号计算为 ΔBTC/ETH - 0.8·ΔUSDT - 0.4·ΔUSDC</p><pre>'+JSON.stringify([pred1,pred2],null,2)+'</pre>';\n"
        "  const toCN = (arr)=>{return (arr||[]).map(t=>{try{let d=new Date(t);if(!isNaN(d)){d=new Date(d.getTime()+8*3600*1000);let m=(d.getUTCMonth()+1).toString().padStart(2,'0');let day=d.getUTCDate().toString().padStart(2,'0');let hh=d.getUTCHours().toString().padStart(2,'0');let mm=d.getUTCMinutes().toString().padStart(2,'0');return `${m}-${day} ${hh}:${mm}`;} }catch(e){} return t;});};\n"
        "  let wrap=document.getElementById('derivs-wrap');\n"
        "  if(!wrap.hasChildNodes()){\n"
        "    // header row\n"
        "    ['Symbol','Funding','Basis','OI'].forEach((h,i)=>{let el=document.createElement('div');el.style.fontWeight='bold';el.style.margin='6px 0';el.textContent=h;wrap.appendChild(el);});\n"
        "    // rows per symbol\n"
        "    derivSyms.forEach(s=>{\n"
        "      let label=document.createElement('div');label.style.fontWeight='bold';label.style.marginTop='10px';label.textContent=s;wrap.appendChild(label);\n"
        "      ['funding','basis','oi'].forEach(kind=>{let box=document.createElement('div');box.id=`derivs-${s.toLowerCase()}-${kind}`;box.style.cssText='width:100%;height:260px;border:1px solid #ccc';wrap.appendChild(box);});\n"
        "    });\n"
        "  }\n"
        "  for(let s of derivSyms){\n"
        "    let d=await fetch(`/chart/derivs?symbol=${s}&window=24h`).then(r=>r.json());\n"
        "    let x=toCN(d.timestamps);\n"
        "    let f=(d.funding||[]).map(v=>Number(v)*100);\n"
        "    let b=(d.basis||[]).map(v=>Number(v));\n"
        "    let o=(d.oi||[]).map(v=>Number(v));\n"
        "    let mk=(id,name,data,axisFmt,color)=>{\n"
        "      let c=echarts.init(document.getElementById(id));\n"
        "      c.setOption({\n"
        "        tooltip:{trigger:'axis',formatter:(ps)=>{let p=ps[0];return p.axisValueLabel+'<br/>'+name+': '+axisFmt(p.data);}},\n"
        "        xAxis:{type:'category',data:x},\n"
        "        yAxis:{type:'value',name:name,axisLabel:{formatter:(val)=>axisFmt(val)}},\n"
        "        series:[{name,showSymbol:false,type:'line',data,lineStyle:{color},itemStyle:{color}}]\n"
        "      });\n"
        "    };\n"
        "    mk(`derivs-${s.toLowerCase()}-funding`,'Funding (%)',f,(v)=>Number(v).toFixed(2)+'%', '#5470C6');\n"
        "    mk(`derivs-${s.toLowerCase()}-basis`,'Basis (USD)',b,(v)=>'$'+Number(v).toFixed(2), '#EE6666');\n"
        "    mk(`derivs-${s.toLowerCase()}-oi`,'Open Interest',o,(v)=>Number(v).toLocaleString(), '#91CC75');\n"
        "  }\n"
        "  let ordersWrap=document.getElementById('orders-wrap');\n"
        "  if(!ordersWrap.hasChildNodes()){\n"
        "    derivSyms.forEach(s=>{let box=document.createElement('div');box.innerHTML=`<h3>${s} Bids/Asks</h3><div id='orders-${s.toLowerCase()}' style='width:800px;height:200px;border:1px solid #ccc'></div>`;ordersWrap.appendChild(box);});\n"
        "  }\n"
        "  for(let s of derivSyms){\n"
        "    let ob=await fetch(`/chart/orders?symbol=${s}`).then(r=>r.json());\n"
        "    let chart=echarts.init(document.getElementById(`orders-${s.toLowerCase()}`));\n"
        "    chart.setOption({xAxis:{type:'category',data:['buy','sell']},yAxis:{type:'value'},series:[{data:[ob.buy,ob.sell],type:'bar'}]});\n"
        "  }\n"
        "}\n"
        "setInterval(load,5000);\nload();\n</script>\n"
        "</body>\n"
        "</html>"
    )


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
    The server prefers SQLite when available, falling back to JSON history.
    """

    def _parse_window(w: str | None) -> int | None:
        if not w:
            return None
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
            return None
        return None

    secs = _parse_window(window)

    # Try DB first
    try:
        data = db_query_derivs(symbol.upper(), secs)
        if data["timestamps"]:
            return data
    except Exception:
        pass

    # Fallback to JSON file
    path = BASE_DIR / "data" / f"derivs_{symbol.upper()}.json"
    try:
        data = json.loads(path.read_text())
        if secs is None:
            return data
        # cut by seconds based on timestamps
        import time

        cutoff = time.time() - secs
        xs = []
        for t in data.get("timestamps", []):
            try:
                ts = time.strptime(t, "%Y-%m-%dT%H:%M:%SZ")
                if time.mktime(ts) >= cutoff:
                    xs.append(True)
                else:
                    xs.append(False)
            except Exception:
                xs.append(True)
        # filter arrays by xs mask
        def filt(arr):
            return [v for v, keep in zip(arr, xs) if keep]

        return {
            "funding": filt(data.get("funding", [])),
            "basis": filt(data.get("basis", [])),
            "oi": filt(data.get("oi", [])),
            "timestamps": [t for t, keep in zip(data.get("timestamps", []), xs) if keep],
        }
    except Exception:
        return {"funding": [], "basis": [], "oi": [], "timestamps": []}


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
    """Return aggregated open buy/sell volumes for ``symbol``.

    The returned payload includes the ``symbol`` itself along with ``buy`` and
    ``sell`` volume totals.  The previous type hint of ``Dict[str, float]``
    implied that *all* values would be floats which caused Pydantic's response
    validation to fail because ``symbol`` is a string.  Using ``Dict[str, Any]``
    accurately reflects the response shape and prevents a runtime validation
    error.
    """

    from orderbook import fetch

    return await fetch(symbol.upper())


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


# End of file

