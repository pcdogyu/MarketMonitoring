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

from derivatives import append_history as append_deriv_history
from derivatives import fetch_all as fetch_derivs
from holdings import refresh_holdings

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


def _load_history(path: Path) -> list[Dict[str, Any]]:
    try:
        return json.loads(path.read_text())
    except Exception:
        return []


async def _refresh_once() -> Dict[str, Any]:
    """Fetch holdings and derivatives and persist them to disk."""

    snapshot = await refresh_holdings(str(_settings_path()))
    ts = snapshot["time"]
    for sym in ("BTCUSDT", "ETHUSDT"):
        deriv = await fetch_derivs(sym)
        deriv["time"] = ts
        append_deriv_history(sym, deriv, BASE_DIR / "data")
    return snapshot


# Global state updated on each refresh
LAST_SNAPSHOT: Dict[str, Any] | None = None


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
    history = _load_history(BASE_DIR / "data" / "holdings_history.json")
    if history:
        LAST_SNAPSHOT = history[-1]
    asyncio.create_task(_refresh_loop())


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
        "</head>\n"
        "<body>\n"
        "<h1>Market Monitoring</h1>\n"
        "<div style='display:flex;gap:8px;flex-wrap:wrap'>\n"
        "  <button onclick=\"showTab('holdings')\">持仓</button>\n"
        "  <button onclick=\"showTab('predict')\">预测</button>\n"
        "  <button onclick=\"showTab('derivs')\">衍生品</button>\n"
        "  <button onclick=\"showTab('orders')\">挂单</button>\n"
        "</div>\n"
        "<div id='tab-holdings'>\n"
        "  <div id='holdings-bar' style='width:800px;height:320px;border:1px solid #ccc;margin-top:10px'></div>\n"
        "  <div id='holdings-line' style='width:800px;height:320px;border:1px solid #ccc;margin-top:10px'></div>\n"
        "</div>\n"
        "<div id='tab-predict' style='display:none'>\n"
        "  <div id='predict-json' style='margin-top:10px'></div>\n"
        "</div>\n"
        "<div id='tab-derivs' style='display:none'>\n"
        "  <div style='display:grid;grid-template-columns:repeat(auto-fit,minmax(360px,1fr));gap:10px'>\n"
        "    <div><h3>BTCUSDT Funding/Basis/OI</h3><div id='derivs-btc' style='width:100%;height:320px;border:1px solid #ccc'></div></div>\n"
        "    <div><h3>ETHUSDT Funding/Basis/OI</h3><div id='derivs-eth' style='width:100%;height:320px;border:1px solid #ccc'></div></div>\n"
        "  </div>\n"
        "</div>\n"
        "<div id='tab-orders' style='display:none'>\n"
        "  <div style='display:flex;flex-direction:column;gap:10px;margin-top:10px'>\n"
        "    <div><h3>BTCUSDT Bids/Asks</h3><div id='orders-btc' style='width:800px;height:200px;border:1px solid #ccc'></div></div>\n"
        "    <div><h3>ETHUSDT Bids/Asks</h3><div id='orders-eth' style='width:800px;height:200px;border:1px solid #ccc'></div></div>\n"
        "  </div>\n"
        "</div>\n"
        "<script>\n"
        "function showTab(tab){\n"
        "  document.getElementById('tab-holdings').style.display = (tab==='holdings')?'block':'none';\n"
        "  document.getElementById('tab-predict').style.display  = (tab==='predict')?'block':'none';\n"
        "  document.getElementById('tab-derivs').style.display   = (tab==='derivs')?'block':'none';\n"
        "  document.getElementById('tab-orders').style.display   = (tab==='orders')?'block':'none';\n"
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
        "  let dbtc = await fetch('/chart/derivs?symbol=BTCUSDT').then(r=>r.json());\n"
        "  let deth = await fetch('/chart/derivs?symbol=ETHUSDT').then(r=>r.json());\n"
        "  let btcChart = echarts.init(document.getElementById('derivs-btc'));\n"
        "  btcChart.setOption({tooltip:{trigger:'axis'},legend:{data:['funding','basis','oi']},xAxis:{type:'category',data:dbtc.timestamps},yAxis:{type:'value'},series:[{name:'funding',type:'line',data:dbtc.funding},{name:'basis',type:'line',data:dbtc.basis},{name:'oi',type:'line',data:dbtc.oi}]});\n"
        "  let ethChart = echarts.init(document.getElementById('derivs-eth'));\n"
        "  ethChart.setOption({tooltip:{trigger:'axis'},legend:{data:['funding','basis','oi']},xAxis:{type:'category',data:deth.timestamps},yAxis:{type:'value'},series:[{name:'funding',type:'line',data:deth.funding},{name:'basis',type:'line',data:deth.basis},{name:'oi',type:'line',data:deth.oi}]});\n"
        "  let ob_btc = await fetch('/chart/orders?symbol=BTCUSDT').then(r=>r.json());\n"
        "  let ob_eth = await fetch('/chart/orders?symbol=ETHUSDT').then(r=>r.json());\n"
        "  let obBtcChart = echarts.init(document.getElementById('orders-btc'));\n"
        "  obBtcChart.setOption({xAxis:{type:'category',data:['buy','sell']},yAxis:{type:'value'},series:[{data:[ob_btc.buy,ob_btc.sell],type:'bar'}]});\n"
        "  let obEthChart = echarts.init(document.getElementById('orders-eth'));\n"
        "  obEthChart.setOption({xAxis:{type:'category',data:['buy','sell']},yAxis:{type:'value'},series:[{data:[ob_eth.buy,ob_eth.sell],type:'bar'}]});\n"
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
def chart_derivs(symbol: str) -> Dict[str, Any]:
    """Return derivatives history for ``symbol``."""

    path = BASE_DIR / "data" / f"derivs_{symbol.upper()}.json"
    try:
        return json.loads(path.read_text())
    except Exception:
        return {"funding": [], "basis": [], "oi": [], "timestamps": []}


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

