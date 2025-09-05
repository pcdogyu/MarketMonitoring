from __future__ import annotations

"""Fetch recent liquidation events from major exchanges and aggregate them.

This module queries REST endpoints of Binance, OKX and Bybit to obtain recent
liquidation orders.  Results are normalised to a common structure and can be
aggregated into price bins suitable for visualising a liquidation map.

Only a very small subset of the exchanges' capabilities is used here – the
intent is to provide lightweight data for the front‑end heatmap.  Binance's
endpoint requires API credentials and request signing.  API key and secret are
loaded from ``settings.json`` (falling back to the ``BINANCE_API_KEY`` and
``BINANCE_API_SECRET`` environment variables).
"""

import os
import time
import json
import hmac
import hashlib
import asyncio
from pathlib import Path
from collections import defaultdict
from typing import Any, Dict, List

import httpx


def _binance_keys() -> tuple[str, str]:
    """Return API key and secret for Binance from settings or environment."""

    cfg = {}
    p = Path(__file__).resolve().parent / "settings.json"
    if p.exists():
        try:
            cfg = json.loads(p.read_text())
        except Exception:
            cfg = {}
    binance = cfg.get("exchanges", {}).get("binance", {})
    key = binance.get("api_key") or os.getenv("BINANCE_API_KEY", "")
    secret = binance.get("secret") or os.getenv("BINANCE_API_SECRET", "")
    return key, secret


async def _binance(symbol: str) -> List[Dict[str, Any]]:
    """Return outstanding liquidation orders from Binance futures."""

    key, secret = _binance_keys()
    if not key or not secret:
        return []

    url = "https://fapi.binance.com/fapi/v1/liquidationOrders"
    params = {
        "symbol": symbol,
        "limit": 1000,
        "timestamp": int(time.time() * 1000),
    }
    query = "&".join(f"{k}={params[k]}" for k in params)
    signature = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    headers = {"X-MBX-APIKEY": key}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params={**params, "signature": signature}, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            return [
                {
                    "price": float(it.get("price", 0)),
                    "qty": max(
                        float(it.get("origQty", 0)) - float(it.get("executedQty", 0)),
                        0.0,
                    ),
                    "side": it.get("side", ""),
                    "ts": int(it.get("time", 0)),
                }
                for it in data
                if it.get("status") != "FILLED"
            ]
    except Exception:
        return []


async def _okx(symbol: str) -> List[Dict[str, Any]]:
    """Return recent liquidation orders from OKX swaps."""
    inst = symbol.replace("USDT", "-USDT")
    url = "https://www.okx.com/api/v5/public/liq-order"
    params = {"instType": "SWAP", "instId": f"{inst}-SWAP"}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json().get("data", [])
            return [
                {
                    "price": float(it.get("fillPx", 0)),
                    "qty": float(it.get("fillSz", 0)),
                    "side": it.get("side", ""),
                    "ts": int(float(it.get("ts", 0))),
                }
                for it in data
            ]
    except Exception:
        return []


async def _bybit(symbol: str) -> List[Dict[str, Any]]:
    """Return recent liquidation orders from Bybit linear swaps."""
    url = "https://api.bybit.com/v5/market/liquidation"
    params = {"category": "linear", "symbol": symbol}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json().get("result", {}).get("list", [])
            return [
                {
                    "price": float(it.get("price", 0)),
                    "qty": float(it.get("qty", 0)),
                    "side": it.get("side", ""),
                    "ts": int(it.get("createdTime", 0)),
                }
                for it in data
            ]
    except Exception:
        return []


async def fetch_map(symbol: str, bin_size: float = 100.0) -> Dict[str, Any]:
    """Fetch liquidation data from all exchanges and aggregate into price bins."""
    binance, okx, bybit = await asyncio.gather(
        _binance(symbol), _okx(symbol), _bybit(symbol)
    )
    events = [("binance", e) for e in binance] + [("okx", e) for e in okx] + [
        ("bybit", e) for e in bybit
    ]
    bins: Dict[int, float] = defaultdict(float)
    for _, ev in events:
        price = float(ev.get("price", 0))
        qty = abs(float(ev.get("qty", 0)))
        if price <= 0 or qty <= 0:
            continue
        b = int(price // bin_size * bin_size)
        bins[b] += qty
    prices = sorted(bins.keys())
    volumes = [bins[p] for p in prices]
    return {"prices": prices, "volumes": volumes, "bin_size": bin_size, "ts": int(time.time())}
