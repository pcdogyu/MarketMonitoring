"""Aggregate open buy/sell orders across major exchanges."""

from __future__ import annotations

import asyncio
from typing import Tuple, Dict, Any

import httpx


async def _binance(symbol: str) -> Tuple[float, float]:
    """Return summed (bids, asks) from Binance futures orderbook."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://fapi.binance.com/fapi/v1/depth",
                params={"symbol": symbol, "limit": 100},
            )
            resp.raise_for_status()
            book = resp.json()
            bids = sum(float(b[1]) for b in book.get("bids", []))
            asks = sum(float(a[1]) for a in book.get("asks", []))
            return bids, asks
    except Exception:
        return 0.0, 0.0


async def _bybit(symbol: str) -> Tuple[float, float]:
    """Return summed (bids, asks) from Bybit linear swaps orderbook."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://api.bybit.com/v5/market/orderbook",
                params={"category": "linear", "symbol": symbol, "limit": 100},
            )
            resp.raise_for_status()
            data = resp.json().get("result", {}).get("list", [])
            if not data:
                return 0.0, 0.0
            item = data[0]
            bids = sum(float(b[1]) for b in item.get("b", []))
            asks = sum(float(a[1]) for a in item.get("a", []))
            return bids, asks
    except Exception:
        return 0.0, 0.0


async def _okx(symbol: str) -> Tuple[float, float]:
    """Return summed (bids, asks) from OKX swaps orderbook."""
    inst = symbol.replace("USDT", "-USDT") + "-SWAP"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://www.okx.com/api/v5/market/books",
                params={"instId": inst, "sz": 100},
            )
            resp.raise_for_status()
            data = resp.json().get("data", [])
            if not data:
                return 0.0, 0.0
            book = data[0]
            bids = sum(float(b[1]) for b in book.get("bids", []))
            asks = sum(float(a[1]) for a in book.get("asks", []))
            return bids, asks
    except Exception:
        return 0.0, 0.0


async def fetch(symbol: str) -> Dict[str, Any]:
    """Aggregate open buy/sell volumes across Binance, Bybit and OKX.

    The returned mapping includes the ``symbol`` string alongside the numeric
    ``buy`` and ``sell`` volume totals.  Using ``Dict[str, Any]`` accurately
    represents this shape and prevents type checkers or FastAPI's response
    validation from assuming all values are floats.
    """
    results = await asyncio.gather(_binance(symbol), _bybit(symbol), _okx(symbol))
    buy = sum(r[0] for r in results)
    sell = sum(r[1] for r in results)
    return {"symbol": symbol, "buy": buy, "sell": sell}
