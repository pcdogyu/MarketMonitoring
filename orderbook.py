"""Aggregate open buy/sell orders across major exchanges."""

from __future__ import annotations

import asyncio
import math
from typing import Tuple, Dict, Any, List

import httpx


async def _binance(symbol: str) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
    """Return (bids, asks) lists from Binance futures orderbook."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://fapi.binance.com/fapi/v1/depth",
                params={"symbol": symbol, "limit": 100},
            )
            resp.raise_for_status()
            book = resp.json()
            bids = [(float(b[0]), float(b[1])) for b in book.get("bids", [])]
            asks = [(float(a[0]), float(a[1])) for a in book.get("asks", [])]
            return bids, asks
    except Exception:
        return [], []


async def _bybit(symbol: str) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
    """Return (bids, asks) lists from Bybit linear swaps orderbook."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://api.bybit.com/v5/market/orderbook",
                params={"category": "linear", "symbol": symbol, "limit": 100},
            )
            resp.raise_for_status()
            data = resp.json().get("result", {}).get("list", [])
            if not data:
                return [], []
            item = data[0]
            bids = [(float(b[0]), float(b[1])) for b in item.get("b", [])]
            asks = [(float(a[0]), float(a[1])) for a in item.get("a", [])]
            return bids, asks
    except Exception:
        return [], []


async def _okx(symbol: str) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
    """Return (bids, asks) lists from OKX swaps orderbook."""
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
                return [], []
            book = data[0]
            bids = [(float(b[0]), float(b[1])) for b in book.get("bids", [])]
            asks = [(float(a[0]), float(a[1])) for a in book.get("asks", [])]
            return bids, asks
    except Exception:
        return [], []


async def fetch(symbol: str) -> Dict[str, Any]:
    """Aggregate open orders across Binance, Bybit and OKX into price buckets.

    Returns mapping ``{symbol, prices, buy, sell}`` where ``prices`` is a
    sorted list of price bucket levels and ``buy``/``sell`` are corresponding
    accumulated quantities.
    """
    books = await asyncio.gather(_binance(symbol), _bybit(symbol), _okx(symbol))

    # Determine interval based on symbol using SOLUSDT-style precision
    # Use mid price from first book as base for 0.01% buckets
    bids0, asks0 = books[0]
    if bids0 and asks0:
        mid = (bids0[0][0] + asks0[0][0]) / 2
    elif bids0:
        mid = bids0[0][0]
    elif asks0:
        mid = asks0[0][0]
    else:
        mid = 1.0
    exp = math.floor(math.log10(mid)) if mid > 0 else 0
    specials = {
        "XRPUSDT",
        "XLMUSDT",
        "DOGEUSDT",
        "SUIUSDT",
        "PEPEUSDT",
        "PUMPUSDT",
        "FARTCOINUSDT",
        "WLFIUSDT",
    }
    if symbol.upper() in specials:
        decimals = 4
    else:
        decimals = max(2, 2 - exp)
    interval = max(mid * 0.0001, 10 ** (-decimals))

    from collections import defaultdict
    buckets: Dict[float, Dict[str, float]] = defaultdict(lambda: {"buy": 0.0, "sell": 0.0})

    for bids, asks in books:
        for price, qty in bids:
            bucket = interval * math.floor(price / interval)
            buckets[bucket]["buy"] += qty
        for price, qty in asks:
            bucket = interval * math.floor(price / interval)
            buckets[bucket]["sell"] += qty

    prices = sorted(buckets.keys())
    buy = [buckets[p]["buy"] for p in prices]
    sell = [buckets[p]["sell"] for p in prices]
    prices = [round(p, decimals) for p in prices]
    return {"symbol": symbol, "prices": prices, "buy": buy, "sell": sell}
