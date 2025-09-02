"""Fetch derivatives metrics from major exchanges.

The public REST APIs of Binance, Bybit and OKX are queried to obtain
perpetual swap funding rates, a simple ``basis`` (mark price minus spot price)
and open interest.  Results from all three venues are aggregated such that

* funding – average of the three exchanges
* basis   – average mark/spot difference
* oi      – sum of open interest

The module exposes :func:`fetch_all` which performs the aggregation for a
single symbol like ``"BTCUSDT"`` or ``"ETHUSDT"``.  Each call returns a
dictionary with the aggregated metrics which can be appended to a history
store by :func:`append_history`.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any, Dict, Tuple

import httpx


# Base directory to resolve data paths independent of CWD
BASE_DIR = Path(__file__).resolve().parent

async def _binance(symbol: str) -> Tuple[float, float, float]:
    """Return ``(funding, basis, oi)`` from Binance futures."""

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            prem = await client.get(
                "https://fapi.binance.com/fapi/v1/premiumIndex",
                params={"symbol": symbol},
            )
            prem.raise_for_status()
            p = prem.json()
            funding = float(p.get("lastFundingRate", 0))
            mark = float(p.get("markPrice", 0))
            spot = float(p.get("indexPrice", 0))

            oi_resp = await client.get(
                "https://fapi.binance.com/fapi/v1/openInterest",
                params={"symbol": symbol},
            )
            oi_resp.raise_for_status()
            oi = float(oi_resp.json().get("openInterest", 0))

            return funding, mark - spot, oi
    except Exception:
        return 0.0, 0.0, 0.0


async def _bybit(symbol: str) -> Tuple[float, float, float]:
    """Return ``(funding, basis, oi)`` from Bybit linear swaps."""

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            ticker = await client.get(
                "https://api.bybit.com/v5/market/tickers",
                params={"category": "linear", "symbol": symbol},
            )
            ticker.raise_for_status()
            t = ticker.json()["result"]["list"][0]
            mark = float(t.get("markPrice", 0))
            spot = float(t.get("indexPrice", 0))

            funding_resp = await client.get(
                "https://api.bybit.com/v5/market/funding/history",
                params={"symbol": symbol, "limit": 1},
            )
            funding_resp.raise_for_status()
            f = funding_resp.json()["result"]["list"][0]
            funding = float(f.get("fundingRate", 0))

            oi_resp = await client.get(
                "https://api.bybit.com/v5/market/open-interest",
                params={"category": "linear", "symbol": symbol, "interval": "5min"},
            )
            oi_resp.raise_for_status()
            oi_list = oi_resp.json()["result"]["list"]
            oi = float(oi_list[-1]["openInterest"]) if oi_list else 0.0

            return funding, mark - spot, oi
    except Exception:
        return 0.0, 0.0, 0.0


async def _okx(symbol: str) -> Tuple[float, float, float]:
    """Return ``(funding, basis, oi)`` from OKX swaps."""

    inst = symbol.replace("USDT", "-USDT")
    swap = f"{inst}-SWAP"

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            spot_t = await client.get(
                "https://www.okx.com/api/v5/market/ticker",
                params={"instId": inst},
            )
            swap_t = await client.get(
                "https://www.okx.com/api/v5/market/ticker",
                params={"instId": swap},
            )
            fr = await client.get(
                "https://www.okx.com/api/v5/public/funding-rate",
                params={"instId": swap},
            )
            oi_resp = await client.get(
                "https://www.okx.com/api/v5/public/open-interest",
                params={"instId": swap},
            )

            for r in (spot_t, swap_t, fr, oi_resp):
                r.raise_for_status()

            spot = float(spot_t.json()["data"][0]["last"])
            mark = float(swap_t.json()["data"][0]["last"])
            funding = float(fr.json()["data"][0]["fundingRate"])
            oi = float(oi_resp.json()["data"][0]["oi"])

            return funding, mark - spot, oi
    except Exception:
        return 0.0, 0.0, 0.0


async def fetch_all(symbol: str) -> Dict[str, float]:
    """Aggregate derivatives metrics for ``symbol`` across all exchanges."""

    results = await asyncio.gather(_binance(symbol), _bybit(symbol), _okx(symbol))

    fundings = [r[0] for r in results if r]
    bases = [r[1] for r in results if r]
    oi_total = sum(r[2] for r in results if r)

    return {
        "symbol": symbol,
        "funding": sum(fundings) / len(fundings) if fundings else 0.0,
        "basis": sum(bases) / len(bases) if bases else 0.0,
        "oi": oi_total,
    }


def append_history(
    symbol: str,
    data: Dict[str, Any],
    base: Path | None = None,
    max_points: int | None = None,
) -> None:
    """Append ``data`` to a derivatives history file for ``symbol``.

    ``max_points`` limits the stored history to the most recent entries.
    """

    base = base or BASE_DIR / "data"
    path = base / f"derivs_{symbol}.json"

    try:
        hist = json.loads(path.read_text())
    except Exception:
        hist = {"funding": [], "basis": [], "oi": [], "timestamps": []}

    hist["funding"].append(data["funding"])
    hist["basis"].append(data["basis"])
    hist["oi"].append(data["oi"])
    hist["timestamps"].append(data.get("time") or time.strftime("%H:%M", time.gmtime()))

    if max_points:
        for k in ("funding", "basis", "oi", "timestamps"):
            hist[k] = hist[k][-max_points:]

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(hist))

