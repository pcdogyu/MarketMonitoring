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


# ---------------------------------------------------------------------------
# Historical backfill via Binance (5m interval)


async def backfill_24h(symbol: str) -> Dict[str, Any]:
    """Return 24h history at 5m interval for ``symbol`` using Binance-only.

    - funding: carry-forward of fundingRate points from ``fapi/v1/fundingRate``
    - basis:   markPriceKlines - indexPriceKlines (close values)
    - oi:      futures/data/openInterestHist (5m)
    """

    interval = "5m"
    points = 288  # 24h

    async with httpx.AsyncClient(timeout=15) as client:
        # Basis via mark/index klines
        basis_x, basis = [], []
        try:
            mk = await client.get(
                "https://fapi.binance.com/fapi/v1/markPriceKlines",
                params={"symbol": symbol, "interval": interval, "limit": points},
            )
            ix = await client.get(
                "https://fapi.binance.com/fapi/v1/indexPriceKlines",
                params={"pair": symbol, "interval": interval, "limit": points},
            )
            mk.raise_for_status(); ix.raise_for_status()
            mkd = mk.json(); ixd = ix.json()
            n = min(len(mkd), len(ixd))
            for i in range(n):
                # Each kline: [openTime, open, high, low, close, ...]
                ts = int(mkd[i][0]) // 1000
                mclose = float(mkd[i][4]); iclose = float(ixd[i][4])
                basis_x.append(ts)
                basis.append(mclose - iclose)
        except Exception:
            pass

        # OI history
        oi_map = {}
        try:
            oi_resp = await client.get(
                "https://fapi.binance.com/futures/data/openInterestHist",
                params={"symbol": symbol, "period": interval, "limit": points},
            )
            oi_resp.raise_for_status()
            for it in oi_resp.json():
                ts = int(it.get("timestamp", 0)) // 1000
                oi_map[ts] = float(it.get("sumOpenInterest", 0))
        except Exception:
            pass

        # Funding history (point every 8h); carry forward into 5m grid
        funding_points = []
        try:
            import time as _t
            end = int(_t.time() * 1000)
            start = end - 24 * 3600 * 1000
            fr = await client.get(
                "https://fapi.binance.com/fapi/v1/fundingRate",
                params={"symbol": symbol, "startTime": start, "endTime": end, "limit": 1000},
            )
            fr.raise_for_status()
            for it in fr.json():
                funding_points.append((int(it.get("fundingTime", 0)) // 1000, float(it.get("fundingRate", 0))))
            funding_points.sort()
        except Exception:
            pass

    # align into 5m grid using basis_x as primary timestamps; if empty, build grid
    if not basis_x:
        import time as _t
        now = int(_t.time())
        start = now - 24 * 3600
        basis_x = list(range(start - (start % 300), now, 300))
        basis = [0.0] * len(basis_x)

    # Build series arrays
    ts_list = basis_x
    # funding carry-forward
    f_series = []
    last_f = 0.0
    idx = 0
    for ts in ts_list:
        while idx < len(funding_points) and funding_points[idx][0] <= ts:
            last_f = funding_points[idx][1]
            idx += 1
        f_series.append(last_f)

    # oi map with default 0
    oi_series = [oi_map.get(ts, 0.0) for ts in ts_list]

    # timestamps as ISO UTC
    import time as _t
    iso = [_t.strftime("%Y-%m-%dT%H:%M:%SZ", _t.gmtime(t)) for t in ts_list]

    return {"funding": f_series, "basis": basis, "oi": oi_series, "timestamps": iso}

