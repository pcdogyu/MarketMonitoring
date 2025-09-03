"""Fetch account balances from centralised exchanges.

The module provides a :func:`refresh_exchange_holdings` coroutine which
reads API credentials from ``settings.json`` and returns per‑exchange
balances for a set of common assets.  Each exchange implementation is kept
lightweight and only depends on :mod:`httpx` from the standard
requirements.

Only a very small subset of each exchange's REST API is used – enough to
retrieve spot wallet balances.  All network errors are silenced and result
in ``0`` balances so the rest of the application can continue to operate
without hard failures.
"""

from __future__ import annotations

import asyncio
import base64
import hmac
import hashlib
import json
import time
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlencode

import httpx

COINS = ("BTC", "ETH", "USDT", "USD", "USDC")
BASE_DIR = Path(__file__).resolve().parent
HISTORY_PATH = BASE_DIR / "data" / "exchange_holdings_history.json"
SAMPLE_PATH = BASE_DIR / "data" / "exchange_holdings_sample.json"


def _append_history(snapshot: Dict[str, Any], path: Path) -> None:
    """Append ``snapshot`` to JSON history at ``path``."""

    try:
        history = json.loads(path.read_text())
    except Exception:
        history = []

    history.append(snapshot)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(history))


async def _binance_balances(client: httpx.AsyncClient, cfg: Dict[str, str]) -> Dict[str, float]:
    """Return balances from Binance spot account."""

    try:
        ts = int(time.time() * 1000)
        params = {"timestamp": ts}
        query = urlencode(params)
        sig = hmac.new(cfg["secret"].encode(), query.encode(), hashlib.sha256).hexdigest()
        headers = {"X-MBX-APIKEY": cfg["api_key"]}
        url = cfg.get("api_base", "https://api.binance.com") + "/api/v3/account"
        resp = await client.get(url, params={**params, "signature": sig}, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        all_bal = {
            b["asset"].upper(): float(b.get("free", 0)) + float(b.get("locked", 0))
            for b in data.get("balances", [])
        }
        return {c: all_bal.get(c, 0.0) for c in COINS}
    except Exception:
        return {c: 0.0 for c in COINS}


async def _bybit_balances(client: httpx.AsyncClient, cfg: Dict[str, str]) -> Dict[str, float]:
    """Return balances from Bybit unified account."""

    try:
        ts = str(int(time.time() * 1000))
        recv_window = "5000"
        params = {"accountType": "UNIFIED"}
        param_str = urlencode(params)
        sign_str = ts + cfg["api_key"] + recv_window + param_str
        sig = hmac.new(cfg["secret"].encode(), sign_str.encode(), hashlib.sha256).hexdigest()
        headers = {
            "X-BAPI-API-KEY": cfg["api_key"],
            "X-BAPI-SIGN": sig,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": recv_window,
        }
        url = cfg.get("api_base", "https://api.bybit.com") + "/v5/account/wallet-balance"
        resp = await client.get(url, params=params, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        coins = {}
        for item in data.get("result", {}).get("list", []):
            for coin in item.get("coin", []):
                coins[coin.get("coin", "").upper()] = float(coin.get("walletBalance", 0))
        return {c: coins.get(c, 0.0) for c in COINS}
    except Exception:
        return {c: 0.0 for c in COINS}


async def _okx_balances(client: httpx.AsyncClient, cfg: Dict[str, str]) -> Dict[str, float]:
    """Return balances from OKX account."""

    try:
        ts = str(time.time())
        params = {"ccy": ",".join(COINS)}
        path = "/api/v5/account/balance"
        query = urlencode(params)
        prehash = ts + "GET" + path + ("?" + query if query else "")
        sig = base64.b64encode(
            hmac.new(cfg["secret"].encode(), prehash.encode(), hashlib.sha256).digest()
        ).decode()
        headers = {
            "OK-ACCESS-KEY": cfg["api_key"],
            "OK-ACCESS-SIGN": sig,
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-PASSPHRASE": cfg.get("passphrase", ""),
        }
        url = cfg.get("api_base", "https://www.okx.com") + path
        resp = await client.get(url, params=params, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        balances = {}
        for d in data.get("data", []):
            for det in d.get("details", []):
                balances[det.get("ccy", "").upper()] = float(det.get("cashBal", 0))
        return {c: balances.get(c, 0.0) for c in COINS}
    except Exception:
        return {c: 0.0 for c in COINS}


async def refresh_exchange_holdings(settings_path: str = "settings.json") -> Dict[str, Any]:
    """Fetch balances for configured exchanges and persist a snapshot."""

    cfg_path = Path(settings_path)
    if not cfg_path.exists():
        cfg_path = BASE_DIR / "settings.example.json"

    settings = json.loads(cfg_path.read_text())
    exchanges = settings.get("exchanges", {})

    async with httpx.AsyncClient(timeout=10) as client:
        results: Dict[str, Dict[str, float]] = {}
        if "binance" in exchanges:
            results["binance"] = await _binance_balances(client, exchanges["binance"])
        if "bybit" in exchanges:
            results["bybit"] = await _bybit_balances(client, exchanges["bybit"])
        if "okx" in exchanges:
            results["okx"] = await _okx_balances(client, exchanges["okx"])

    if not results and SAMPLE_PATH.exists():
        try:
            results = json.loads(SAMPLE_PATH.read_text())
        except Exception:
            pass

    snapshot = {
        "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "exchanges": results,
    }

    _append_history(snapshot, HISTORY_PATH)
    return snapshot


# End of file
