"""Utility functions for fetching on‑chain holdings.

This module talks to public blockchain APIs and aggregates balances for
addresses listed in ``settings.json``.  Only a very small subset of the
Etherscan and Blockstream APIs are used which keeps the implementation
lightweight and dependency free beyond :mod:`httpx`.

The main entry point is :func:`refresh_holdings` which reads the settings,
fetches balances for all configured wallets and appends the resulting
snapshot to ``data/holdings_history.json``.

The code is written with network failures in mind – every API call is wrapped
in ``try/except`` blocks and returns ``0`` on error so the rest of the
pipeline can continue to operate.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any, Dict, Iterable

import httpx

ETH_DECIMALS = 10**18
TOKEN_DECIMALS = 10**6  # USDT/USDC on Ethereum use 6 decimals


async def _eth_balance(client: httpx.AsyncClient, api_base: str, api_key: str, address: str) -> float:
    """Return ETH balance for ``address`` in whole ETH."""

    try:
        resp = await client.get(
            api_base,
            params={
                "module": "account",
                "action": "balance",
                "address": address,
                "tag": "latest",
                "apikey": api_key,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return float(data.get("result", 0)) / ETH_DECIMALS
    except Exception:
        return 0.0


async def _token_balance(
    client: httpx.AsyncClient,
    api_base: str,
    api_key: str,
    contract: str,
    address: str,
) -> float:
    """Return ERC20 token balance for ``address``.

    The result is normalised using :data:`TOKEN_DECIMALS` which is suitable for
    USDT/USDC tokens.
    """

    try:
        resp = await client.get(
            api_base,
            params={
                "module": "account",
                "action": "tokenbalance",
                "contractaddress": contract,
                "address": address,
                "tag": "latest",
                "apikey": api_key,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return float(data.get("result", 0)) / TOKEN_DECIMALS
    except Exception:
        return 0.0


async def _btc_balance(client: httpx.AsyncClient, api_base: str, address: str) -> float:
    """Return BTC balance for ``address`` using Blockstream API."""

    try:
        resp = await client.get(f"{api_base}/address/{address}")
        resp.raise_for_status()
        data = resp.json()
        chain = data.get("chain_stats", {})
        funded = float(chain.get("funded_txo_sum", 0))
        spent = float(chain.get("spent_txo_sum", 0))
        return (funded - spent) / 1e8
    except Exception:
        return 0.0


async def _gather_balances(settings: Dict[str, Any]) -> Dict[str, float]:
    """Fetch balances for all wallets defined in ``settings``."""

    totals = {"BTC": 0.0, "ETH": 0.0, "USDT": 0.0, "USDC": 0.0}

    eth_cfg = settings.get("eth", {})
    btc_cfg = settings.get("btc", {})
    api_key = settings.get("etherscan_api_key", "")

    async with httpx.AsyncClient(timeout=10) as client:
        for mm in settings.get("mm", []):
            eth = mm.get("eth", {})
            btc = mm.get("btc", {})

            eth_addrs: Iterable[str] = eth.get("hot", []) + eth.get("cold", [])
            btc_addrs: Iterable[str] = btc.get("hot", []) + btc.get("cold", [])

            for addr in eth_addrs:
                totals["ETH"] += await _eth_balance(client, eth_cfg.get("api_base", ""), api_key, addr)
                totals["USDT"] += await _token_balance(
                    client,
                    eth_cfg.get("api_base", ""),
                    api_key,
                    eth_cfg.get("usdt_contract", ""),
                    addr,
                )
                totals["USDC"] += await _token_balance(
                    client,
                    eth_cfg.get("api_base", ""),
                    api_key,
                    eth_cfg.get("usdc_contract", ""),
                    addr,
                )

            for addr in btc_addrs:
                totals["BTC"] += await _btc_balance(client, btc_cfg.get("api_base", ""), addr)

    return totals


def _append_history(snapshot: Dict[str, Any], path: Path) -> None:
    """Append ``snapshot`` to JSON history at ``path``."""

    try:
        history = json.loads(path.read_text())
    except Exception:
        history = []

    history.append(snapshot)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(history))


async def refresh_holdings(settings_path: str = "settings.json") -> Dict[str, Any]:
    """Fetch balances and persist a new snapshot.

    Parameters
    ----------
    settings_path:
        Path to the settings JSON file.  If the file does not exist a
        :mod:`settings.example.json` will be used instead.

    Returns
    -------
    dict
        Snapshot in the form ``{"time": ..., "totals": {...}}``.
    """

    cfg_path = Path(settings_path)
    if not cfg_path.exists():
        cfg_path = Path("settings.example.json")

    settings = json.loads(cfg_path.read_text())

    totals = await _gather_balances(settings)

    snapshot = {
        "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "totals": totals,
    }

    _append_history(snapshot, Path("data/holdings_history.json"))

    return snapshot

