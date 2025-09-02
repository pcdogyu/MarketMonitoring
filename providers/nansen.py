from __future__ import annotations

from typing import Any, Dict, Iterable

import httpx


def _read_amount(item: dict) -> float:
    for k in ("amount", "balance", "tokenBalance", "quantity", "qty"):
        v = item.get(k)
        if isinstance(v, (int, float)):
            return float(v)
        try:
            return float(v)
        except Exception:
            continue
    return 0.0


def _norm(sym: str | None) -> str | None:
    if not sym:
        return None
    s = sym.upper()
    if s == "WETH":
        return "ETH"
    if s == "WBTC":
        return "BTC"
    return s


async def fetch_totals(cfg: Dict[str, Any]) -> Dict[str, float]:
    """Aggregate BTC/ETH/USDT/USDC balances for Nansen data.

    Configuration (settings.json -> "nansen") options (choose one style):
    - addresses:  List of wallet addresses to query individually.
                  ``address_path`` is the endpoint template with
                  ``{address}`` placeholder. Default
                  "https://api.nansen.ai/v1/balances?address={address}".
    - clusters:   List of cluster/label identifiers. For each, the code will
                  call ``cluster_path`` (default
                  "/v1/labels/{id}/balances").
    Common options:
    - api_key:    Nansen API key (required)
    - api_base:   Base URL (default "https://api.nansen.ai")

    This module intentionally avoids assuming a rigid response schema and will
    look for objects with ``symbol`` and numeric balance fields.
    """

    api_key = cfg.get("api_key")
    api_base = (cfg.get("api_base") or "https://api.nansen.ai").rstrip("/")
    addrs: Iterable[str] = cfg.get("addresses", [])
    clusters: Iterable[str] = cfg.get("clusters", [])
    addr_tpl = cfg.get("address_path") or "/v1/balances?address={address}"
    clus_tpl = cfg.get("cluster_path") or "/v1/labels/{id}/balances"

    if not api_key or (not addrs and not clusters):
        raise ValueError("nansen.api_key and one of addresses/clusters are required")

    headers = {
        "x-api-key": api_key,
        "authorization": f"Bearer {api_key}",
        "accept": "application/json",
    }

    totals = {"BTC": 0.0, "ETH": 0.0, "USDT": 0.0, "USDC": 0.0}
    async with httpx.AsyncClient(timeout=15) as client:
        # Per-address style
        for a in addrs:
            url = f"{api_base}{addr_tpl.format(address=a)}"
            try:
                r = await client.get(url, headers=headers)
                r.raise_for_status()
                data = r.json()
                items = None
                # try a few common top-level keys
                for k in ("balances", "assets", "data", "result"):
                    v = data.get(k)
                    if isinstance(v, list):
                        items = v
                        break
                if items is None and isinstance(data, list):
                    items = data
                for it in items or []:
                    sym = _norm(it.get("symbol") or it.get("token") or it.get("ticker"))
                    if sym in totals:
                        totals[sym] += _read_amount(it)
            except Exception:
                continue

        # Cluster/label style
        for cid in clusters:
            url = f"{api_base}{clus_tpl.format(id=cid)}"
            try:
                r = await client.get(url, headers=headers)
                r.raise_for_status()
                data = r.json()
                items = None
                for k in ("balances", "assets", "data", "result"):
                    v = data.get(k)
                    if isinstance(v, list):
                        items = v
                        break
                if items is None and isinstance(data, list):
                    items = data
                for it in items or []:
                    sym = _norm(it.get("symbol") or it.get("token") or it.get("ticker"))
                    if sym in totals:
                        totals[sym] += _read_amount(it)
            except Exception:
                continue

    return totals

