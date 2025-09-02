from __future__ import annotations

import asyncio
from typing import Any, Dict, Iterable

import httpx


def _first_list_of_dicts(obj: Any) -> Iterable[dict] | None:
    """Best‑effort helper to find a list of dicts with token balances.

    Different Arkham endpoints may use slightly different response shapes.
    This function tries several common keys and otherwise scans values for the
    first list of dictionaries.
    """

    if not isinstance(obj, dict):
        return None
    for key in ("balances", "assets", "holdings", "portfolio", "data", "result"):
        v = obj.get(key)
        if isinstance(v, list) and (not v or isinstance(v[0], dict)):
            return v  # type: ignore[return-value]
        if isinstance(v, dict):
            inner = _first_list_of_dicts(v)
            if inner is not None:
                return inner
    # Fallback: scan values
    for v in obj.values():
        if isinstance(v, list) and (not v or isinstance(v[0], dict)):
            return v  # type: ignore[return-value]
        if isinstance(v, dict):
            inner = _first_list_of_dicts(v)
            if inner is not None:
                return inner
    return None


def _read_amount(item: dict) -> float:
    for k in ("amount", "balance", "tokenBalance", "quantity", "qty"):
        v = item.get(k)
        if isinstance(v, (int, float)):
            return float(v)
        try:
            return float(v)
        except Exception:
            continue
    # If only USD value is present we cannot convert to units here
    return 0.0


def _normalise_symbol(sym: str | None) -> str | None:
    if not sym:
        return None
    s = sym.upper()
    if s in ("WETH",):
        return "ETH"
    if s in ("WBTC",):
        return "BTC"
    return s


async def fetch_totals(cfg: Dict[str, Any]) -> Dict[str, float]:
    """Aggregate BTC/ETH/USDT/USDC balances for Arkham entities.

    Configuration (settings.json -> "arkham"):
    - api_key:       Your Arkham API key (required)
    - entity_ids:    List of Arkham entity IDs to aggregate (required)
    - api_base:      Base URL, default "https://api.arkhamintelligence.com"
    - balance_path:  Endpoint template with "{id}" placeholder. Default
                     "/v1/entity/{id}/balances".

    Notes
    - The code is resilient to minor schema differences and will try common
      fields to extract token amounts. If an endpoint returns only USD values
      the amount cannot be inferred and will be treated as 0.
    """

    api_key = cfg.get("api_key")
    entity_ids: Iterable[str] = cfg.get("entity_ids", [])
    api_base = (cfg.get("api_base") or "https://api.arkhamintelligence.com").rstrip("/")
    tpl = cfg.get("balance_path") or "/v1/entity/{id}/balances"

    if not api_key or not entity_ids:
        raise ValueError("arkham.api_key and arkham.entity_ids are required")

    headers = {
        "x-api-key": api_key,
        "authorization": f"Bearer {api_key}",  # some deployments use bearer
        "accept": "application/json",
    }

    totals = {"BTC": 0.0, "ETH": 0.0, "USDT": 0.0, "USDC": 0.0}
    async with httpx.AsyncClient(timeout=15) as client:
        for eid in entity_ids:
            url = f"{api_base}{tpl.format(id=eid)}"
            try:
                r = await client.get(url, headers=headers)
                r.raise_for_status()
                data = r.json()
                items = _first_list_of_dicts(data) or []
                for it in items:
                    sym = _normalise_symbol(it.get("symbol") or it.get("token") or it.get("ticker"))
                    if sym in totals:
                        totals[sym] += _read_amount(it)
            except Exception:
                # ignore per-entity failures; continue to aggregate others
                continue
    return totals

