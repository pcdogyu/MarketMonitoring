
from typing import Dict, Any

def aggregate_metrics(
    st: Any,
    symbol: str,
    onchain_scale_usd: float = 1.0,
    ex_weights: Dict[str, float] | None = None,
) -> Dict[str, Any]:
    ex_weights = ex_weights or {}
    total_w = 0.0
    w_bid = 0.0
    w_ask = 0.0
    vol_sum = 0.0
    ex_count = 0

    for k, v in (st.orderbook or {}).items():
        try:
            ex, sym = k.split("::", 1)
        except ValueError:
            continue
        if sym != symbol:
            continue

        w = float(ex_weights.get(ex, 1.0))
        bid = float(v.get("bid", 0.0))
        ask = float(v.get("ask", 0.0))
        vol = float(v.get("volume", 0.0))

        total_w += w
        w_bid += bid * w
        w_ask += ask * w
        vol_sum += vol
        ex_count += 1

    mid = None
    if total_w > 0:
        mid = ((w_bid / total_w) + (w_ask / total_w)) / 2.0

    return {
        "symbol": symbol,
        "ex_count": ex_count,
        "weighted_bid": w_bid / total_w if total_w else None,
        "weighted_ask": w_ask / total_w if total_w else None,
        "mid": mid,
        "volume_sum": vol_sum * float(onchain_scale_usd or 1.0),
        "scale": float(onchain_scale_usd or 1.0),
    }
