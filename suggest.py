
import re
from collections import Counter, defaultdict
from typing import List, Dict, Any

# 基础同义词→canon 映射（可以按需扩）
_CANON_MAP = {
    "binance": "binance", "bnb": "binance", "binancehot": "binance", "binancecold": "binance",
    "okx": "okx", "okex": "okx", "okgroup": "okx",
    "bybit": "bybit",
    "coinbase": "coinbase", "cb": "coinbase", "coinbasepro": "coinbase", "cbpro": "coinbase", "prime": "coinbase",
    "kraken": "kraken",
    "bitfinex": "bitfinex", "bfx": "bitfinex",
    "huobi": "huobi", "htx": "huobi",
    "gate": "gate", "gateio": "gate",
    "kucoin": "kucoin",
    "deribit": "deribit",
    "bitstamp": "bitstamp",
}

# token 提取：按非字母数字切分 + 小写
def _tokens(s: str) -> List[str]:
    s = (s or "").lower()
    toks = re.split(r"[^a-z0-9]+", s)
    toks = [t for t in toks if t]
    return toks

def _canon_guess(tokens: List[str]) -> str | None:
    # 优先精确命中映射；其次基于包含关键字（如 'coinbase', 'okx'）
    for t in tokens:
        if t in _CANON_MAP:
            return _CANON_MAP[t]
    for t in tokens:
        for k in list(_CANON_MAP.keys()):
            if k in t:
                return _CANON_MAP[k]
    return None

def _regex_for_variants(variants: List[str]) -> str:
    # 生成简单 OR 正则，自动去重并按长度降序（避免子串过早匹配）
    uniq = sorted(set(variants), key=lambda x: (-len(x), x))
    parts = [re.escape(v) for v in uniq]
    if len(parts) == 1:
        return parts[0]
    return "(?:" + "|".join(parts) + ")"

def suggest_rules_from_labels(labels: List[str], min_support: int = 3, max_rules: int = 20) -> List[Dict[str, Any]]:
    # 1) 提取 tokens 并按 canon 聚合
    buckets = defaultdict(list)  # canon -> list[label]
    for s in labels:
        toks = _tokens(s)
        c = _canon_guess(toks)
        if not c:
            continue
        buckets[c].append(s)

    # 2) 对每个 canon，统计高频“关键子串/前缀”
    suggestions = []
    for canon, items in buckets.items():
        if len(items) < min_support:
            continue
        # 统计 top token
        token_counter = Counter()
        for s in items:
            token_counter.update(_tokens(s))
        # 选出与 canon 相关的 token（包含 canon 或别名）
        rel_tokens = []
        for t, cnt in token_counter.most_common():
            if canon in t or any(k in t for k, v in _CANON_MAP.items() if v == canon):
                rel_tokens.append((t, cnt))
        if not rel_tokens:
            # fallback：直接按 canon 构造一条规则
            pattern = canon
            suggestions.append({"pattern": pattern, "canon": canon, "support": len(items), "examples": items[:5]})
            continue
        # 取前 5 个相关 token 作为变体，构造 OR 正则
        variants = [t for t, _ in rel_tokens[:5]]
        pattern = _regex_for_variants(variants)
        suggestions.append({"pattern": pattern, "canon": canon, "support": len(items), "examples": items[:5]})

    # 3) 支持度排序 + 截断
    suggestions.sort(key=lambda r: (-r["support"], r["canon"]))
    return suggestions[:max_rules]
