
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from aggregate import aggregate_metrics
from ex_dict import ExDict
from ex_dict_ab import AbExDict
from suggest import suggest_rules_from_labels
from ch_query import CH
import json

app = FastAPI()
GLOBAL_STATE = {"state": None}

HTML = """
<!doctype html><meta charset="utf-8">
<title>MM Monitor v5p15</title>
<style>
body{font-family:ui-monospace,monospace;padding:16px}
.card{border:1px solid #ddd;border-radius:12px;padding:12px;margin:12px 0}
h1{margin:0 0 12px 0}
code{background:#f6f8fa;padding:2px 6px;border-radius:6px}
</style>
<h1>做市商动向（v5p15, /ex-dict/suggest 规则建议器）</h1>
<div id="root"></div>
<script>
async function load(){
  const syms = await fetch('/symbols').then(r=>r.json());
  const root = document.getElementById('root'); root.innerHTML = '';
  root.innerHTML += `<div class="card"><h3>Symbols</h3><pre>${JSON.stringify(syms,null,2)}</pre></div>`;
}
setInterval(load, 6000); load();
</script>
"""

@app.get("/", response_class=HTMLResponse)
def index(): return HTML

@app.get("/symbols")
def symbols():
    st = GLOBAL_STATE["state"]
    return sorted(set(k.split("::")[1] for k in st.orderbook.keys())) if st else []

@app.get("/aggregate/{symbol}")
def aggregate(symbol: str):
    st = GLOBAL_STATE["state"]
    if not st: return JSONResponse({"error":"no state"}, status_code=503)
    return aggregate_metrics(st, symbol, onchain_scale_usd=st.onchain_scale_usd, ex_weights=st.onchain_ex_weights)

# ---- /ex-dict/suggest ----
@app.post("/ex-dict/suggest")
async def exdict_suggest(req: Request):
    """
    简单包含/前缀聚类的规则建议器
    payload：
    {
      "source": "ch|stats|both",            # 默认 both
      "ch": {"url":"http://localhost:8123","database":"default","table":"mm_ab_samples","user":"","password":""},
      "since_seconds": 86400,               # CH 时间窗口
      "limit": 2000,                         # CH 限制（优先 since_seconds，不传则用 limit）
      "min_support": 3,
      "max_rules": 20
    }
    返回：[{pattern, canon, support, examples[]}...]
    """
    try:
        body = await req.json()
        source = (body or {}).get("source", "both")
        labels = []

        if source in ("ch","both"):
            ch_cfg = (body or {}).get("ch") or {}
            url = ch_cfg.get("url")
            if url:
                db = ch_cfg.get("database","default"); tb = ch_cfg.get("table","mm_ab_samples")
                ch = CH(url, ch_cfg.get("user",""), ch_cfg.get("password",""))
                since = int((body or {}).get("since_seconds") or 0)
                limit = int((body or {}).get("limit") or 1000)
                if since > 0:
                    sql = f"SELECT label FROM {db}.{tb} WHERE ts > now() - toIntervalSecond({since}) ORDER BY ts DESC"
                else:
                    sql = f"SELECT label FROM {db}.{tb} ORDER BY ts DESC LIMIT {limit}"
                rows = ch.select_json_each_row(sql)
                labels += [r.get("label","") for r in rows]

        if source in ("stats","both"):
            st = AbExDict.stats()
            labels += st.get("A",{}).get("recent_unmatched",[]) or []
            labels += st.get("B",{}).get("recent_unmatched",[]) or []

        # 去重
        seen = set(); uniq_labels = []
        for s in labels:
            if s and s not in seen:
                uniq_labels.append(s); seen.add(s)

        if not uniq_labels:
            return {"rules": [], "msg": "no labels to suggest from (check source streams)"}

        rules = suggest_rules_from_labels(
            uniq_labels,
            min_support=int((body or {}).get("min_support") or 3),
            max_rules=int((body or {}).get("max_rules") or 20)
        )
        return {"rules": rules, "total_labels": len(uniq_labels)}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)
