from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI()

# demo data
DEMO_SNAPSHOT = {
    "time": "2025-09-02T12:00:00Z",
    "totals": {"BTC": 123.45, "ETH": 4567.89, "USDT": 2000000, "USDC": 1500000}
}
# 历史演示（用于折线图）
DEMO_HISTORY = {
    "time": [f"12:{str(i).zfill(2)}" for i in range(0,30,5)],
    "BTC":  [120,121,122,123,124,125],
    "ETH":  [4500,4520,4550,4560,4568,4570],
    "USDT": [1990000,1995000,1998000,2000000,2003000,2005000],
    "USDC": [1495000,1497000,1499000,1500000,1501000,1502000]
}
DEMO_DERIVS = {
    "BTCUSDT": {
        "funding": [0.01, 0.015, 0.02, 0.018, 0.022, 0.019],
        "basis": [50, 70, 40, 55, 65, 60],
        "oi": [1000, 1200, 900, 1100, 1300, 1250],
        "timestamps": ["12:00","12:05","12:10","12:15","12:20","12:25"]
    },
    "ETHUSDT": {
        "funding": [0.008, 0.011, 0.013, 0.012, 0.014, 0.013],
        "basis": [12, 18, 9, 14, 16, 13],
        "oi": [600, 720, 680, 700, 740, 735],
        "timestamps": ["12:00","12:05","12:10","12:15","12:20","12:25"]
    }
}

@app.get("/", response_class=HTMLResponse)
def index():
    return """
<!doctype html>
<meta charset=\"utf-8\">
<title>MarketMonitoring v4.9</title>
<script src=\"https://cdn.jsdelivr.net/npm/echarts/dist/echarts.min.js\"></script>
<h1>做市商监控 v4.9</h1>
<div style=\"display:flex;gap:8px;flex-wrap:wrap\">
  <button onclick=\"showTab('holdings')\">持仓</button>
  <button onclick=\"showTab('predict')\">预测</button>
  <button onclick=\"showTab('derivs')\">衍生品</button>
</div>

<!-- Tab: 持仓 -->
<div id=\"tab-holdings\">
  <div id=\"holdings-bar\" style=\"width:800px;height:320px;border:1px solid #ccc;margin-top:10px\"></div>
  <div id=\"holdings-line\" style=\"width:800px;height:320px;border:1px solid #ccc;margin-top:10px\"></div>
</div>

<!-- Tab: 预测 -->
<div id=\"tab-predict\" style=\"display:none\">
  <div id=\"predict-json\" style=\"margin-top:10px\"></div>
</div>

<!-- Tab: 衍生品 -->
<div id=\"tab-derivs\" style=\"display:none\">
  <div style=\"display:grid;grid-template-columns:repeat(auto-fit,minmax(360px,1fr));gap:10px\">
    <div>
      <h3>BTCUSDT Funding/Basis/OI</h3>
      <div id=\"derivs-btc\" style=\"width:100%;height:320px;border:1px solid #ccc\"></div>
    </div>
    <div>
      <h3>ETHUSDT Funding/Basis/OI</h3>
      <div id=\"derivs-eth\" style=\"width:100%;height:320px;border:1px solid #ccc\"></div>
    </div>
  </div>
</div>

<script>
function showTab(tab){
  document.getElementById('tab-holdings').style.display = (tab==='holdings')?'block':'none';
  document.getElementById('tab-predict').style.display  = (tab==='predict')?'block':'none';
  document.getElementById('tab-derivs').style.display   = (tab==='derivs')?'block':'none';
}
async function load(){
  // holdings bar
  let snap = await fetch('/mm/holdings').then(r=>r.json());
  let bar = echarts.init(document.getElementById('holdings-bar'));
  bar.setOption({
    title:{text:'最近快照'},
    xAxis:{type:'category',data:Object.keys(snap.totals)},
    yAxis:{type:'value'},
    series:[{data:Object.values(snap.totals),type:'bar'}]
  });
  // holdings line
  let hist = await fetch('/chart/holdings').then(r=>r.json());
  let line = echarts.init(document.getElementById('holdings-line'));
  line.setOption({
    tooltip:{trigger:'axis'},
    legend:{data:['BTC','ETH','USDT','USDC']},
    xAxis:{type:'category',data:hist.time},
    yAxis:{type:'value'},
    series:[
      {name:'BTC', type:'line', data:hist.BTC},
      {name:'ETH', type:'line', data:hist.ETH},
      {name:'USDT', type:'line', data:hist.USDT},
      {name:'USDC', type:'line', data:hist.USDC},
    ]
  });
  // predict
  let pred1 = await fetch('/predict/BTCUSDT').then(r=>r.json());
  let pred2 = await fetch('/predict/ETHUSDT').then(r=>r.json());
  document.getElementById('predict-json').innerHTML =
    '<h3>预测信号</h3><pre>'+JSON.stringify([pred1,pred2],null,2)+'</pre>';
  // derivs
  let dbtc = await fetch('/chart/derivs?symbol=BTCUSDT').then(r=>r.json());
  let deth = await fetch('/chart/derivs?symbol=ETHUSDT').then(r=>r.json());
  let btcChart = echarts.init(document.getElementById('derivs-btc'));
  btcChart.setOption({
    tooltip:{trigger:'axis'},
    legend:{data:['funding','basis','oi']},
    xAxis:{type:'category',data:dbtc.timestamps},
    yAxis:{type:'value'},
    series:[
      {name:'funding', type:'line', data:dbtc.funding},
      {name:'basis',   type:'line', data:dbtc.basis},
      {name:'oi',      type:'line', data:dbtc.oi}
    ]
  });
  let ethChart = echarts.init(document.getElementById('derivs-eth'));
  ethChart.setOption({
    tooltip:{trigger:'axis'},
    legend:{data:['funding','basis','oi']},
    xAxis:{type:'category',data:deth.timestamps},
    yAxis:{type:'value'},
    series:[
      {name:'funding', type:'line', data:deth.funding},
      {name:'basis',   type:'line', data:deth.basis},
      {name:'oi',      type:'line', data:deth.oi}
    ]
  });
}
setInterval(load, 5000);
load();
</script>
"""

@app.get("/mm/holdings")
def mm_holdings():
    return DEMO_SNAPSHOT

@app.get("/chart/holdings")
def chart_holdings():
    return DEMO_HISTORY

@app.get("/predict/{symbol}")
def predict(symbol: str):
    sym = symbol.upper()
    # demo: ETH 略低，BTC 略高
    score = 0.18 if sym == "BTCUSDT" else 0.07
    sig = "bullish" if score>0 else "bearish" if score<0 else "neutral"
    return {"symbol": sym, "score": score, "signal": sig}

@app.get("/chart/derivs")
def chart_derivs(symbol: str):
    return DEMO_DERIVS.get(symbol.upper(), DEMO_DERIVS["BTCUSDT"])
