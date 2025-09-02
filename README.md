# MarketMonitoring v4.9

## 功能
- 做市商持仓监控（ETH / BTC）
- 链上数据源（预留）：
  - ETH & ERC20 (USDT, USDC) via Etherscan
  - BTC via Blockstream API
- 衍生品数据（预留）：Binance / Bybit / OKX Funding, Basis, OI
- 地址标签导入（CSV/JSON → settings.json）
- FastAPI 服务 + ECharts 前端：
  - Tab1: 最近持仓快照柱状图 + 历史折线
  - Tab2: 预测信号 (BTCUSDT, ETHUSDT)
  - Tab3: 衍生品 Funding / Basis / OI 曲线（**BTCUSDT & ETHUSDT 并排展示**）
  - Tab4: 三大交易所挂单统计（BTCUSDT & ETHUSDT）

## 使用方法
```bash
python -m venv mmenv
# Windows:
.\mmenv\Scriptsctivate
# macOS/Linux:
# source mmenv/bin/activate
pip install -r requirements.txt
uvicorn server:app --reload --host 0.0.0.0 --port 8000
```
浏览器访问: http://localhost:8000
