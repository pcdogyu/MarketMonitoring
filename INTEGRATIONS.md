# Arkham / Nansen Integrations

This project can optionally pull market‑maker holdings from Arkham Intelligence
or Nansen Smart Money instead of manual address aggregation.

How it works
- Add an `arkham` or `nansen` block to `settings.json` (see `settings.example.json`).
- When configured, the server will prefer these providers to compute BTC/ETH/USDT/USDC totals.
- On any network/auth error the app automatically falls back to address‑based
  aggregation and, if that also yields zero, to `data/holdings_sample.json` so
  the UI remains usable offline.

Arkham
- Required: `api_key`, `entity_ids` (list of Arkham entity IDs)
- Optional: `api_base` (default `https://api.arkhamintelligence.com`),
  `balance_path` endpoint template (default `/v1/entity/{id}/balances`)

Nansen
- Required: `api_key`
- Choose one:
  - `addresses`: list of wallet addresses (uses `address_path` template)
  - `clusters`: list of label/cluster IDs (uses `cluster_path` template)
- Optional: `api_base` (default `https://api.nansen.ai`)

Notes
- Provider clients are in `providers/arkham.py` and `providers/nansen.py`.
- Response formats vary across accounts. The parsers are defensive; if your
  responses differ, tweak the small extraction helpers accordingly.
