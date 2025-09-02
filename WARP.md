# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Project Overview

MarketMonitoring is a FastAPI-based market monitoring system that provides:
- Real-time market data aggregation and monitoring
- ClickHouse database integration for time-series data
- Rule suggestion engine for market patterns
- Web interface for monitoring symbols and metrics

## Architecture

### Core Components

- **server.py**: FastAPI application with main endpoints and UI
- **ch_query.py**: ClickHouse database client wrapper
- **suggest.py**: Pattern matching and rule suggestion engine
- **Missing modules** (referenced but not present):
  - `aggregate.py`: Market data aggregation logic
  - `ex_dict.py`: Exchange dictionary utilities
  - `ex_dict_ab.py`: A/B testing exchange dictionary with stats

### Key Features

1. **Web Interface**: Simple HTML frontend with auto-refresh for symbol monitoring
2. **Rule Suggestion API**: `/ex-dict/suggest` endpoint that analyzes market labels and suggests patterns
3. **ClickHouse Integration**: Direct SQL queries for time-series market data
4. **Pattern Recognition**: Token-based analysis with exchange canonicalization

## Development Commands

### Environment Setup
```bash
# Activate virtual environment (Windows)
mmenv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Running the Application
```bash
# Start the FastAPI development server
uvicorn server:app --reload --host 0.0.0.0 --port 8000

# Alternative with direct python
python -m uvicorn server:app --reload
```

### Testing
```bash
# Test individual modules
python -c "import suggest; print('Suggest module OK')"
python -c "import ch_query; print('ClickHouse client OK')"

# Test server imports (will fail due to missing modules)
python -c "import server"
```

## Development Notes

### Missing Dependencies
The server.py imports several modules that are not present in the repository:
- `aggregate.aggregate_metrics`
- `ex_dict.ExDict`
- `ex_dict_ab.AbExDict`

These need to be implemented or the imports removed for the application to run.

### Database Configuration
The system expects ClickHouse database access with:
- Default URL: `http://localhost:8123`
- Default database: `default`
- Default table: `mm_ab_samples`
- Expected schema: `ts` (timestamp), `label` (text)

### API Endpoints

- `GET /`: Web interface showing symbol monitoring
- `GET /symbols`: Returns list of available trading symbols
- `GET /aggregate/{symbol}`: Get aggregated metrics for a symbol
- `POST /ex-dict/suggest`: Generate pattern rules from market labels

### Pattern Recognition Logic

The suggest.py module uses:
- Token extraction via regex split on non-alphanumeric characters
- Exchange canonicalization mapping (binance, okx, coinbase, etc.)
- Support threshold filtering (min_support parameter)
- Regex pattern generation for exchange variants

### State Management

Uses a global state dictionary (`GLOBAL_STATE`) to maintain:
- Current market state
- Orderbook data
- Exchange weights and scaling factors

## Environment

- Python 3.12.6
- Virtual environment: `mmenv/`
- Windows PowerShell compatible
- FastAPI + Uvicorn web server
- ClickHouse database integration
