from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "market.db"


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS holdings (
              ts TEXT NOT NULL,
              BTC REAL, ETH REAL, USDT REAL, USDC REAL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS derivs (
              symbol TEXT NOT NULL,
              ts TEXT NOT NULL,
              funding REAL, basis REAL, oi REAL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS prices (
              symbol TEXT NOT NULL,
              ts TEXT NOT NULL,
              price REAL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS cex_holdings (
              ts TEXT NOT NULL,
              exchange TEXT NOT NULL,
              BTC REAL, ETH REAL, USDT REAL, USD REAL, USDC REAL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS oi_cache (
              symbol TEXT NOT NULL,
              exchange TEXT NOT NULL,
              oi REAL NOT NULL,
              ts TEXT NOT NULL,
              PRIMARY KEY(symbol, exchange)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_volumes (
              symbol TEXT NOT NULL,
              date TEXT NOT NULL,
              price REAL NOT NULL,
              volume REAL NOT NULL,
              PRIMARY KEY(symbol,date,price)
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_derivs_sym_ts ON derivs(symbol, ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_prices_sym_ts ON prices(symbol, ts)")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_cex_holdings_ts_ex ON cex_holdings(ts, exchange)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_oi_cache_symbol ON oi_cache(symbol)"
        )
        con.commit()


def save_holdings(snapshot: Dict[str, Any]) -> None:
    """Persist a holdings snapshot into SQLite."""

    ts: str = snapshot["time"]
    t = snapshot.get("totals", {})
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            "INSERT INTO holdings(ts,BTC,ETH,USDT,USDC) VALUES (?,?,?,?,?)",
            (ts, float(t.get("BTC", 0)), float(t.get("ETH", 0)), float(t.get("USDT", 0)), float(t.get("USDC", 0))),
        )
        con.commit()


def save_derivs(symbol: str, data: Dict[str, Any]) -> None:
    """Persist a derivatives data point into SQLite."""

    ts: str = data.get("time")
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            "INSERT INTO derivs(symbol,ts,funding,basis,oi) VALUES (?,?,?,?,?)",
            (symbol, ts, float(data.get("funding", 0)), float(data.get("basis", 0)), float(data.get("oi", 0))),
        )
        con.commit()


def save_price(symbol: str, ts: str, price: float) -> None:
    """Persist a price point into SQLite."""

    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            "INSERT INTO prices(symbol,ts,price) VALUES (?,?,?)",
            (symbol, ts, float(price)),
        )
        con.commit()


def save_oi_partial(symbol: str, exchange: str, oi: float, ts: str) -> None:
    """Store a partial open-interest value for ``symbol`` from ``exchange``."""

    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            "INSERT OR REPLACE INTO oi_cache(symbol,exchange,oi,ts) VALUES (?,?,?,?)",
            (symbol, exchange, float(oi), ts),
        )
        con.commit()


def sum_oi_if_complete(symbol: str) -> float | None:
    """Return total open interest for ``symbol`` if all exchanges reported."""

    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()
        cur.execute("SELECT oi FROM oi_cache WHERE symbol=?", (symbol,))
        rows = cur.fetchall()

    if len(rows) >= 3:
        return sum(r[0] for r in rows)
    return None


def save_cex_holdings(snapshot: Dict[str, Any]) -> None:
    """Persist centralised exchange holdings snapshot into SQLite."""

    ts: str = snapshot.get("time")
    exchanges: Dict[str, Any] = snapshot.get("exchanges", {})
    with sqlite3.connect(DB_PATH) as con:
        for ex, bal in exchanges.items():
            con.execute(
                "INSERT INTO cex_holdings(ts,exchange,BTC,ETH,USDT,USD,USDC) VALUES (?,?,?,?,?,?,?)",
                (
                    ts,
                    ex,
                    float(bal.get("BTC", 0)),
                    float(bal.get("ETH", 0)),
                    float(bal.get("USDT", 0)),
                    float(bal.get("USD", 0)),
                    float(bal.get("USDC", 0)),
                ),
            )
        con.commit()


def save_trade_volumes(symbol: str, date: str, volumes: Dict[float, float]) -> None:
    """Persist aggregated trade volumes for a symbol on a given date."""

    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()
        for price, vol in volumes.items():
            cur.execute(
                "INSERT OR REPLACE INTO trade_volumes(symbol,date,price,volume) VALUES (?,?,?,?)",
                (symbol, date, float(price), float(vol)),
            )
        con.commit()


def query_trade_volumes(symbol: str, date: str) -> Dict[float, float]:
    """Load cached trade volume profile for ``symbol`` on ``date``."""

    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()
        cur.execute(
            "SELECT price,volume FROM trade_volumes WHERE symbol=? AND date=?",
            (symbol, date),
        )
        rows = cur.fetchall()
    return {float(r[0]): float(r[1]) for r in rows}


def query_derivs(symbol: str, since_seconds: int | None = None) -> Dict[str, Any]:
    """Load derivatives history for ``symbol`` from DB.

    If ``since_seconds`` is provided, only return rows newer than now-interval.
    Timestamps in DB are ISO strings written by server (UTC).
    """

    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()
        if since_seconds is None:
            cur.execute(
                "SELECT ts,funding,basis,oi FROM derivs WHERE symbol=? ORDER BY ts",
                (symbol,),
            )
        else:
            # Compare lexicographically on ISO strings is safe when formatted as %Y-%m-%dT%H:%M:%SZ
            # Incoming since_seconds is handled by server to compute cutoff string.
            cur.execute(
                "SELECT ts,funding,basis,oi FROM derivs WHERE symbol=? AND ts>=? ORDER BY ts",
                (symbol, _cutoff_iso(since_seconds)),
            )
        rows = cur.fetchall()

    return {
        "funding": [r[1] for r in rows],
        "basis": [r[2] for r in rows],
        "oi": [r[3] for r in rows],
        "timestamps": [r[0] for r in rows],
    }


def query_price(symbol: str, since_seconds: int | None = None) -> Dict[str, Any]:
    """Load price history for ``symbol`` from DB."""

    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()
        if since_seconds is None:
            cur.execute(
                "SELECT ts,price FROM prices WHERE symbol=? ORDER BY ts",
                (symbol,),
            )
        else:
            cur.execute(
                "SELECT ts,price FROM prices WHERE symbol=? AND ts>=? ORDER BY ts",
                (symbol, _cutoff_iso(since_seconds)),
            )
        rows = cur.fetchall()

    return {"price": [r[1] for r in rows], "timestamps": [r[0] for r in rows]}


def _cutoff_iso(since_seconds: int) -> str:
    import time

    t = int(time.time()) - max(0, since_seconds)
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))


def prune_old_data(days: int = 14) -> None:
    """Delete rows older than ``days`` from all tables."""

    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()
        for table in ("holdings", "derivs", "prices", "cex_holdings"):
            cur.execute(f"DELETE FROM {table} WHERE ts < ?", (cutoff_str,))
        con.commit()

