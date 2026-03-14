"""SQLite caching layer for stock data to minimize API calls."""

import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

from config.settings import CACHE_DB_PATH


class DataCache:
    """SQLite-based cache for daily OHLCV and stock info."""

    def __init__(self, db_path: Path | None = None):
        self.db_path = db_path or CACHE_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self._init_schema()

    def _init_schema(self) -> None:
        """Create tables if they don't exist."""
        schema_path = Path(__file__).parent / "schema.sql"
        with open(schema_path) as f:
            self.conn.executescript(f.read())
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    # --- Daily OHLCV ---

    def get_cached_range(self, symbol: str) -> tuple[str, str] | None:
        """Return (earliest_date, latest_date) cached for this symbol."""
        cursor = self.conn.execute(
            "SELECT MIN(date), MAX(date) FROM daily_ohlcv WHERE symbol = ?",
            (symbol,),
        )
        row = cursor.fetchone()
        if row and row[0] is not None:
            return (row[0], row[1])
        return None

    def get_daily(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        """Retrieve cached daily data. Returns None if no data found."""
        df = pd.read_sql_query(
            "SELECT * FROM daily_ohlcv WHERE symbol = ? AND date >= ? AND date <= ? ORDER BY date",
            self.conn,
            params=(symbol, start_date, end_date),
        )
        if df.empty:
            return None
        return df

    def store_daily(self, symbol: str, df: pd.DataFrame) -> None:
        """Upsert daily OHLCV data into cache."""
        if df.empty:
            return
        records = df.copy()
        records["symbol"] = symbol
        cols = ["symbol", "date", "open", "close", "high", "low", "volume", "amount", "factor"]
        available_cols = [c for c in cols if c in records.columns]
        records = records[available_cols]

        # Use INSERT OR REPLACE to handle duplicates
        placeholders = ", ".join(["?"] * len(available_cols))
        col_names = ", ".join(available_cols)
        for _, row in records.iterrows():
            self.conn.execute(
                f"INSERT OR REPLACE INTO daily_ohlcv ({col_names}) VALUES ({placeholders})",
                tuple(row[c] for c in available_cols),
            )
        self.conn.commit()

        # Update log
        latest = df["date"].max()
        self.conn.execute(
            "INSERT OR REPLACE INTO update_log (symbol, last_date, updated_at) VALUES (?, ?, ?)",
            (symbol, latest, datetime.now().isoformat()),
        )
        self.conn.commit()

    # --- Stock Info ---

    def get_stock_info(self, symbol: str) -> dict | None:
        """Get cached stock info."""
        cursor = self.conn.execute(
            "SELECT symbol, name, market, industry, list_date FROM stock_info WHERE symbol = ?",
            (symbol,),
        )
        row = cursor.fetchone()
        if row:
            return dict(zip(["symbol", "name", "market", "industry", "list_date"], row))
        return None

    def store_stock_info(self, info_df: pd.DataFrame) -> None:
        """Bulk upsert stock info."""
        if info_df.empty:
            return
        info_df = info_df.copy()
        info_df["updated_at"] = datetime.now().isoformat()
        cols = ["symbol", "name", "market", "industry", "list_date", "updated_at"]
        available_cols = [c for c in cols if c in info_df.columns]
        for _, row in info_df[available_cols].iterrows():
            self.conn.execute(
                "INSERT OR REPLACE INTO stock_info (symbol, name, market, industry, list_date, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                tuple(row.get(c) for c in cols),
            )
        self.conn.commit()

    def search_stock(self, query: str) -> list[dict]:
        """Fuzzy search stock by code or name fragment."""
        cursor = self.conn.execute(
            "SELECT symbol, name, market, industry FROM stock_info "
            "WHERE symbol LIKE ? OR name LIKE ? LIMIT 20",
            (f"%{query}%", f"%{query}%"),
        )
        return [
            dict(zip(["symbol", "name", "market", "industry"], row))
            for row in cursor.fetchall()
        ]

    def get_last_update(self, symbol: str) -> str | None:
        """Get the last cached date for a symbol."""
        cursor = self.conn.execute(
            "SELECT last_date FROM update_log WHERE symbol = ?", (symbol,)
        )
        row = cursor.fetchone()
        return row[0] if row else None
