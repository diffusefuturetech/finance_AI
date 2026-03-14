-- SQLite schema for finance_AI data cache

CREATE TABLE IF NOT EXISTS daily_ohlcv (
    symbol    TEXT NOT NULL,
    date      TEXT NOT NULL,
    open      REAL,
    close     REAL,
    high      REAL,
    low       REAL,
    volume    REAL,
    amount    REAL,
    factor    REAL,
    PRIMARY KEY (symbol, date)
);

CREATE TABLE IF NOT EXISTS stock_info (
    symbol      TEXT PRIMARY KEY,
    name        TEXT,
    market      TEXT,
    industry    TEXT,
    list_date   TEXT,
    updated_at  TEXT
);

CREATE TABLE IF NOT EXISTS update_log (
    symbol      TEXT PRIMARY KEY,
    last_date   TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ohlcv_date ON daily_ohlcv(date);
CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol ON daily_ohlcv(symbol);
