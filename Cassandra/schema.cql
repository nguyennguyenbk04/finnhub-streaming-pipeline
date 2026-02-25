-- ============================================================
-- Keyspace
-- ============================================================
CREATE KEYSPACE IF NOT EXISTS trading
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE trading;

-- ============================================================
-- Table 1: Raw trades (written by Spark)
-- Query: Get recent trades for a symbol on a given date
-- Partition: (symbol, trade_date) → keeps partitions bounded
-- Clustering: trade_time DESC → latest trades first
-- ============================================================
CREATE TABLE IF NOT EXISTS trades (
    symbol       TEXT,
    trade_date   DATE,
    trade_time   TIMESTAMP,
    price        DOUBLE,
    volume       DOUBLE,
    conditions   LIST<TEXT>,
    PRIMARY KEY ((symbol, trade_date), trade_time)
) WITH CLUSTERING ORDER BY (trade_time DESC);

-- ============================================================
-- Table 2: 1-minute aggregations (written by Spark)
-- Query: OHLCV candlestick data per symbol per minute
-- Used by: Price Ticker, Volume Analysis dashboards
-- ============================================================
CREATE TABLE IF NOT EXISTS trades_agg_1m (
    symbol        TEXT,
    trade_date    DATE,
    window_start  TIMESTAMP,
    open_price    DOUBLE,
    close_price   DOUBLE,
    high_price    DOUBLE,
    low_price     DOUBLE,
    total_volume  DOUBLE,
    trade_count   INT,
    avg_price     DOUBLE,
    PRIMARY KEY ((symbol, trade_date), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

-- ============================================================
-- Table 3: Latest price per symbol (written by Spark)
-- Query: Get current price for all tracked symbols
-- Used by: Price Ticker stat panel, leaderboard
-- ============================================================
CREATE TABLE IF NOT EXISTS latest_price (
    symbol       TEXT,
    trade_time   TIMESTAMP,
    price        DOUBLE,
    volume       DOUBLE,
    PRIMARY KEY (symbol)
);
