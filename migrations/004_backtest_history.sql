-- Migration: 004_backtest_history
-- Description: Schema for backtesting historical data
-- Created: 2025-01-18

-- =============================================================================
-- PRICE_HISTORY TABLE
-- =============================================================================
-- Stores historical price snapshots for closed markets

CREATE TABLE IF NOT EXISTS price_history (
    id BIGSERIAL PRIMARY KEY,
    condition_id VARCHAR(255) NOT NULL,

    -- Token IDs
    yes_token_id VARCHAR(255) NOT NULL,
    no_token_id VARCHAR(255) NOT NULL,

    -- Prices at snapshot
    yes_price DECIMAL(10,6) NOT NULL,
    no_price DECIMAL(10,6) NOT NULL,

    -- Timestamp of this price point
    timestamp TIMESTAMPTZ NOT NULL,

    -- Data source metadata
    source VARCHAR(50) DEFAULT 'clob_history'  -- 'clob_history', 'gamma_snapshot'
);

-- Index for querying price history by market
CREATE INDEX IF NOT EXISTS idx_price_history_condition
    ON price_history(condition_id, timestamp DESC);

-- Index for time-range queries
CREATE INDEX IF NOT EXISTS idx_price_history_time
    ON price_history(timestamp DESC);

-- Unique constraint to prevent duplicate entries
CREATE UNIQUE INDEX IF NOT EXISTS idx_price_history_unique
    ON price_history(condition_id, timestamp);

-- =============================================================================
-- MARKET_RESOLUTIONS TABLE
-- =============================================================================
-- Stores resolved market outcomes (YES or NO won)

CREATE TABLE IF NOT EXISTS market_resolutions (
    id BIGSERIAL PRIMARY KEY,
    condition_id VARCHAR(255) UNIQUE NOT NULL,

    -- Market metadata
    market_type VARCHAR(50) NOT NULL,           -- 'up_down', 'above', 'price_range'
    asset VARCHAR(10) NOT NULL,                  -- 'BTC', 'ETH', 'SOL', 'XRP'
    timeframe VARCHAR(50) NOT NULL,              -- e.g., '15m', '1h', '4h', 'daily'
    name TEXT NOT NULL,

    -- Token IDs
    yes_token_id VARCHAR(255) NOT NULL,
    no_token_id VARCHAR(255) NOT NULL,

    -- Resolution details
    winning_side VARCHAR(10) NOT NULL,           -- 'yes' or 'no'
    end_time TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,

    -- Final prices before resolution (for analysis)
    final_yes_price DECIMAL(10,6),
    final_no_price DECIMAL(10,6),

    -- Raw API data for debugging
    raw_data JSONB,

    -- Tracking
    fetched_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for querying by asset and timeframe (common pattern)
CREATE INDEX IF NOT EXISTS idx_resolutions_asset_timeframe
    ON market_resolutions(asset, timeframe, end_time DESC);

-- Index for time-based queries
CREATE INDEX IF NOT EXISTS idx_resolutions_end_time
    ON market_resolutions(end_time DESC);

-- Index for winning side analysis
CREATE INDEX IF NOT EXISTS idx_resolutions_winning
    ON market_resolutions(winning_side, asset);

-- =============================================================================
-- BACKTEST_RUNS TABLE
-- =============================================================================
-- Stores backtest execution results

CREATE TABLE IF NOT EXISTS backtest_runs (
    id BIGSERIAL PRIMARY KEY,

    -- Strategy identification
    strategy_name VARCHAR(100) NOT NULL,         -- 'expiry_scalper', 'contrarian_scalper'
    strategy_params JSONB,                       -- Strategy-specific parameters

    -- Data range
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,

    -- Filter criteria
    assets TEXT[],                               -- ['BTC', 'ETH', 'SOL', 'XRP']
    timeframes TEXT[],                           -- ['15m']
    skew_threshold DECIMAL(10,6),                -- e.g., 0.75

    -- Aggregate results
    total_signals INTEGER DEFAULT 0,
    orders_placed INTEGER DEFAULT 0,
    orders_filled INTEGER DEFAULT 0,
    winning_trades INTEGER DEFAULT 0,
    losing_trades INTEGER DEFAULT 0,

    -- Financial metrics
    total_invested DECIMAL(20,8) DEFAULT 0,
    total_payout DECIMAL(20,8) DEFAULT 0,
    net_pnl DECIMAL(20,8) DEFAULT 0,

    -- Computed metrics
    win_rate DECIMAL(10,6),                      -- winning / filled
    fill_rate DECIMAL(10,6),                     -- filled / signals
    roi DECIMAL(10,6),                           -- net_pnl / total_invested
    profit_factor DECIMAL(10,6),                 -- gross_profit / gross_loss
    max_drawdown DECIMAL(10,6),

    -- Execution tracking
    executed_at TIMESTAMPTZ DEFAULT NOW(),
    duration_seconds DECIMAL(10,3)
);

-- Index for querying backtest history
CREATE INDEX IF NOT EXISTS idx_backtest_runs_strategy
    ON backtest_runs(strategy_name, executed_at DESC);

-- =============================================================================
-- BACKTEST_TRADES TABLE
-- =============================================================================
-- Individual simulated trades from backtest runs

CREATE TABLE IF NOT EXISTS backtest_trades (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES backtest_runs(id) ON DELETE CASCADE,

    -- Market reference
    condition_id VARCHAR(255) NOT NULL,
    market_name TEXT,

    -- Signal details
    signal_time TIMESTAMPTZ NOT NULL,
    time_to_expiry_seconds INTEGER,

    -- Price at signal
    yes_price_at_signal DECIMAL(10,6) NOT NULL,
    no_price_at_signal DECIMAL(10,6) NOT NULL,
    skewed_side VARCHAR(10) NOT NULL,            -- 'yes' or 'no' (the skewed side)
    skew_magnitude DECIMAL(10,6) NOT NULL,       -- e.g., 0.82

    -- Trade execution
    trade_side VARCHAR(10) NOT NULL,             -- 'yes' or 'no' (side we bet on)
    order_type VARCHAR(20) NOT NULL,             -- 'market' or 'limit'
    order_price DECIMAL(10,6),                   -- For limit orders
    filled BOOLEAN DEFAULT FALSE,
    fill_price DECIMAL(10,6),
    shares DECIMAL(20,8) NOT NULL,
    cost DECIMAL(20,8) NOT NULL,

    -- Resolution
    winning_side VARCHAR(10) NOT NULL,           -- Market resolution
    won BOOLEAN NOT NULL,
    payout DECIMAL(20,8) NOT NULL,
    pnl DECIMAL(20,8) NOT NULL
);

-- Index for querying trades by run
CREATE INDEX IF NOT EXISTS idx_backtest_trades_run
    ON backtest_trades(run_id);

-- Index for analyzing by market
CREATE INDEX IF NOT EXISTS idx_backtest_trades_market
    ON backtest_trades(condition_id);

-- Index for P&L analysis
CREATE INDEX IF NOT EXISTS idx_backtest_trades_pnl
    ON backtest_trades(won, pnl DESC);

-- =============================================================================
-- COMMENTS
-- =============================================================================

COMMENT ON TABLE price_history IS 'Historical price data for closed markets (from CLOB /prices-history)';
COMMENT ON TABLE market_resolutions IS 'Resolved market outcomes (YES or NO won)';
COMMENT ON TABLE backtest_runs IS 'Backtest execution runs with aggregate metrics';
COMMENT ON TABLE backtest_trades IS 'Individual simulated trades from backtest runs';

COMMENT ON COLUMN market_resolutions.winning_side IS 'Which side won: yes or no';
COMMENT ON COLUMN backtest_trades.skewed_side IS 'The side with price >= threshold (potential winner)';
COMMENT ON COLUMN backtest_trades.trade_side IS 'The side the strategy bet on (may differ from skewed_side for contrarian)';
