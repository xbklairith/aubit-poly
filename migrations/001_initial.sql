-- Migration: 001_initial
-- Description: Initial schema for polyglot architecture
-- Created: 2025-01-08

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- =============================================================================
-- MARKETS TABLE
-- =============================================================================
-- Stores discovered prediction markets from Gamma API

CREATE TABLE IF NOT EXISTS markets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    condition_id VARCHAR(255) UNIQUE NOT NULL,
    market_type VARCHAR(50) NOT NULL,           -- 'up_down', 'above', 'price_range'
    asset VARCHAR(10) NOT NULL,                  -- 'BTC', 'ETH', 'SOL', 'XRP'
    timeframe VARCHAR(50) NOT NULL,              -- e.g., '1h', '4h', 'daily'
    yes_token_id VARCHAR(255) NOT NULL,
    no_token_id VARCHAR(255) NOT NULL,
    name TEXT NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    is_active BOOLEAN DEFAULT true,
    discovered_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for markets
CREATE INDEX IF NOT EXISTS idx_markets_active ON markets(is_active) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_markets_end_time ON markets(end_time);
CREATE INDEX IF NOT EXISTS idx_markets_asset ON markets(asset);
CREATE INDEX IF NOT EXISTS idx_markets_type ON markets(market_type);

-- =============================================================================
-- ORDERBOOK_SNAPSHOTS TABLE
-- =============================================================================
-- Stores orderbook snapshots with full depth as JSONB

CREATE TABLE IF NOT EXISTS orderbook_snapshots (
    id BIGSERIAL PRIMARY KEY,
    market_id UUID NOT NULL REFERENCES markets(id) ON DELETE CASCADE,

    -- Best prices (extracted from depth for quick queries)
    yes_best_ask DECIMAL(10,6),
    yes_best_bid DECIMAL(10,6),
    no_best_ask DECIMAL(10,6),
    no_best_bid DECIMAL(10,6),

    -- Computed spread (cost to buy both YES and NO)
    spread DECIMAL(10,6) GENERATED ALWAYS AS (yes_best_ask + no_best_ask) STORED,

    -- Full orderbook depth as JSONB arrays
    -- Format: [{"price": "0.55", "size": "100.00"}, ...]
    yes_asks JSONB,
    yes_bids JSONB,
    no_asks JSONB,
    no_bids JSONB,

    captured_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for querying latest snapshot per market
CREATE INDEX IF NOT EXISTS idx_snapshots_market_time
    ON orderbook_snapshots(market_id, captured_at DESC);

-- Unique constraint for ON CONFLICT upsert (one snapshot per market)
CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshots_market_unique
    ON orderbook_snapshots(market_id);

-- Index for finding profitable spreads
CREATE INDEX IF NOT EXISTS idx_snapshots_spread
    ON orderbook_snapshots(spread) WHERE spread < 1.0;

-- =============================================================================
-- POSITIONS TABLE
-- =============================================================================
-- Tracks open and closed trading positions

CREATE TABLE IF NOT EXISTS positions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    market_id UUID NOT NULL REFERENCES markets(id),

    -- Shares held
    yes_shares DECIMAL(20,8) NOT NULL DEFAULT 0,
    no_shares DECIMAL(20,8) NOT NULL DEFAULT 0,

    -- Cost basis
    total_invested DECIMAL(20,8) NOT NULL DEFAULT 0,

    -- Status
    status VARCHAR(20) DEFAULT 'open',           -- 'open', 'closed', 'settled'
    is_dry_run BOOLEAN DEFAULT true,

    -- Timestamps
    opened_at TIMESTAMPTZ DEFAULT NOW(),
    closed_at TIMESTAMPTZ
);

-- Index for finding open positions
CREATE INDEX IF NOT EXISTS idx_positions_status
    ON positions(status) WHERE status = 'open';

-- Index for positions by market
CREATE INDEX IF NOT EXISTS idx_positions_market
    ON positions(market_id);

-- =============================================================================
-- TRADES TABLE
-- =============================================================================
-- Individual trade executions

CREATE TABLE IF NOT EXISTS trades (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    position_id UUID NOT NULL REFERENCES positions(id) ON DELETE CASCADE,

    -- Trade details
    side VARCHAR(10) NOT NULL,                   -- 'yes', 'no'
    action VARCHAR(10) NOT NULL,                 -- 'buy', 'sell'
    price DECIMAL(10,6) NOT NULL,
    shares DECIMAL(20,8) NOT NULL,

    -- Computed total
    total DECIMAL(20,8) GENERATED ALWAYS AS (price * shares) STORED,

    executed_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for trades by position
CREATE INDEX IF NOT EXISTS idx_trades_position
    ON trades(position_id);

-- Index for trades by time
CREATE INDEX IF NOT EXISTS idx_trades_time
    ON trades(executed_at DESC);

-- =============================================================================
-- BOT_SESSIONS TABLE (optional - for tracking bot runs)
-- =============================================================================

CREATE TABLE IF NOT EXISTS bot_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Session info
    is_dry_run BOOLEAN DEFAULT true,
    starting_balance DECIMAL(20,8) NOT NULL,
    current_balance DECIMAL(20,8) NOT NULL,

    -- Statistics
    total_trades INTEGER DEFAULT 0,
    winning_trades INTEGER DEFAULT 0,
    total_opportunities INTEGER DEFAULT 0,
    positions_opened INTEGER DEFAULT 0,
    positions_closed INTEGER DEFAULT 0,

    -- P&L
    gross_profit DECIMAL(20,8) DEFAULT 0,
    fees_paid DECIMAL(20,8) DEFAULT 0,
    net_profit DECIMAL(20,8) DEFAULT 0,

    -- Timestamps
    started_at TIMESTAMPTZ DEFAULT NOW(),
    ended_at TIMESTAMPTZ
);

-- =============================================================================
-- HELPER FUNCTIONS
-- =============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger for markets table
DROP TRIGGER IF EXISTS update_markets_updated_at ON markets;
CREATE TRIGGER update_markets_updated_at
    BEFORE UPDATE ON markets
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- =============================================================================
-- COMMENTS
-- =============================================================================

COMMENT ON TABLE markets IS 'Prediction markets discovered from Gamma API';
COMMENT ON TABLE orderbook_snapshots IS 'Real-time orderbook snapshots from CLOB WebSocket';
COMMENT ON TABLE positions IS 'Trading positions (spread arbitrage)';
COMMENT ON TABLE trades IS 'Individual trade executions';
COMMENT ON TABLE bot_sessions IS 'Bot session statistics and P&L tracking';

COMMENT ON COLUMN orderbook_snapshots.spread IS 'Cost to buy both YES and NO (< 1.0 is profitable)';
COMMENT ON COLUMN orderbook_snapshots.yes_asks IS 'Full orderbook depth: [{price, size}, ...]';
