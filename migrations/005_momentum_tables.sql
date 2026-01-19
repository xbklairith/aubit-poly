-- Migration: Add momentum trading tables
-- Description: Tables for tracking momentum signals and trades

-- Momentum signals for analytics
CREATE TABLE IF NOT EXISTS momentum_signals (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset VARCHAR(10) NOT NULL,
    direction VARCHAR(4) NOT NULL,  -- 'UP' or 'DOWN'
    momentum_pct DECIMAL(10, 6) NOT NULL,
    binance_price DECIMAL(20, 8) NOT NULL,
    polymarket_price DECIMAL(10, 4),
    market_id UUID REFERENCES markets(id),
    condition_id TEXT,
    expiry_secs INTEGER,
    status VARCHAR(20) DEFAULT 'detected',  -- detected, traded, skipped, failed
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_momentum_signals_asset_created ON momentum_signals(asset, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_momentum_signals_created ON momentum_signals(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_momentum_signals_status ON momentum_signals(status);

-- Momentum trades for P&L tracking
CREATE TABLE IF NOT EXISTS momentum_trades (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    signal_id UUID REFERENCES momentum_signals(id),
    market_id UUID REFERENCES markets(id),
    side VARCHAR(3) NOT NULL,  -- 'YES' or 'NO'
    entry_price DECIMAL(10, 4) NOT NULL,
    estimated_fill_price DECIMAL(10, 4),  -- From orderbook depth
    shares DECIMAL(10, 2) NOT NULL,
    order_id TEXT,
    is_dry_run BOOLEAN DEFAULT true,
    winning_side VARCHAR(3),  -- populated after resolution
    pnl DECIMAL(12, 4),  -- populated after resolution
    latency_ms INTEGER,  -- Detection to order time
    created_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_momentum_trades_market ON momentum_trades(market_id);
CREATE INDEX IF NOT EXISTS idx_momentum_trades_created ON momentum_trades(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_momentum_trades_dry_run ON momentum_trades(is_dry_run);
