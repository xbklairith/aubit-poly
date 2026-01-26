-- Migration: 006_kalshi_support
-- Description: Add Kalshi platform support for cross-platform arbitrage
-- Created: 2025-01-23

-- =============================================================================
-- PLATFORM COLUMN FOR MARKETS TABLE
-- =============================================================================
-- Add platform column to distinguish between Polymarket and Kalshi markets

-- Add platform column with default 'polymarket' for existing records
ALTER TABLE markets ADD COLUMN IF NOT EXISTS platform VARCHAR(20) DEFAULT 'polymarket' NOT NULL;

-- Add Kalshi-specific columns (nullable for Polymarket)
ALTER TABLE markets ADD COLUMN IF NOT EXISTS rules_primary TEXT;
ALTER TABLE markets ADD COLUMN IF NOT EXISTS liquidity_dollars DECIMAL(20,8);
ALTER TABLE markets ADD COLUMN IF NOT EXISTS volume_24h DECIMAL(20,8);
ALTER TABLE markets ADD COLUMN IF NOT EXISTS strike_price BIGINT;
ALTER TABLE markets ADD COLUMN IF NOT EXISTS direction VARCHAR(10);

-- =============================================================================
-- FIX UNIQUE CONSTRAINT
-- =============================================================================
-- Current: UNIQUE(condition_id) - blocks multi-platform storage
-- New: UNIQUE(platform, condition_id) - allows same market_id on different platforms

-- Drop old unique constraint on condition_id
ALTER TABLE markets DROP CONSTRAINT IF EXISTS markets_condition_id_key;

-- Create composite unique constraint
ALTER TABLE markets ADD CONSTRAINT markets_platform_condition_id_unique
    UNIQUE (platform, condition_id);

-- =============================================================================
-- NEW INDEXES FOR PLATFORM FILTERING
-- =============================================================================

-- Index for platform filtering with end_time ordering
CREATE INDEX IF NOT EXISTS idx_markets_platform_endtime
    ON markets(platform, end_time ASC)
    WHERE is_active = true;

-- Index for cross-platform queries (same asset across platforms)
CREATE INDEX IF NOT EXISTS idx_markets_asset_platform
    ON markets(asset, platform, end_time ASC)
    WHERE is_active = true;

-- Index for timeframe filtering (useful for matching 15m markets)
CREATE INDEX IF NOT EXISTS idx_markets_timeframe_platform
    ON markets(timeframe, platform, end_time ASC)
    WHERE is_active = true;

-- =============================================================================
-- CROSS-PLATFORM MATCHES TABLE
-- =============================================================================
-- Cache high-confidence matches between Polymarket and Kalshi markets

CREATE TABLE IF NOT EXISTS cross_platform_matches (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- References to both platforms' markets
    polymarket_id UUID NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
    kalshi_id UUID NOT NULL REFERENCES markets(id) ON DELETE CASCADE,

    -- Match metadata
    match_confidence DECIMAL(5,4) NOT NULL,  -- 0.0000 to 1.0000
    match_reason TEXT,

    -- Common entity attributes (for verification)
    entity_asset VARCHAR(20),
    entity_timeframe VARCHAR(20),
    entity_direction VARCHAR(20),  -- 'up', 'down', 'above', 'below'

    -- Timestamps
    discovered_at TIMESTAMPTZ DEFAULT NOW(),
    validated_at TIMESTAMPTZ,
    invalidated_at TIMESTAMPTZ,

    -- Prevent duplicate matches
    UNIQUE(polymarket_id, kalshi_id)
);

-- Index for finding matches by confidence
CREATE INDEX IF NOT EXISTS idx_matches_confidence
    ON cross_platform_matches(match_confidence DESC)
    WHERE match_confidence >= 0.9 AND invalidated_at IS NULL;

-- Index for finding matches for a specific market
CREATE INDEX IF NOT EXISTS idx_matches_polymarket
    ON cross_platform_matches(polymarket_id)
    WHERE invalidated_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_matches_kalshi
    ON cross_platform_matches(kalshi_id)
    WHERE invalidated_at IS NULL;

-- =============================================================================
-- CROSS-PLATFORM ARBITRAGE OPPORTUNITIES TABLE
-- =============================================================================
-- Track detected arbitrage opportunities between platforms

CREATE TABLE IF NOT EXISTS cross_platform_opportunities (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Match reference
    match_id UUID NOT NULL REFERENCES cross_platform_matches(id) ON DELETE CASCADE,

    -- Which platform to buy each side
    buy_yes_platform VARCHAR(20) NOT NULL,  -- 'polymarket' or 'kalshi'
    buy_no_platform VARCHAR(20) NOT NULL,

    -- Prices at detection time (dollars 0-1)
    yes_price DECIMAL(10,6) NOT NULL,
    no_price DECIMAL(10,6) NOT NULL,
    total_cost DECIMAL(10,6) NOT NULL,

    -- Profit calculations
    gross_profit_pct DECIMAL(10,6) NOT NULL,
    net_profit_pct DECIMAL(10,6) NOT NULL,  -- After fees

    -- Liquidity at detection
    min_liquidity DECIMAL(20,8),

    -- Timestamps
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ,  -- Market close time

    -- Status
    status VARCHAR(20) DEFAULT 'detected'  -- 'detected', 'stale', 'executed', 'missed'
);

-- Index for finding active opportunities
CREATE INDEX IF NOT EXISTS idx_opportunities_active
    ON cross_platform_opportunities(detected_at DESC)
    WHERE status = 'detected';

-- Index for finding opportunities by profit
CREATE INDEX IF NOT EXISTS idx_opportunities_profit
    ON cross_platform_opportunities(net_profit_pct DESC)
    WHERE status = 'detected';

-- =============================================================================
-- COMMENTS
-- =============================================================================

COMMENT ON COLUMN markets.platform IS 'Platform source: polymarket or kalshi';
COMMENT ON COLUMN markets.rules_primary IS 'Kalshi market resolution rules';
COMMENT ON COLUMN markets.liquidity_dollars IS 'Market liquidity in USD';
COMMENT ON COLUMN markets.strike_price IS 'Price target for above/below markets (cents or basis)';
COMMENT ON COLUMN markets.direction IS 'Market direction: up, down, above, below';

COMMENT ON TABLE cross_platform_matches IS 'Cached matches between equivalent markets on different platforms';
COMMENT ON COLUMN cross_platform_matches.match_confidence IS 'Confidence score 0-1 that these markets are equivalent';
COMMENT ON COLUMN cross_platform_matches.match_reason IS 'Human-readable explanation of why markets match';

COMMENT ON TABLE cross_platform_opportunities IS 'Detected arbitrage opportunities between platforms';
COMMENT ON COLUMN cross_platform_opportunities.net_profit_pct IS 'Expected profit after platform fees (Kalshi ~1%)';
