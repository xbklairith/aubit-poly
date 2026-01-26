-- Migration: 007_limitless_support
-- Description: Add Limitless Exchange platform support for cross-platform arbitrage
-- Created: 2025-01-26
--
-- Limitless Exchange is a Polymarket fork on Base L2 with:
-- - No KYC required (global access)
-- - Hourly crypto markets (15m coming soon)
-- - $750M+ volume traded
-- - 0% trading fees
-- - WebSocket orderbook support

-- =============================================================================
-- UPDATE PLATFORM COLUMN COMMENTS
-- =============================================================================
-- Now supports: polymarket, kalshi, limitless

COMMENT ON COLUMN markets.platform IS 'Platform source: polymarket, kalshi, or limitless';

-- =============================================================================
-- ADD SETTLEMENT CHAIN TRACKING
-- =============================================================================
-- Different platforms settle on different chains:
-- - Polymarket: Polygon (MATIC)
-- - Kalshi: Centralized (USD)
-- - Limitless: Base L2 (ETH)

ALTER TABLE markets ADD COLUMN IF NOT EXISTS settlement_chain VARCHAR(20);

-- Update existing records with known chains
UPDATE markets SET settlement_chain = 'polygon' WHERE platform = 'polymarket' AND settlement_chain IS NULL;
UPDATE markets SET settlement_chain = 'centralized' WHERE platform = 'kalshi' AND settlement_chain IS NULL;

COMMENT ON COLUMN markets.settlement_chain IS 'Blockchain for settlement: polygon, base, or centralized';

-- =============================================================================
-- ADD LIMITLESS-SPECIFIC NOTES
-- =============================================================================
-- Limitless position IDs are stored in yes_token_id / no_token_id columns
-- (same as Polymarket token IDs, reusing existing schema)

-- =============================================================================
-- INDEX FOR LIMITLESS MARKETS
-- =============================================================================

-- Index for Limitless-specific queries
CREATE INDEX IF NOT EXISTS idx_markets_limitless_active
    ON markets(end_time)
    WHERE platform = 'limitless' AND is_active = true;

-- Index for settlement chain queries (useful for cross-chain arbitrage planning)
CREATE INDEX IF NOT EXISTS idx_markets_settlement_chain
    ON markets(settlement_chain, platform)
    WHERE is_active = true;

-- =============================================================================
-- UPDATE CROSS-PLATFORM MATCHES TABLE
-- =============================================================================
-- The existing table uses polymarket_id and kalshi_id columns.
-- We need a more flexible schema for 3+ platforms.

-- Add optional limitless_id column
ALTER TABLE cross_platform_matches ADD COLUMN IF NOT EXISTS limitless_id UUID REFERENCES markets(id) ON DELETE CASCADE;

-- Add index for limitless matches
CREATE INDEX IF NOT EXISTS idx_matches_limitless
    ON cross_platform_matches(limitless_id)
    WHERE limitless_id IS NOT NULL AND invalidated_at IS NULL;

-- Update comment
COMMENT ON TABLE cross_platform_matches IS 'Cached matches between equivalent markets on different platforms (Polymarket, Kalshi, Limitless)';
COMMENT ON COLUMN cross_platform_matches.limitless_id IS 'Limitless market ID (NULL if match is Polymarket-Kalshi only)';

-- =============================================================================
-- ADD CONSTRAINT TO ENSURE AT LEAST TWO PLATFORMS IN A MATCH
-- =============================================================================
-- A match must have at least 2 platforms to be valid

ALTER TABLE cross_platform_matches DROP CONSTRAINT IF EXISTS match_has_two_platforms;
ALTER TABLE cross_platform_matches ADD CONSTRAINT match_has_two_platforms
    CHECK (
        (CASE WHEN polymarket_id IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN kalshi_id IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN limitless_id IS NOT NULL THEN 1 ELSE 0 END) >= 2
    );

-- =============================================================================
-- UPDATE OPPORTUNITY TABLE COMMENTS
-- =============================================================================

COMMENT ON COLUMN cross_platform_opportunities.buy_yes_platform IS 'Platform to buy YES: polymarket, kalshi, or limitless';
COMMENT ON COLUMN cross_platform_opportunities.buy_no_platform IS 'Platform to buy NO: polymarket, kalshi, or limitless';

-- =============================================================================
-- ADD PLATFORM-SPECIFIC STATS VIEW (OPTIONAL)
-- =============================================================================

CREATE OR REPLACE VIEW platform_market_stats AS
SELECT
    platform,
    settlement_chain,
    COUNT(*) as total_markets,
    COUNT(*) FILTER (WHERE is_active = true) as active_markets,
    COUNT(*) FILTER (WHERE is_active = true AND end_time > NOW()) as upcoming_markets,
    MIN(end_time) FILTER (WHERE is_active = true AND end_time > NOW()) as next_expiry,
    MAX(liquidity_dollars) as max_liquidity
FROM markets
GROUP BY platform, settlement_chain;

COMMENT ON VIEW platform_market_stats IS 'Summary statistics per platform for monitoring';
