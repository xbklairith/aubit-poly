-- Migration: 002_order_tracking
-- Description: Add order tracking columns for Polymarket order IDs and fill amounts
-- Created: 2025-01-10

-- =============================================================================
-- TRADES TABLE UPDATES
-- =============================================================================
-- Add columns to track Polymarket order IDs and actual fill amounts

-- Order ID from Polymarket CLOB API
ALTER TABLE trades ADD COLUMN IF NOT EXISTS order_id VARCHAR(255);

-- Actual filled amount (may differ from requested shares due to partial fills)
ALTER TABLE trades ADD COLUMN IF NOT EXISTS filled_shares DECIMAL(20,8);

-- Order status: 'pending', 'filled', 'partial', 'cancelled', 'failed'
ALTER TABLE trades ADD COLUMN IF NOT EXISTS order_status VARCHAR(20) DEFAULT 'pending';

-- Index for looking up trades by order_id (for reconciliation)
CREATE INDEX IF NOT EXISTS idx_trades_order_id ON trades(order_id) WHERE order_id IS NOT NULL;

-- =============================================================================
-- POSITIONS TABLE UPDATES
-- =============================================================================
-- Add columns to track actual filled amounts vs requested

-- Actual filled YES shares (may differ from yes_shares due to partial fills)
ALTER TABLE positions ADD COLUMN IF NOT EXISTS yes_filled DECIMAL(20,8);

-- Actual filled NO shares (may differ from no_shares due to partial fills)
ALTER TABLE positions ADD COLUMN IF NOT EXISTS no_filled DECIMAL(20,8);

-- =============================================================================
-- COMMENTS
-- =============================================================================
COMMENT ON COLUMN trades.order_id IS 'Polymarket CLOB order ID for tracking and reconciliation';
COMMENT ON COLUMN trades.filled_shares IS 'Actual filled amount from Polymarket (may differ from requested)';
COMMENT ON COLUMN trades.order_status IS 'Order status: pending, filled, partial, cancelled, failed';
COMMENT ON COLUMN positions.yes_filled IS 'Actual YES shares filled (for partial fill tracking)';
COMMENT ON COLUMN positions.no_filled IS 'Actual NO shares filled (for partial fill tracking)';
