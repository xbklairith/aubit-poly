-- Migration: Add separate timestamps for YES and NO orderbook updates
-- This allows tracking when each side was last updated independently,
-- solving the staleness issue where YES/NO arrive at different times.

-- Add columns for side-specific event timestamps (from Polymarket WebSocket)
ALTER TABLE orderbook_snapshots
ADD COLUMN IF NOT EXISTS yes_updated_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS no_updated_at TIMESTAMPTZ;

-- Backfill existing rows with captured_at as initial value
UPDATE orderbook_snapshots
SET yes_updated_at = captured_at,
    no_updated_at = captured_at
WHERE yes_updated_at IS NULL OR no_updated_at IS NULL;

-- Add index for freshness queries (find stale sides)
CREATE INDEX IF NOT EXISTS idx_snapshots_yes_updated
    ON orderbook_snapshots(yes_updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_snapshots_no_updated
    ON orderbook_snapshots(no_updated_at DESC);
