"""Historical base rate calibration for probability estimation."""

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class MomentumBucket(str, Enum):
    """Momentum categories for calibration lookup."""

    STRONG_BEARISH = "strong_bearish"  # < -2% momentum
    BEARISH = "bearish"                 # -2% to -0.5%
    NEUTRAL = "neutral"                 # -0.5% to +0.5%
    BULLISH = "bullish"                 # +0.5% to +2%
    STRONG_BULLISH = "strong_bullish"  # > +2%


@dataclass
class CalibrationBucket:
    """Historical win rate for a momentum bucket."""

    bucket: MomentumBucket
    win_rate_up: Decimal  # How often UP wins in this bucket
    sample_count: int
    asset: str
    timeframe: str

    @property
    def win_rate_down(self) -> Decimal:
        """How often DOWN wins in this bucket."""
        return Decimal("1") - self.win_rate_up


@dataclass
class CalibrationTable:
    """Full calibration table for an asset/timeframe."""

    asset: str
    timeframe: str
    buckets: dict[MomentumBucket, CalibrationBucket] = field(default_factory=dict)
    total_samples: int = 0

    def get_base_rate(self, momentum_pct: Decimal) -> Decimal:
        """
        Get historical base rate for a given momentum percentage.

        Args:
            momentum_pct: Momentum as percentage (e.g., 0.015 = 1.5%)

        Returns:
            Historical win rate for UP
        """
        bucket = self._momentum_to_bucket(momentum_pct)
        if bucket in self.buckets:
            return self.buckets[bucket].win_rate_up
        # Default to 50% if no data
        return Decimal("0.5")

    @staticmethod
    def _momentum_to_bucket(momentum_pct: Decimal) -> MomentumBucket:
        """Convert momentum percentage to bucket category."""
        if momentum_pct < Decimal("-0.02"):
            return MomentumBucket.STRONG_BEARISH
        elif momentum_pct < Decimal("-0.005"):
            return MomentumBucket.BEARISH
        elif momentum_pct <= Decimal("0.005"):
            return MomentumBucket.NEUTRAL
        elif momentum_pct <= Decimal("0.02"):
            return MomentumBucket.BULLISH
        else:
            return MomentumBucket.STRONG_BULLISH


# Default calibration based on typical crypto market behavior
# These are starting estimates that should be updated with real data
DEFAULT_CALIBRATION: dict[str, dict[MomentumBucket, Decimal]] = {
    "BTC": {
        MomentumBucket.STRONG_BEARISH: Decimal("0.25"),
        MomentumBucket.BEARISH: Decimal("0.38"),
        MomentumBucket.NEUTRAL: Decimal("0.50"),
        MomentumBucket.BULLISH: Decimal("0.62"),
        MomentumBucket.STRONG_BULLISH: Decimal("0.75"),
    },
    "ETH": {
        MomentumBucket.STRONG_BEARISH: Decimal("0.28"),
        MomentumBucket.BEARISH: Decimal("0.40"),
        MomentumBucket.NEUTRAL: Decimal("0.50"),
        MomentumBucket.BULLISH: Decimal("0.60"),
        MomentumBucket.STRONG_BULLISH: Decimal("0.72"),
    },
    "SOL": {
        MomentumBucket.STRONG_BEARISH: Decimal("0.30"),
        MomentumBucket.BEARISH: Decimal("0.42"),
        MomentumBucket.NEUTRAL: Decimal("0.50"),
        MomentumBucket.BULLISH: Decimal("0.58"),
        MomentumBucket.STRONG_BULLISH: Decimal("0.70"),
    },
}


class HistoricalCalibrator:
    """Build and query calibration tables from historical data."""

    def __init__(self) -> None:
        """Initialize the calibrator."""
        self.logger = logging.getLogger(f"{__name__}.HistoricalCalibrator")
        self._calibration_cache: dict[str, CalibrationTable] = {}

    def get_default_calibration(
        self,
        asset: str,
        timeframe: str = "15min",
    ) -> CalibrationTable:
        """
        Get default calibration table for an asset.

        Args:
            asset: Asset name (BTC, ETH, SOL)
            timeframe: Market timeframe

        Returns:
            CalibrationTable with default values
        """
        cache_key = f"{asset}_{timeframe}"
        if cache_key in self._calibration_cache:
            return self._calibration_cache[cache_key]

        # Get asset-specific defaults or use BTC as fallback
        asset_defaults = DEFAULT_CALIBRATION.get(
            asset.upper(),
            DEFAULT_CALIBRATION["BTC"]
        )

        buckets = {}
        for bucket, win_rate in asset_defaults.items():
            buckets[bucket] = CalibrationBucket(
                bucket=bucket,
                win_rate_up=win_rate,
                sample_count=0,  # Default values, no real samples
                asset=asset,
                timeframe=timeframe,
            )

        table = CalibrationTable(
            asset=asset,
            timeframe=timeframe,
            buckets=buckets,
            total_samples=0,
        )

        self._calibration_cache[cache_key] = table
        return table

    async def build_calibration_from_db(
        self,
        session: AsyncSession,
        asset: str,
        timeframe: str = "15min",
    ) -> CalibrationTable | None:
        """
        Build calibration table from database historical data.

        This queries the market_resolutions table (if exists) to get
        actual win rates for different momentum buckets.

        Args:
            session: Database session
            asset: Asset name
            timeframe: Market timeframe

        Returns:
            CalibrationTable built from historical data, or None if no data
        """
        cache_key = f"{asset}_{timeframe}_db"
        if cache_key in self._calibration_cache:
            return self._calibration_cache[cache_key]

        try:
            # Query to get win rates by momentum bucket
            # This assumes a market_resolutions table exists with momentum data
            query = text("""
                SELECT
                    CASE
                        WHEN momentum_pct < -0.02 THEN 'strong_bearish'
                        WHEN momentum_pct < -0.005 THEN 'bearish'
                        WHEN momentum_pct <= 0.005 THEN 'neutral'
                        WHEN momentum_pct <= 0.02 THEN 'bullish'
                        ELSE 'strong_bullish'
                    END as bucket,
                    COUNT(*) as total,
                    SUM(CASE WHEN winning_side = 'yes' THEN 1 ELSE 0 END) as up_wins
                FROM market_resolutions
                WHERE asset = :asset
                  AND timeframe = :timeframe
                  AND momentum_pct IS NOT NULL
                GROUP BY bucket
            """)

            result = await session.execute(
                query,
                {"asset": asset.upper(), "timeframe": timeframe}
            )
            rows = result.fetchall()

            if not rows:
                self.logger.info(
                    f"No historical data for {asset} {timeframe}, using defaults"
                )
                return self.get_default_calibration(asset, timeframe)

            buckets = {}
            total_samples = 0

            bucket_map = {
                "strong_bearish": MomentumBucket.STRONG_BEARISH,
                "bearish": MomentumBucket.BEARISH,
                "neutral": MomentumBucket.NEUTRAL,
                "bullish": MomentumBucket.BULLISH,
                "strong_bullish": MomentumBucket.STRONG_BULLISH,
            }

            for row in rows:
                bucket_name, total, up_wins = row
                bucket = bucket_map.get(bucket_name)
                if bucket and total > 0:
                    win_rate = Decimal(str(up_wins)) / Decimal(str(total))
                    buckets[bucket] = CalibrationBucket(
                        bucket=bucket,
                        win_rate_up=win_rate.quantize(Decimal("0.0001")),
                        sample_count=total,
                        asset=asset,
                        timeframe=timeframe,
                    )
                    total_samples += total

            # Fill in missing buckets with defaults
            default_table = self.get_default_calibration(asset, timeframe)
            for bucket in MomentumBucket:
                if bucket not in buckets:
                    buckets[bucket] = default_table.buckets[bucket]

            table = CalibrationTable(
                asset=asset,
                timeframe=timeframe,
                buckets=buckets,
                total_samples=total_samples,
            )

            self._calibration_cache[cache_key] = table
            self.logger.info(
                f"Built calibration for {asset} {timeframe} "
                f"from {total_samples} samples"
            )
            return table

        except Exception as e:
            self.logger.warning(
                f"Failed to build calibration from DB: {e}. Using defaults."
            )
            return self.get_default_calibration(asset, timeframe)

    def lookup_base_rate(
        self,
        asset: str,
        timeframe: str,
        momentum_pct: Decimal,
    ) -> Decimal:
        """
        Quick lookup of base rate for given parameters.

        Args:
            asset: Asset name
            timeframe: Market timeframe
            momentum_pct: Momentum percentage

        Returns:
            Historical base rate for UP winning
        """
        table = self.get_default_calibration(asset, timeframe)
        return table.get_base_rate(momentum_pct)

    def clear_cache(self) -> None:
        """Clear the calibration cache."""
        self._calibration_cache.clear()
