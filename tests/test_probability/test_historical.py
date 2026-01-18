"""Tests for historical base rate calibration."""

from decimal import Decimal

from pylo.probability.historical import (
    DEFAULT_CALIBRATION,
    CalibrationBucket,
    CalibrationTable,
    HistoricalCalibrator,
    MomentumBucket,
)


class TestMomentumBucket:
    """Tests for MomentumBucket enum."""

    def test_bucket_values(self):
        """Test bucket enum values."""
        assert MomentumBucket.STRONG_BEARISH.value == "strong_bearish"
        assert MomentumBucket.BEARISH.value == "bearish"
        assert MomentumBucket.NEUTRAL.value == "neutral"
        assert MomentumBucket.BULLISH.value == "bullish"
        assert MomentumBucket.STRONG_BULLISH.value == "strong_bullish"


class TestCalibrationBucket:
    """Tests for CalibrationBucket dataclass."""

    def test_creation(self):
        """Test creating a calibration bucket."""
        bucket = CalibrationBucket(
            bucket=MomentumBucket.BULLISH,
            win_rate_up=Decimal("0.62"),
            sample_count=100,
            asset="BTC",
            timeframe="15min",
        )

        assert bucket.win_rate_up == Decimal("0.62")
        assert bucket.win_rate_down == Decimal("0.38")
        assert bucket.sample_count == 100

    def test_win_rate_down(self):
        """Test win_rate_down property."""
        bucket = CalibrationBucket(
            bucket=MomentumBucket.STRONG_BEARISH,
            win_rate_up=Decimal("0.25"),
            sample_count=50,
            asset="ETH",
            timeframe="15min",
        )

        assert bucket.win_rate_down == Decimal("0.75")


class TestCalibrationTable:
    """Tests for CalibrationTable dataclass."""

    def test_get_base_rate_strong_bullish(self):
        """Test getting base rate for strong bullish momentum."""
        table = CalibrationTable(
            asset="BTC",
            timeframe="15min",
            buckets={
                MomentumBucket.STRONG_BULLISH: CalibrationBucket(
                    bucket=MomentumBucket.STRONG_BULLISH,
                    win_rate_up=Decimal("0.75"),
                    sample_count=100,
                    asset="BTC",
                    timeframe="15min",
                ),
            },
        )

        # >2% momentum should use STRONG_BULLISH
        rate = table.get_base_rate(Decimal("0.03"))
        assert rate == Decimal("0.75")

    def test_get_base_rate_bullish(self):
        """Test getting base rate for bullish momentum."""
        table = CalibrationTable(
            asset="BTC",
            timeframe="15min",
            buckets={
                MomentumBucket.BULLISH: CalibrationBucket(
                    bucket=MomentumBucket.BULLISH,
                    win_rate_up=Decimal("0.62"),
                    sample_count=100,
                    asset="BTC",
                    timeframe="15min",
                ),
            },
        )

        # 0.5% to 2% momentum should use BULLISH
        rate = table.get_base_rate(Decimal("0.01"))
        assert rate == Decimal("0.62")

    def test_get_base_rate_neutral(self):
        """Test getting base rate for neutral momentum."""
        table = CalibrationTable(
            asset="BTC",
            timeframe="15min",
            buckets={
                MomentumBucket.NEUTRAL: CalibrationBucket(
                    bucket=MomentumBucket.NEUTRAL,
                    win_rate_up=Decimal("0.50"),
                    sample_count=100,
                    asset="BTC",
                    timeframe="15min",
                ),
            },
        )

        # -0.5% to 0.5% momentum should use NEUTRAL
        rate = table.get_base_rate(Decimal("0.002"))
        assert rate == Decimal("0.50")

    def test_get_base_rate_bearish(self):
        """Test getting base rate for bearish momentum."""
        table = CalibrationTable(
            asset="BTC",
            timeframe="15min",
            buckets={
                MomentumBucket.BEARISH: CalibrationBucket(
                    bucket=MomentumBucket.BEARISH,
                    win_rate_up=Decimal("0.38"),
                    sample_count=100,
                    asset="BTC",
                    timeframe="15min",
                ),
            },
        )

        # -2% to -0.5% momentum should use BEARISH
        rate = table.get_base_rate(Decimal("-0.01"))
        assert rate == Decimal("0.38")

    def test_get_base_rate_strong_bearish(self):
        """Test getting base rate for strong bearish momentum."""
        table = CalibrationTable(
            asset="BTC",
            timeframe="15min",
            buckets={
                MomentumBucket.STRONG_BEARISH: CalibrationBucket(
                    bucket=MomentumBucket.STRONG_BEARISH,
                    win_rate_up=Decimal("0.25"),
                    sample_count=100,
                    asset="BTC",
                    timeframe="15min",
                ),
            },
        )

        # <-2% momentum should use STRONG_BEARISH
        rate = table.get_base_rate(Decimal("-0.03"))
        assert rate == Decimal("0.25")

    def test_get_base_rate_missing_bucket(self):
        """Test default 50% when bucket is missing."""
        table = CalibrationTable(
            asset="BTC",
            timeframe="15min",
            buckets={},  # No buckets
        )

        rate = table.get_base_rate(Decimal("0.01"))
        assert rate == Decimal("0.5")


class TestHistoricalCalibrator:
    """Tests for HistoricalCalibrator class."""

    def test_init(self):
        """Test initialization."""
        calibrator = HistoricalCalibrator()
        assert calibrator._calibration_cache == {}

    def test_get_default_calibration_btc(self):
        """Test getting default calibration for BTC."""
        calibrator = HistoricalCalibrator()
        table = calibrator.get_default_calibration("BTC", "15min")

        assert table.asset == "BTC"
        assert table.timeframe == "15min"
        assert len(table.buckets) == 5  # All 5 buckets

        # Check BTC-specific values from DEFAULT_CALIBRATION
        assert table.buckets[MomentumBucket.STRONG_BULLISH].win_rate_up == Decimal("0.75")
        assert table.buckets[MomentumBucket.BULLISH].win_rate_up == Decimal("0.62")
        assert table.buckets[MomentumBucket.NEUTRAL].win_rate_up == Decimal("0.50")
        assert table.buckets[MomentumBucket.BEARISH].win_rate_up == Decimal("0.38")
        assert table.buckets[MomentumBucket.STRONG_BEARISH].win_rate_up == Decimal("0.25")

    def test_get_default_calibration_eth(self):
        """Test getting default calibration for ETH."""
        calibrator = HistoricalCalibrator()
        table = calibrator.get_default_calibration("ETH", "15min")

        assert table.asset == "ETH"
        # ETH values are different from BTC
        assert table.buckets[MomentumBucket.STRONG_BULLISH].win_rate_up == Decimal("0.72")

    def test_get_default_calibration_unknown_asset(self):
        """Test unknown asset falls back to BTC defaults."""
        calibrator = HistoricalCalibrator()
        table = calibrator.get_default_calibration("DOGE", "15min")

        assert table.asset == "DOGE"
        # Should use BTC defaults
        assert table.buckets[MomentumBucket.STRONG_BULLISH].win_rate_up == Decimal("0.75")

    def test_caching(self):
        """Test that calibration tables are cached."""
        calibrator = HistoricalCalibrator()

        table1 = calibrator.get_default_calibration("BTC", "15min")
        table2 = calibrator.get_default_calibration("BTC", "15min")

        assert table1 is table2  # Same object from cache

    def test_clear_cache(self):
        """Test clearing the cache."""
        calibrator = HistoricalCalibrator()

        calibrator.get_default_calibration("BTC", "15min")
        assert len(calibrator._calibration_cache) > 0

        calibrator.clear_cache()
        assert len(calibrator._calibration_cache) == 0

    def test_lookup_base_rate(self):
        """Test quick lookup of base rate."""
        calibrator = HistoricalCalibrator()

        # Bullish momentum
        rate = calibrator.lookup_base_rate("BTC", "15min", Decimal("0.015"))
        assert rate == Decimal("0.62")

        # Bearish momentum
        rate = calibrator.lookup_base_rate("BTC", "15min", Decimal("-0.015"))
        assert rate == Decimal("0.38")


class TestDefaultCalibration:
    """Tests for DEFAULT_CALIBRATION constant."""

    def test_btc_calibration(self):
        """Test BTC default calibration values."""
        btc = DEFAULT_CALIBRATION["BTC"]

        assert btc[MomentumBucket.STRONG_BEARISH] == Decimal("0.25")
        assert btc[MomentumBucket.BEARISH] == Decimal("0.38")
        assert btc[MomentumBucket.NEUTRAL] == Decimal("0.50")
        assert btc[MomentumBucket.BULLISH] == Decimal("0.62")
        assert btc[MomentumBucket.STRONG_BULLISH] == Decimal("0.75")

    def test_eth_calibration(self):
        """Test ETH default calibration values."""
        eth = DEFAULT_CALIBRATION["ETH"]

        assert eth[MomentumBucket.STRONG_BEARISH] == Decimal("0.28")
        assert eth[MomentumBucket.BULLISH] == Decimal("0.60")

    def test_sol_calibration(self):
        """Test SOL default calibration values."""
        sol = DEFAULT_CALIBRATION["SOL"]

        assert sol[MomentumBucket.STRONG_BEARISH] == Decimal("0.30")
        assert sol[MomentumBucket.STRONG_BULLISH] == Decimal("0.70")

    def test_calibration_symmetry(self):
        """Test that calibration values are roughly symmetric around 0.50."""
        for asset, buckets in DEFAULT_CALIBRATION.items():
            # Strong bullish + strong bearish should average ~0.50
            avg = (buckets[MomentumBucket.STRONG_BULLISH] + buckets[MomentumBucket.STRONG_BEARISH]) / 2
            assert Decimal("0.45") <= avg <= Decimal("0.55"), f"{asset} asymmetric"
