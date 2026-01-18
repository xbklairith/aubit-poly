"""Tests for momentum-based probability calculation."""

from datetime import UTC, datetime
from decimal import Decimal

from pylo.data_sources.crypto.binance_klines import Candle
from pylo.probability.momentum import MomentumCalculator, MomentumSignal


class TestMomentumCalculator:
    """Tests for MomentumCalculator class."""

    def test_init_default_params(self):
        """Test initialization with default parameters."""
        calc = MomentumCalculator()
        assert calc.recent_weight == 0.6
        assert calc.min_candles == 5

    def test_init_custom_params(self):
        """Test initialization with custom parameters."""
        calc = MomentumCalculator(recent_weight=0.8, min_candles=10)
        assert calc.recent_weight == 0.8
        assert calc.min_candles == 10

    def test_calculate_insufficient_candles(self):
        """Test calculation with insufficient candles returns None."""
        calc = MomentumCalculator(min_candles=5)
        candles = [self._make_candle(100, 101) for _ in range(3)]

        result = calc.calculate_from_candles(candles, "BTC", 15)

        assert result is None

    def test_calculate_bullish_momentum(self):
        """Test calculation with bullish (upward) momentum."""
        calc = MomentumCalculator()

        # Create 10 consistently bullish candles (each closes 1% higher)
        candles = []
        price = Decimal("100")
        for _ in range(10):
            close = price * Decimal("1.01")
            candles.append(self._make_candle(float(price), float(close)))
            price = close

        result = calc.calculate_from_candles(candles, "BTC", 15)

        assert result is not None
        assert result.probability_up > Decimal("0.5")
        assert result.is_bullish
        assert result.momentum_score > Decimal("0")
        assert result.asset == "BTC"
        assert result.interval_minutes == 15

    def test_calculate_bearish_momentum(self):
        """Test calculation with bearish (downward) momentum."""
        calc = MomentumCalculator()

        # Create 10 consistently bearish candles (each closes 1% lower)
        candles = []
        price = Decimal("100")
        for _ in range(10):
            close = price * Decimal("0.99")
            candles.append(self._make_candle(float(price), float(close)))
            price = close

        result = calc.calculate_from_candles(candles, "BTC", 15)

        assert result is not None
        assert result.probability_up < Decimal("0.5")
        assert not result.is_bullish
        assert result.momentum_score < Decimal("0")

    def test_calculate_neutral_momentum(self):
        """Test calculation with neutral/flat momentum."""
        calc = MomentumCalculator()

        # Create 10 flat candles (open = close)
        candles = [self._make_candle(100, 100) for _ in range(10)]

        result = calc.calculate_from_candles(candles, "BTC", 15)

        assert result is not None
        # Should be close to 50% with low confidence
        assert Decimal("0.4") <= result.probability_up <= Decimal("0.6")
        assert result.confidence < Decimal("0.5")

    def test_calculate_mixed_momentum(self):
        """Test calculation with mixed up/down candles."""
        calc = MomentumCalculator()

        # Create alternating candles
        candles = []
        for i in range(10):
            if i % 2 == 0:
                candles.append(self._make_candle(100, 101))  # Bullish
            else:
                candles.append(self._make_candle(100, 99))  # Bearish

        result = calc.calculate_from_candles(candles, "ETH", 15)

        assert result is not None
        # Should be close to 50%
        assert Decimal("0.3") <= result.probability_up <= Decimal("0.7")

    def test_probability_down(self):
        """Test probability_down property."""
        calc = MomentumCalculator()
        candles = [self._make_candle(100, 101) for _ in range(10)]

        result = calc.calculate_from_candles(candles, "BTC", 15)

        assert result is not None
        assert result.probability_down == Decimal("1") - result.probability_up

    def test_is_strong_signal(self):
        """Test is_strong_signal property."""
        calc = MomentumCalculator()

        # Strong bullish
        candles = []
        price = Decimal("100")
        for _ in range(10):
            close = price * Decimal("1.02")  # 2% up each candle
            candles.append(self._make_candle(float(price), float(close)))
            price = close

        result = calc.calculate_from_candles(candles, "BTC", 15)

        assert result is not None
        assert result.is_strong_signal  # >60% or <40%

    def test_trend_consistency(self):
        """Test trend consistency calculation."""
        calc = MomentumCalculator()

        # All bullish candles with varying returns (to avoid zero volatility)
        candles = []
        for i in range(10):
            # Vary returns between 0.5% and 1.5% to have non-zero volatility
            close_pct = 1.005 + (i % 3) * 0.005  # 1.005, 1.010, 1.015, repeat
            candles.append(self._make_candle(100, float(100 * close_pct)))

        result = calc.calculate_from_candles(candles, "BTC", 15)

        assert result is not None
        assert result.trend_consistency == Decimal("1.0")

    @staticmethod
    def _make_candle(open_price: float, close_price: float) -> Candle:
        """Create a test candle."""
        now = datetime.now(UTC)
        return Candle(
            open_time=now,
            open=Decimal(str(open_price)),
            high=Decimal(str(max(open_price, close_price) * 1.001)),
            low=Decimal(str(min(open_price, close_price) * 0.999)),
            close=Decimal(str(close_price)),
            volume=Decimal("1000"),
            close_time=now,
            quote_volume=Decimal("100000"),
            trades=100,
        )


class TestMomentumSignal:
    """Tests for MomentumSignal dataclass."""

    def test_signal_creation(self):
        """Test creating a MomentumSignal."""
        signal = MomentumSignal(
            probability_up=Decimal("0.65"),
            confidence=Decimal("0.8"),
            momentum_score=Decimal("0.5"),
            volatility=Decimal("0.01"),
            trend_consistency=Decimal("0.8"),
            sample_size=10,
            asset="BTC",
            interval_minutes=15,
            timestamp=datetime.now(UTC),
        )

        assert signal.probability_up == Decimal("0.65")
        assert signal.probability_down == Decimal("0.35")
        assert signal.is_bullish
        assert signal.is_strong_signal

    def test_signal_bearish(self):
        """Test bearish signal."""
        signal = MomentumSignal(
            probability_up=Decimal("0.35"),
            confidence=Decimal("0.7"),
            momentum_score=Decimal("-0.5"),
            volatility=Decimal("0.02"),
            trend_consistency=Decimal("0.3"),
            sample_size=10,
            asset="ETH",
            interval_minutes=15,
            timestamp=datetime.now(UTC),
        )

        assert not signal.is_bullish
        assert signal.is_strong_signal

    def test_signal_neutral(self):
        """Test neutral signal."""
        signal = MomentumSignal(
            probability_up=Decimal("0.52"),
            confidence=Decimal("0.5"),
            momentum_score=Decimal("0.05"),
            volatility=Decimal("0.01"),
            trend_consistency=Decimal("0.5"),
            sample_size=10,
            asset="SOL",
            interval_minutes=15,
            timestamp=datetime.now(UTC),
        )

        assert signal.is_bullish  # Slightly bullish
        assert not signal.is_strong_signal  # But not strong
