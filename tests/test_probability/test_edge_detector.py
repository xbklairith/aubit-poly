"""Tests for edge detection module."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from pylo.bots.models import Asset, Timeframe, UpDownMarket
from pylo.probability.edge_detector import (
    EdgeDetector,
    EdgeSignal,
    expiry_confidence_multiplier,
)


class TestExpiryConfidenceMultiplier:
    """Tests for expiry_confidence_multiplier function."""

    def test_too_early_period(self):
        """Test >80% time remaining gives 0.6x multiplier."""
        # 15 min market, 14 min left = 93%
        result = expiry_confidence_multiplier(
            time_to_expiry_seconds=840,  # 14 min
            market_duration_seconds=900,  # 15 min
        )
        assert result == Decimal("0.6")

    def test_sweet_spot_period(self):
        """Test 40-80% time remaining gives 1.0x multiplier."""
        # 15 min market, 10 min left = 67%
        result = expiry_confidence_multiplier(
            time_to_expiry_seconds=600,  # 10 min
            market_duration_seconds=900,  # 15 min
        )
        assert result == Decimal("1.0")

    def test_late_period(self):
        """Test 20-40% time remaining gives 0.8x multiplier."""
        # 15 min market, 5 min left = 33%
        result = expiry_confidence_multiplier(
            time_to_expiry_seconds=300,  # 5 min
            market_duration_seconds=900,  # 15 min
        )
        assert result == Decimal("0.8")

    def test_near_expiry_period(self):
        """Test 7-20% time remaining gives 0.4x multiplier."""
        # 15 min market, 2 min left = 13%
        result = expiry_confidence_multiplier(
            time_to_expiry_seconds=120,  # 2 min
            market_duration_seconds=900,  # 15 min
        )
        assert result == Decimal("0.4")

    def test_very_near_expiry_period(self):
        """Test <7% time remaining gives 0.2x multiplier."""
        # 15 min market, 30s left = 3%
        result = expiry_confidence_multiplier(
            time_to_expiry_seconds=30,
            market_duration_seconds=900,
        )
        assert result == Decimal("0.2")

    def test_zero_duration(self):
        """Test zero market duration returns 0.5x."""
        result = expiry_confidence_multiplier(
            time_to_expiry_seconds=100,
            market_duration_seconds=0,
        )
        assert result == Decimal("0.5")


class TestEdgeDetector:
    """Tests for EdgeDetector class."""

    def test_init_default_params(self):
        """Test initialization with default parameters."""
        detector = EdgeDetector()
        assert detector.min_edge == Decimal("0.05")
        assert detector.min_confidence == Decimal("0.5")
        assert detector.fee_rate == Decimal("0.02")
        assert detector.kelly_fraction == Decimal("0.25")
        assert detector.max_position_pct == Decimal("0.10")

    def test_init_custom_params(self):
        """Test initialization with custom parameters."""
        detector = EdgeDetector(
            min_edge=Decimal("0.10"),
            min_confidence=Decimal("0.7"),
            fee_rate=Decimal("0.01"),
        )
        assert detector.min_edge == Decimal("0.10")
        assert detector.min_confidence == Decimal("0.7")
        assert detector.fee_rate == Decimal("0.01")

    def test_detect_edge_bullish(self):
        """Test detecting bullish edge."""
        detector = EdgeDetector(min_edge=Decimal("0.05"))
        market = self._make_market(yes_ask=Decimal("0.50"), no_ask=Decimal("0.50"))

        # True probability is 60% up (10% edge)
        signal = detector.detect_edge(
            market=market,
            estimated_prob_up=Decimal("0.60"),
            confidence=Decimal("0.8"),
            market_duration_seconds=900,
        )

        assert signal.edge_up == Decimal("0.10")
        assert signal.edge_down == Decimal("-0.10")
        assert signal.recommended_side == "UP"
        assert signal.has_edge
        assert signal.recommended_size > Decimal("0")

    def test_detect_edge_bearish(self):
        """Test detecting bearish edge."""
        detector = EdgeDetector(min_edge=Decimal("0.05"))
        market = self._make_market(yes_ask=Decimal("0.50"), no_ask=Decimal("0.50"))

        # True probability is 40% up = 60% down (10% edge on DOWN)
        signal = detector.detect_edge(
            market=market,
            estimated_prob_up=Decimal("0.40"),
            confidence=Decimal("0.8"),
            market_duration_seconds=900,
        )

        assert signal.edge_up == Decimal("-0.10")
        assert signal.edge_down == Decimal("0.10")
        assert signal.recommended_side == "DOWN"
        assert signal.has_edge

    def test_detect_edge_no_edge(self):
        """Test when there's no significant edge."""
        detector = EdgeDetector(min_edge=Decimal("0.05"))
        market = self._make_market(yes_ask=Decimal("0.50"), no_ask=Decimal("0.50"))

        # True probability is 52% up (only 2% edge, below threshold)
        signal = detector.detect_edge(
            market=market,
            estimated_prob_up=Decimal("0.52"),
            confidence=Decimal("0.8"),
            market_duration_seconds=900,
        )

        assert signal.recommended_side == "NONE"
        assert not signal.has_edge

    def test_detect_edge_low_confidence(self):
        """Test that low confidence prevents trading."""
        detector = EdgeDetector(
            min_edge=Decimal("0.05"),
            min_confidence=Decimal("0.5"),
        )
        market = self._make_market(yes_ask=Decimal("0.50"), no_ask=Decimal("0.50"))

        # High edge but low confidence
        signal = detector.detect_edge(
            market=market,
            estimated_prob_up=Decimal("0.70"),  # 20% edge
            confidence=Decimal("0.3"),  # Below threshold
            market_duration_seconds=900,
        )

        assert signal.recommended_side == "NONE"
        assert not signal.has_edge

    def test_detect_edge_fee_adjusted(self):
        """Test that fees are accounted for in EV calculation."""
        detector = EdgeDetector(
            min_edge=Decimal("0.05"),
            fee_rate=Decimal("0.10"),  # 10% fee
        )
        market = self._make_market(yes_ask=Decimal("0.50"), no_ask=Decimal("0.50"))

        # Edge exists but fees eat into it
        signal = detector.detect_edge(
            market=market,
            estimated_prob_up=Decimal("0.55"),  # 5% edge
            confidence=Decimal("0.8"),
            market_duration_seconds=900,
        )

        # EV should be reduced by fees
        assert signal.ev_up < signal.edge_up

    def test_kelly_sizing(self):
        """Test Kelly criterion position sizing."""
        detector = EdgeDetector(
            min_edge=Decimal("0.05"),
            kelly_fraction=Decimal("0.25"),
            max_position_pct=Decimal("0.10"),
        )
        market = self._make_market(yes_ask=Decimal("0.50"), no_ask=Decimal("0.50"))

        signal = detector.detect_edge(
            market=market,
            estimated_prob_up=Decimal("0.65"),  # 15% edge
            confidence=Decimal("1.0"),  # Full confidence
            market_duration_seconds=900,
        )

        # Should recommend a position but capped at max_position_pct
        assert signal.recommended_size > Decimal("0")
        assert signal.recommended_size <= Decimal("0.10")

    def test_expiry_adjustment(self):
        """Test that expiry time adjusts confidence."""
        detector = EdgeDetector()

        # Create market expiring soon
        market = self._make_market(
            yes_ask=Decimal("0.50"),
            no_ask=Decimal("0.50"),
            time_to_expiry_seconds=60,  # 1 minute left
        )

        signal = detector.detect_edge(
            market=market,
            estimated_prob_up=Decimal("0.65"),
            confidence=Decimal("1.0"),
            market_duration_seconds=900,
        )

        # Adjusted confidence should be lower due to near expiry
        assert signal.adjusted_confidence < signal.confidence
        assert signal.expiry_multiplier < Decimal("1.0")

    def test_filter_signals(self):
        """Test filtering signals by EV."""
        detector = EdgeDetector()

        signals = [
            EdgeSignal(
                market_id="1",
                market_name="BTC Up",
                asset="BTC",
                timeframe="15min",
                market_yes_price=Decimal("0.50"),
                market_no_price=Decimal("0.50"),
                estimated_prob_up=Decimal("0.60"),
                estimated_prob_down=Decimal("0.40"),
                edge_up=Decimal("0.10"),
                edge_down=Decimal("-0.10"),
                confidence=Decimal("0.8"),
                adjusted_confidence=Decimal("0.8"),
                recommended_side="UP",
                recommended_size=Decimal("0.05"),
                ev_up=Decimal("0.08"),
                ev_down=Decimal("-0.12"),
                time_to_expiry_seconds=600,
                expiry_multiplier=Decimal("1.0"),
            ),
            EdgeSignal(
                market_id="2",
                market_name="ETH Down",
                asset="ETH",
                timeframe="15min",
                market_yes_price=Decimal("0.40"),
                market_no_price=Decimal("0.60"),
                estimated_prob_up=Decimal("0.35"),
                estimated_prob_down=Decimal("0.65"),
                edge_up=Decimal("-0.05"),
                edge_down=Decimal("0.05"),
                confidence=Decimal("0.7"),
                adjusted_confidence=Decimal("0.7"),
                recommended_side="DOWN",
                recommended_size=Decimal("0.03"),
                ev_up=Decimal("-0.07"),
                ev_down=Decimal("0.03"),
                time_to_expiry_seconds=600,
                expiry_multiplier=Decimal("1.0"),
            ),
            EdgeSignal(
                market_id="3",
                market_name="SOL Flat",
                asset="SOL",
                timeframe="15min",
                market_yes_price=Decimal("0.50"),
                market_no_price=Decimal("0.50"),
                estimated_prob_up=Decimal("0.51"),
                estimated_prob_down=Decimal("0.49"),
                edge_up=Decimal("0.01"),
                edge_down=Decimal("-0.01"),
                confidence=Decimal("0.5"),
                adjusted_confidence=Decimal("0.5"),
                recommended_side="NONE",
                recommended_size=Decimal("0"),
                ev_up=Decimal("-0.01"),
                ev_down=Decimal("-0.01"),
                time_to_expiry_seconds=600,
                expiry_multiplier=Decimal("1.0"),
            ),
        ]

        filtered = detector.filter_signals(signals, min_ev=Decimal("0.01"))

        # Should only include signals with edge and positive EV
        assert len(filtered) == 2
        # Should be sorted by EV
        assert filtered[0].market_id == "1"  # Higher EV first
        assert filtered[1].market_id == "2"

    @staticmethod
    def _make_market(
        yes_ask: Decimal,
        no_ask: Decimal,
        time_to_expiry_seconds: int = 600,
    ) -> UpDownMarket:
        """Create a test UpDownMarket."""
        end_time = datetime.now(UTC) + timedelta(seconds=time_to_expiry_seconds)
        return UpDownMarket(
            id="test-market-1",
            name="BTC Up or Down?",
            asset=Asset.BTC,
            timeframe=Timeframe.FIFTEEN_MIN,
            end_time=end_time,
            yes_token_id="yes-token",
            no_token_id="no-token",
            condition_id="condition-1",
            yes_ask=yes_ask,
            yes_bid=yes_ask - Decimal("0.01"),
            no_ask=no_ask,
            no_bid=no_ask - Decimal("0.01"),
        )


class TestEdgeSignal:
    """Tests for EdgeSignal dataclass."""

    def test_has_edge_property(self):
        """Test has_edge property."""
        signal = EdgeSignal(
            market_id="1",
            market_name="Test",
            asset="BTC",
            timeframe="15min",
            market_yes_price=Decimal("0.50"),
            market_no_price=Decimal("0.50"),
            estimated_prob_up=Decimal("0.60"),
            estimated_prob_down=Decimal("0.40"),
            edge_up=Decimal("0.10"),
            edge_down=Decimal("-0.10"),
            confidence=Decimal("0.8"),
            adjusted_confidence=Decimal("0.8"),
            recommended_side="UP",
            recommended_size=Decimal("0.05"),
            ev_up=Decimal("0.08"),
            ev_down=Decimal("-0.12"),
            time_to_expiry_seconds=600,
            expiry_multiplier=Decimal("1.0"),
        )

        assert signal.has_edge
        assert signal.best_edge == Decimal("0.10")
        assert signal.best_ev == Decimal("0.08")

    def test_to_dict(self):
        """Test serialization to dictionary."""
        signal = EdgeSignal(
            market_id="1",
            market_name="Test",
            asset="BTC",
            timeframe="15min",
            market_yes_price=Decimal("0.50"),
            market_no_price=Decimal("0.50"),
            estimated_prob_up=Decimal("0.60"),
            estimated_prob_down=Decimal("0.40"),
            edge_up=Decimal("0.10"),
            edge_down=Decimal("-0.10"),
            confidence=Decimal("0.8"),
            adjusted_confidence=Decimal("0.8"),
            recommended_side="UP",
            recommended_size=Decimal("0.05"),
            ev_up=Decimal("0.08"),
            ev_down=Decimal("-0.12"),
            time_to_expiry_seconds=600,
            expiry_multiplier=Decimal("1.0"),
        )

        d = signal.to_dict()

        assert d["market_id"] == "1"
        assert d["recommended_side"] == "UP"
        assert d["edge_up"] == "0.10"
