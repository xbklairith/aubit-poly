"""Tests for probability utility functions."""

from decimal import Decimal

from pylo.utils.probability import (
    break_even_probability,
    calculate_edge,
    edge_expected_value,
    kelly_for_binary_market,
)


class TestKellyForBinaryMarket:
    """Tests for kelly_for_binary_market function."""

    def test_positive_edge(self):
        """Test Kelly with positive edge."""
        # 60% true prob, 50% market price = 10% edge
        kelly = kelly_for_binary_market(
            true_probability=Decimal("0.60"),
            market_price=Decimal("0.50"),
        )

        # Kelly = (0.60 - 0.50) / (1 - 0.50) = 0.20
        assert kelly == Decimal("0.20")

    def test_positive_edge_with_fees(self):
        """Test Kelly with positive edge and fees."""
        kelly = kelly_for_binary_market(
            true_probability=Decimal("0.60"),
            market_price=Decimal("0.50"),
            fee_rate=Decimal("0.02"),
        )

        # Effective price = 0.50 / 0.98 ≈ 0.5102
        # Kelly = (0.60 - 0.5102) / (1 - 0.5102) ≈ 0.183
        assert kelly > Decimal("0")
        assert kelly < Decimal("0.20")  # Less than no-fee Kelly

    def test_no_edge(self):
        """Test Kelly with no edge returns 0."""
        kelly = kelly_for_binary_market(
            true_probability=Decimal("0.50"),
            market_price=Decimal("0.50"),
        )

        assert kelly == Decimal("0")

    def test_negative_edge(self):
        """Test Kelly with negative edge returns 0."""
        kelly = kelly_for_binary_market(
            true_probability=Decimal("0.40"),
            market_price=Decimal("0.50"),
        )

        assert kelly == Decimal("0")

    def test_edge_eaten_by_fees(self):
        """Test when edge is eaten by fees."""
        # Small edge that fees eat up
        kelly = kelly_for_binary_market(
            true_probability=Decimal("0.51"),
            market_price=Decimal("0.50"),
            fee_rate=Decimal("0.05"),  # 5% fees
        )

        # Break-even = 0.50 / 0.95 ≈ 0.526 > 0.51
        assert kelly == Decimal("0")

    def test_invalid_market_price(self):
        """Test with invalid market price."""
        # Price = 0
        kelly = kelly_for_binary_market(
            true_probability=Decimal("0.60"),
            market_price=Decimal("0"),
        )
        assert kelly == Decimal("0")

        # Price = 1
        kelly = kelly_for_binary_market(
            true_probability=Decimal("0.60"),
            market_price=Decimal("1"),
        )
        assert kelly == Decimal("0")


class TestEdgeExpectedValue:
    """Tests for edge_expected_value function."""

    def test_positive_ev(self):
        """Test positive expected value."""
        ev = edge_expected_value(
            true_probability=Decimal("0.60"),
            market_price=Decimal("0.50"),
        )

        # EV = 0.60 × 0.50 - 0.40 × 0.50 = 0.30 - 0.20 = 0.10
        assert ev == Decimal("0.10")

    def test_positive_ev_with_fees(self):
        """Test positive EV with fees."""
        ev = edge_expected_value(
            true_probability=Decimal("0.60"),
            market_price=Decimal("0.50"),
            fee_rate=Decimal("0.02"),
        )

        # Net win = 0.50 - (0.50 × 0.02) = 0.50 - 0.01 = 0.49
        # EV = 0.60 × 0.49 - 0.40 × 0.50 = 0.294 - 0.20 = 0.094
        assert ev < Decimal("0.10")
        assert ev > Decimal("0.09")

    def test_negative_ev(self):
        """Test negative expected value."""
        ev = edge_expected_value(
            true_probability=Decimal("0.40"),
            market_price=Decimal("0.50"),
        )

        # EV = 0.40 × 0.50 - 0.60 × 0.50 = 0.20 - 0.30 = -0.10
        assert ev == Decimal("-0.10")

    def test_ev_with_bet_amount(self):
        """Test EV scales with bet amount."""
        ev = edge_expected_value(
            true_probability=Decimal("0.60"),
            market_price=Decimal("0.50"),
            bet_amount=Decimal("100"),
        )

        # EV = 0.10 × 100 = 10
        assert ev == Decimal("10")

    def test_breakeven_ev(self):
        """Test EV at break-even."""
        ev = edge_expected_value(
            true_probability=Decimal("0.50"),
            market_price=Decimal("0.50"),
        )

        assert ev == Decimal("0")


class TestBreakEvenProbability:
    """Tests for break_even_probability function."""

    def test_no_fees(self):
        """Test break-even without fees."""
        be = break_even_probability(Decimal("0.50"))
        assert be == Decimal("0.50")

    def test_with_fees(self):
        """Test break-even with fees."""
        be = break_even_probability(
            market_price=Decimal("0.50"),
            fee_rate=Decimal("0.02"),
        )

        # BE = 0.50 / 0.98 ≈ 0.5102
        assert be > Decimal("0.50")
        assert be < Decimal("0.52")

    def test_high_fees(self):
        """Test break-even with high fees."""
        be = break_even_probability(
            market_price=Decimal("0.50"),
            fee_rate=Decimal("0.10"),  # 10% fees
        )

        # BE = 0.50 / 0.90 ≈ 0.556
        assert be > Decimal("0.55")

    def test_100_percent_fees(self):
        """Test with 100% fees returns 1."""
        be = break_even_probability(
            market_price=Decimal("0.50"),
            fee_rate=Decimal("1.0"),
        )

        assert be == Decimal("1")


class TestCalculateEdge:
    """Tests for calculate_edge function."""

    def test_positive_edge(self):
        """Test positive edge calculation."""
        edge = calculate_edge(
            true_probability=Decimal("0.60"),
            market_price=Decimal("0.50"),
        )

        assert edge == Decimal("0.10")

    def test_negative_edge(self):
        """Test negative edge calculation."""
        edge = calculate_edge(
            true_probability=Decimal("0.40"),
            market_price=Decimal("0.50"),
        )

        assert edge == Decimal("-0.10")

    def test_zero_edge(self):
        """Test zero edge."""
        edge = calculate_edge(
            true_probability=Decimal("0.50"),
            market_price=Decimal("0.50"),
        )

        assert edge == Decimal("0")

    def test_large_edge(self):
        """Test large edge."""
        edge = calculate_edge(
            true_probability=Decimal("0.90"),
            market_price=Decimal("0.50"),
        )

        assert edge == Decimal("0.40")
