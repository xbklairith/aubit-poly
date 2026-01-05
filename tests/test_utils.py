"""Tests for utility functions."""

from decimal import Decimal

import pytest

from src.utils.probability import (
    arbitrage_profit,
    calculate_ev,
    calculate_kelly_fraction,
    implied_probability_from_price,
    normalize_probabilities,
    odds_from_probability,
    price_from_probability,
    probability_from_odds,
)


class TestProbabilityUtils:
    """Tests for probability calculation utilities."""

    def test_implied_probability_from_price(self) -> None:
        """Test implied probability calculation."""
        assert implied_probability_from_price(Decimal("0.50")) == Decimal("0.50")
        assert implied_probability_from_price(Decimal("0.75")) == Decimal("0.75")
        assert implied_probability_from_price(Decimal("0")) == Decimal("0")

    def test_price_from_probability(self) -> None:
        """Test price calculation from probability."""
        assert price_from_probability(Decimal("0.50")) == Decimal("0.50")
        assert price_from_probability(Decimal("0.75")) == Decimal("0.75")

    def test_kelly_fraction_positive_edge(self) -> None:
        """Test Kelly fraction with positive edge."""
        # 60% win rate, 1:1 payout
        kelly = calculate_kelly_fraction(
            win_probability=Decimal("0.60"),
            win_payout=Decimal("1"),
            loss_amount=Decimal("1"),
        )
        # Kelly = (bp - q) / b = (1*0.6 - 0.4) / 1 = 0.2
        assert kelly == Decimal("0.20")

    def test_kelly_fraction_negative_edge(self) -> None:
        """Test Kelly fraction with negative edge."""
        # 40% win rate, 1:1 payout
        kelly = calculate_kelly_fraction(
            win_probability=Decimal("0.40"),
            win_payout=Decimal("1"),
            loss_amount=Decimal("1"),
        )
        # Kelly should be negative (don't bet)
        assert kelly < 0

    def test_calculate_ev_positive(self) -> None:
        """Test positive expected value calculation."""
        ev = calculate_ev(
            win_probability=Decimal("0.60"),
            win_amount=Decimal("1"),
            loss_amount=Decimal("1"),
        )
        # EV = 0.6 * 1 - 0.4 * 1 = 0.2
        assert ev == Decimal("0.20")

    def test_calculate_ev_negative(self) -> None:
        """Test negative expected value calculation."""
        ev = calculate_ev(
            win_probability=Decimal("0.40"),
            win_amount=Decimal("1"),
            loss_amount=Decimal("1"),
        )
        # EV = 0.4 * 1 - 0.6 * 1 = -0.2
        assert ev == Decimal("-0.20")

    def test_arbitrage_profit_exists(self) -> None:
        """Test arbitrage profit calculation when opportunity exists."""
        profit, yes_alloc, no_alloc = arbitrage_profit(
            yes_price=Decimal("0.45"),
            no_price=Decimal("0.50"),
            total_investment=Decimal("100"),
        )
        # Total cost = 0.95, profit = 0.05 per dollar
        assert profit == Decimal("5")  # 5% of $100

    def test_arbitrage_profit_none(self) -> None:
        """Test arbitrage profit when no opportunity."""
        profit, yes_alloc, no_alloc = arbitrage_profit(
            yes_price=Decimal("0.50"),
            no_price=Decimal("0.52"),
        )
        assert profit == Decimal("0")

    def test_normalize_probabilities(self) -> None:
        """Test probability normalization."""
        probs = [Decimal("0.3"), Decimal("0.4"), Decimal("0.5")]
        normalized = normalize_probabilities(probs)

        # Should sum to 1
        assert sum(normalized) == Decimal("1")

        # Should maintain ratios
        assert normalized[1] > normalized[0]
        assert normalized[2] > normalized[1]

    def test_odds_conversion(self) -> None:
        """Test odds to probability conversion."""
        # 2.0 odds = 50% probability
        assert probability_from_odds(Decimal("2")) == Decimal("0.5")

        # Round trip
        prob = Decimal("0.25")
        odds = odds_from_probability(prob)
        assert probability_from_odds(odds) == prob
