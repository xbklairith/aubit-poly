"""Probability Gap strategy - trade when momentum-based probability differs from market price."""

from decimal import Decimal

from pylo.backtest.models import (
    BacktestTrade,
    MarketResolution,
    OrderType,
    PriceSnapshot,
    TradeSide,
)
from pylo.backtest.strategies.base import BaseStrategy


class ProbabilityGapStrategy(BaseStrategy):
    """
    Strategy that trades when estimated probability differs from market price.

    Uses market price trend as a momentum signal:
    - If YES price is rising → momentum suggests YES more likely
    - If YES price is falling → momentum suggests NO more likely

    Compares momentum-based probability to market price to find edge.
    """

    name = "probability_gap"
    description = "Trade based on probability gap between momentum estimate and market price"

    def __init__(
        self,
        skew_threshold: Decimal = Decimal("0.50"),  # Lower threshold - we care about edge, not skew
        position_size: Decimal = Decimal("50"),
        expiry_window_seconds: int = 600,  # 10 minutes - need time for momentum
        min_edge: Decimal = Decimal("0.05"),  # 5% minimum edge to trade
        kelly_fraction: Decimal = Decimal("0.25"),  # 25% Kelly for position sizing
    ):
        """
        Initialize Probability Gap strategy.

        Args:
            skew_threshold: Not used for signal, kept for compatibility
            position_size: Base position size in dollars
            expiry_window_seconds: Window before expiry to consider
            min_edge: Minimum edge (P(true) - P(market)) to trigger trade
            kelly_fraction: Fraction of Kelly criterion for sizing
        """
        super().__init__(skew_threshold, position_size, expiry_window_seconds)
        self.min_edge = min_edge
        self.kelly_fraction = kelly_fraction

        # State for momentum calculation across snapshots
        self._price_history: list[Decimal] = []
        self._current_edge: Decimal = Decimal("0")
        self._estimated_prob_up: Decimal = Decimal("0.5")

    @property
    def params(self) -> dict:
        """Get strategy parameters as dict."""
        base_params = super().params
        base_params.update({
            "min_edge": str(self.min_edge),
            "kelly_fraction": str(self.kelly_fraction),
        })
        return base_params

    def reset_state(self) -> None:
        """Reset momentum state for new market."""
        self._price_history = []
        self._current_edge = Decimal("0")
        self._estimated_prob_up = Decimal("0.5")

    def update_momentum(self, snapshot: PriceSnapshot) -> None:
        """
        Update momentum calculation with new price snapshot.

        Args:
            snapshot: New price data point
        """
        self._price_history.append(snapshot.yes_price)

        if len(self._price_history) < 3:
            # Not enough data for momentum
            self._estimated_prob_up = Decimal("0.5")
            self._current_edge = Decimal("0")
            return

        # Calculate momentum from price trend
        # Simple approach: compare recent average to older average
        recent = list(self._price_history[-3:])
        older = list(self._price_history[:-3]) if len(self._price_history) > 3 else recent

        recent_avg = sum(recent) / len(recent)
        older_avg = sum(older) / len(older) if older else recent_avg

        # Momentum = direction of price change
        # If YES price rising, probability of UP increases
        momentum = recent_avg - older_avg

        # Convert momentum to probability adjustment
        # Scale momentum to ±20% probability adjustment max
        prob_adjustment = min(Decimal("0.20"), max(Decimal("-0.20"), momentum * 2))

        # Base probability is current market price
        # Adjust based on momentum
        market_prob = snapshot.yes_price
        self._estimated_prob_up = max(
            Decimal("0.05"),
            min(Decimal("0.95"), market_prob + prob_adjustment)
        )

        # Edge = estimated probability - market price
        self._current_edge = self._estimated_prob_up - market_prob

    def should_signal(self, snapshot: PriceSnapshot) -> bool:
        """
        Signal when edge exceeds threshold.

        Args:
            snapshot: Current price data

        Returns:
            True if edge is significant enough to trade
        """
        self.update_momentum(snapshot)

        # Signal when we have meaningful edge in either direction
        edge_up = self._estimated_prob_up - snapshot.yes_price
        edge_down = (Decimal("1") - self._estimated_prob_up) - snapshot.no_price

        return abs(edge_up) >= self.min_edge or abs(edge_down) >= self.min_edge

    def get_trade_side(self, snapshot: PriceSnapshot) -> TradeSide:
        """
        Trade the side with positive edge.

        Args:
            snapshot: Price data at signal time

        Returns:
            Side with better edge
        """
        edge_up = self._estimated_prob_up - snapshot.yes_price
        edge_down = (Decimal("1") - self._estimated_prob_up) - snapshot.no_price

        if edge_up >= self.min_edge:
            return TradeSide.YES
        elif edge_down >= self.min_edge:
            return TradeSide.NO
        else:
            # Default to the side with higher edge
            return TradeSide.YES if edge_up > edge_down else TradeSide.NO

    def get_order_type(self) -> OrderType:
        """Use market orders for guaranteed fills."""
        return OrderType.MARKET

    def get_order_price(self, snapshot: PriceSnapshot, trade_side: TradeSide) -> Decimal | None:
        """Market orders don't need a limit price."""
        return None

    def generate_trade(
        self,
        resolution: MarketResolution,
        snapshot: PriceSnapshot,
        time_to_expiry_seconds: int,
    ) -> BacktestTrade | None:
        """
        Generate a trade with probability gap analysis.

        Args:
            resolution: Resolved market outcome
            snapshot: Price data at signal time
            time_to_expiry_seconds: Seconds until market expires

        Returns:
            BacktestTrade or None if no signal
        """
        # Check if signal triggers (also updates momentum)
        if not self.should_signal(snapshot):
            return None

        # Skip if too close to expiry (market already knows)
        if time_to_expiry_seconds < 60:  # Less than 1 minute
            return None

        # Determine trade parameters
        trade_side = self.get_trade_side(snapshot)
        order_type = self.get_order_type()
        order_price = self.get_order_price(snapshot, trade_side)

        # Get skewed side info
        skewed_side = snapshot.skewed_side or TradeSide.YES
        skew_magnitude = snapshot.skew_magnitude

        # Simulate fill
        filled, fill_price = self.simulate_fill(
            trade_side=trade_side,
            order_type=order_type,
            order_price=order_price,
            winning_side=resolution.winning_side,
            snapshot=snapshot,
        )

        # Create trade with enhanced info
        trade = BacktestTrade(
            condition_id=resolution.condition_id,
            market_name=resolution.name,
            signal_time=snapshot.timestamp,
            time_to_expiry_seconds=time_to_expiry_seconds,
            yes_price_at_signal=snapshot.yes_price,
            no_price_at_signal=snapshot.no_price,
            skewed_side=skewed_side,
            skew_magnitude=skew_magnitude,
            trade_side=trade_side,
            order_type=order_type,
            order_price=order_price,
            filled=filled,
            fill_price=fill_price,
            shares=self.position_size,
            winning_side=resolution.winning_side,
        )

        # Calculate P&L
        trade.calculate_pnl()

        return trade


class MomentumContrarianStrategy(BaseStrategy):
    """
    Enhanced contrarian strategy using momentum for timing.

    Instead of blindly betting against the market, this strategy:
    1. Waits for momentum to show signs of reversal
    2. Only bets contrarian when momentum suggests the skew is weakening
    """

    name = "momentum_contrarian"
    description = "Contrarian trades with momentum-based timing"

    def __init__(
        self,
        skew_threshold: Decimal = Decimal("0.75"),
        position_size: Decimal = Decimal("50"),
        expiry_window_seconds: int = 300,  # 5 minutes
        reversal_threshold: Decimal = Decimal("0.02"),  # 2% reversal to trigger
    ):
        """
        Initialize Momentum Contrarian strategy.

        Args:
            skew_threshold: Minimum skew to consider market
            position_size: Position size in dollars
            expiry_window_seconds: Window before expiry
            reversal_threshold: Price drop from peak to trigger contrarian
        """
        super().__init__(skew_threshold, position_size, expiry_window_seconds)
        self.reversal_threshold = reversal_threshold

        # Track peak price for reversal detection
        self._peak_yes_price: Decimal = Decimal("0")
        self._peak_no_price: Decimal = Decimal("0")

    @property
    def params(self) -> dict:
        """Get strategy parameters as dict."""
        base_params = super().params
        base_params["reversal_threshold"] = str(self.reversal_threshold)
        return base_params

    def reset_state(self) -> None:
        """Reset state for new market."""
        self._peak_yes_price = Decimal("0")
        self._peak_no_price = Decimal("0")

    def update_peaks(self, snapshot: PriceSnapshot) -> None:
        """Update peak prices."""
        if snapshot.yes_price > self._peak_yes_price:
            self._peak_yes_price = snapshot.yes_price
        if snapshot.no_price > self._peak_no_price:
            self._peak_no_price = snapshot.no_price

    def should_signal(self, snapshot: PriceSnapshot) -> bool:
        """
        Signal when skewed market shows reversal.

        Args:
            snapshot: Current price data

        Returns:
            True if reversal detected
        """
        self.update_peaks(snapshot)

        # Need significant skew first
        if snapshot.yes_price < self.skew_threshold and snapshot.no_price < self.skew_threshold:
            return False

        # Check for reversal from peak
        if snapshot.yes_price >= self.skew_threshold:
            reversal = self._peak_yes_price - snapshot.yes_price
            return reversal >= self.reversal_threshold
        else:
            reversal = self._peak_no_price - snapshot.no_price
            return reversal >= self.reversal_threshold

    def get_trade_side(self, snapshot: PriceSnapshot) -> TradeSide:
        """
        Bet against the skewed side (contrarian).

        Args:
            snapshot: Price data at signal time

        Returns:
            Opposite of the favored side
        """
        if snapshot.yes_price >= self.skew_threshold:
            return TradeSide.NO  # Bet against YES
        return TradeSide.YES  # Bet against NO

    def get_order_type(self) -> OrderType:
        """Use market orders."""
        return OrderType.MARKET

    def get_order_price(self, snapshot: PriceSnapshot, trade_side: TradeSide) -> Decimal | None:
        """Market orders don't need price."""
        return None
