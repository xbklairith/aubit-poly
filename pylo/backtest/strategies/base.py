"""Base strategy interface for backtesting."""

from abc import ABC, abstractmethod
from decimal import Decimal

from pylo.backtest.models import (
    BacktestTrade,
    MarketResolution,
    OrderType,
    PriceSnapshot,
    TradeSide,
)


class BaseStrategy(ABC):
    """Abstract base class for backtest strategies."""

    name: str = "base"
    description: str = "Base strategy"

    def __init__(
        self,
        skew_threshold: Decimal = Decimal("0.75"),
        position_size: Decimal = Decimal("50"),
        expiry_window_seconds: int = 180,  # 3 minutes
    ):
        """
        Initialize strategy.

        Args:
            skew_threshold: Minimum price to trigger signal (default 0.75)
            position_size: Position size in dollars (default $50)
            expiry_window_seconds: Time before expiry to trigger (default 180s = 3min)
        """
        self.skew_threshold = skew_threshold
        self.position_size = position_size
        self.expiry_window_seconds = expiry_window_seconds

    @property
    def params(self) -> dict:
        """Get strategy parameters as dict."""
        return {
            "skew_threshold": str(self.skew_threshold),
            "position_size": str(self.position_size),
            "expiry_window_seconds": self.expiry_window_seconds,
        }

    def should_signal(self, snapshot: PriceSnapshot) -> bool:
        """
        Check if this price snapshot should trigger a signal.

        Args:
            snapshot: Price data at a point in time

        Returns:
            True if signal should trigger
        """
        return snapshot.yes_price >= self.skew_threshold or snapshot.no_price >= self.skew_threshold

    @abstractmethod
    def get_trade_side(self, snapshot: PriceSnapshot) -> TradeSide:
        """
        Determine which side to trade.

        Args:
            snapshot: Price data at signal time

        Returns:
            Side to bet on (YES or NO)
        """
        pass

    @abstractmethod
    def get_order_type(self) -> OrderType:
        """Get the order type for this strategy."""
        pass

    @abstractmethod
    def get_order_price(self, snapshot: PriceSnapshot, trade_side: TradeSide) -> Decimal | None:
        """
        Get the order price (for limit orders).

        Args:
            snapshot: Price data at signal time
            trade_side: The side we're betting on

        Returns:
            Limit price or None for market orders
        """
        pass

    def simulate_fill(
        self,
        trade_side: TradeSide,
        order_type: OrderType,
        order_price: Decimal | None,
        winning_side: TradeSide,
        snapshot: PriceSnapshot,
    ) -> tuple[bool, Decimal | None]:
        """
        Simulate whether an order would fill and at what price.

        Args:
            trade_side: Side we bet on
            order_type: Market or limit order
            order_price: Limit price (if applicable)
            winning_side: Which side won the market
            snapshot: Price at signal time

        Returns:
            Tuple of (filled, fill_price)
        """
        if order_type == OrderType.MARKET:
            # Market orders always fill at current price
            fill_price = snapshot.yes_price if trade_side == TradeSide.YES else snapshot.no_price
            return True, fill_price

        elif order_type == OrderType.LIMIT:
            # REALISTIC fill logic for limit orders at low prices (e.g. $0.01):
            # - If we bet on LOSING side: price drops $0.20 → $0.00, passes through $0.01 → FILLS
            # - If we bet on WINNING side: price rises $0.20 → $1.00, never hits $0.01 → NO FILL
            #
            # So limit orders at low prices ONLY fill when betting on the losing side!
            if trade_side != winning_side and order_price is not None:
                return True, order_price
            return False, None

        return False, None

    def generate_trade(
        self,
        resolution: MarketResolution,
        snapshot: PriceSnapshot,
        time_to_expiry_seconds: int,
    ) -> BacktestTrade | None:
        """
        Generate a trade from a price snapshot.

        Args:
            resolution: Resolved market outcome
            snapshot: Price data at signal time
            time_to_expiry_seconds: Seconds until market expires

        Returns:
            BacktestTrade or None if no signal
        """
        # Check if signal triggers
        if not self.should_signal(snapshot):
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

        # Create trade
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
