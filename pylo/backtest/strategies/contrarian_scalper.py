"""Contrarian Scalper strategy - bet AGAINST the skew."""

from decimal import Decimal

from pylo.backtest.models import OrderType, PriceSnapshot, TradeSide
from pylo.backtest.strategies.base import BaseStrategy


class ContrarianScalperStrategy(BaseStrategy):
    """
    Contrarian Scalper Strategy.

    Logic:
        IF market expires within X seconds
           AND market is 15m up/down crypto
           AND (YES_price >= threshold OR NO_price >= threshold)
        THEN buy OPPOSITE side

    Order types:
        - LIMIT: Buy at low price ($0.01-$0.10) - only fills when wrong (guaranteed loss)
        - MARKET: Buy at current price (~$0.20-$0.25) - always fills

    With MARKET orders, profitability depends on whether betting against skew is correct.
    Historical data shows skew is correct ~75-85% of time, so contrarian is expected
    to lose even with market orders, but less catastrophically than limit orders.
    """

    name = "contrarian_scalper"
    description = "Bet AGAINST skew near expiry"

    def __init__(
        self,
        skew_threshold: Decimal = Decimal("0.75"),
        position_size: Decimal = Decimal("50"),
        expiry_window_seconds: int = 180,
        limit_price: Decimal | None = Decimal("0.01"),
        use_market_order: bool = False,
    ):
        """
        Initialize contrarian strategy.

        Args:
            skew_threshold: Minimum price to trigger signal
            position_size: Position size in shares
            expiry_window_seconds: Time before expiry to trigger
            limit_price: Limit order price (default $0.01) - ignored if use_market_order=True
            use_market_order: If True, use market orders instead of limit
        """
        super().__init__(
            skew_threshold=skew_threshold,
            position_size=position_size,
            expiry_window_seconds=expiry_window_seconds,
        )
        self.limit_price = limit_price
        self.use_market_order = use_market_order

    @property
    def params(self) -> dict:
        """Get strategy parameters as dict."""
        params = super().params
        params["order_type"] = "MARKET" if self.use_market_order else "LIMIT"
        if not self.use_market_order:
            params["limit_price"] = str(self.limit_price)
        return params

    def get_trade_side(self, snapshot: PriceSnapshot) -> TradeSide:
        """
        Trade OPPOSITE the skewed side.

        If YES >= threshold (market thinks YES will win), bet NO.
        If NO >= threshold (market thinks NO will win), bet YES.
        """
        if snapshot.yes_price >= self.skew_threshold:
            return TradeSide.NO  # Bet against YES
        return TradeSide.YES  # Bet against NO

    def get_order_type(self) -> OrderType:
        """Return order type based on configuration."""
        return OrderType.MARKET if self.use_market_order else OrderType.LIMIT

    def get_order_price(self, snapshot: PriceSnapshot, trade_side: TradeSide) -> Decimal | None:
        """Return order price - market price or limit price."""
        if self.use_market_order:
            # Market order: buy at current price of the side we're betting on
            return snapshot.no_price if trade_side == TradeSide.NO else snapshot.yes_price
        return self.limit_price
