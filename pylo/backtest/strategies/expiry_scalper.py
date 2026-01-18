"""Expiry Scalper strategy - bet WITH the skew at market price."""

from decimal import Decimal

from pylo.backtest.models import OrderType, PriceSnapshot, TradeSide
from pylo.backtest.strategies.base import BaseStrategy


class ExpiryScalperStrategy(BaseStrategy):
    """
    Expiry Scalper (Normal) Strategy.

    Logic:
        IF market expires within 3 minutes
           AND market is 15m up/down crypto
           AND (YES_price >= 0.75 OR NO_price >= 0.75)
        THEN buy the skewed side at market price

    This strategy bets that the heavily favored side will win.
    It pays a premium (0.75+) for high confidence outcomes.
    """

    name = "expiry_scalper"
    description = "Bet WITH skew at market price near expiry"

    def get_trade_side(self, snapshot: PriceSnapshot) -> TradeSide:
        """
        Trade the skewed side (the one with higher price).

        If YES >= 0.75, bet YES.
        If NO >= 0.75, bet NO.
        """
        if snapshot.yes_price >= self.skew_threshold:
            return TradeSide.YES
        return TradeSide.NO

    def get_order_type(self) -> OrderType:
        """Use market orders for immediate fill."""
        return OrderType.MARKET

    def get_order_price(self, snapshot: PriceSnapshot, trade_side: TradeSide) -> Decimal | None:  # noqa: ARG002
        """Market orders don't have a limit price."""
        return None
