"""Backtest data models."""

from datetime import datetime
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, Field


class TradeSide(str, Enum):
    """Side of a trade."""

    YES = "yes"
    NO = "no"


class OrderType(str, Enum):
    """Order type."""

    MARKET = "market"
    LIMIT = "limit"


class PriceSnapshot(BaseModel):
    """Historical price snapshot for a market."""

    condition_id: str
    yes_token_id: str
    no_token_id: str
    yes_price: Decimal
    no_price: Decimal
    timestamp: datetime

    @property
    def skewed_side(self) -> TradeSide | None:
        """Get the side with price >= 0.75 (the likely winner)."""
        if self.yes_price >= Decimal("0.75"):
            return TradeSide.YES
        if self.no_price >= Decimal("0.75"):
            return TradeSide.NO
        return None

    @property
    def skew_magnitude(self) -> Decimal:
        """Get the price of the skewed side."""
        return max(self.yes_price, self.no_price)


class MarketResolution(BaseModel):
    """Resolved market with outcome."""

    condition_id: str
    market_type: str
    asset: str
    timeframe: str
    name: str
    yes_token_id: str
    no_token_id: str
    winning_side: TradeSide
    end_time: datetime
    resolved_at: datetime | None = None
    final_yes_price: Decimal | None = None
    final_no_price: Decimal | None = None
    raw_data: dict | None = None


class BacktestTrade(BaseModel):
    """Simulated trade from a backtest."""

    condition_id: str
    market_name: str | None = None
    signal_time: datetime
    time_to_expiry_seconds: int

    # Prices at signal
    yes_price_at_signal: Decimal
    no_price_at_signal: Decimal
    skewed_side: TradeSide
    skew_magnitude: Decimal

    # Trade execution
    trade_side: TradeSide
    order_type: OrderType
    order_price: Decimal | None = None
    filled: bool = False
    fill_price: Decimal | None = None
    shares: Decimal = Decimal("50")  # Default $50 position
    cost: Decimal = Decimal("0")

    # Resolution
    winning_side: TradeSide
    won: bool = False
    payout: Decimal = Decimal("0")
    pnl: Decimal = Decimal("0")

    def calculate_pnl(self) -> None:
        """Calculate P&L based on fill and resolution."""
        if not self.filled:
            self.pnl = Decimal("0")
            self.payout = Decimal("0")
            return

        # Cost is fill_price * shares
        self.cost = (self.fill_price or Decimal("0")) * self.shares

        # Check if we won
        self.won = self.trade_side == self.winning_side

        if self.won:
            # Each share pays $1 on win
            self.payout = self.shares
            self.pnl = self.payout - self.cost
        else:
            self.payout = Decimal("0")
            self.pnl = -self.cost


class BacktestMetrics(BaseModel):
    """Aggregate metrics from a backtest run."""

    total_signals: int = 0
    orders_placed: int = 0
    orders_filled: int = 0
    winning_trades: int = 0
    losing_trades: int = 0

    total_invested: Decimal = Decimal("0")
    total_payout: Decimal = Decimal("0")
    net_pnl: Decimal = Decimal("0")
    gross_profit: Decimal = Decimal("0")
    gross_loss: Decimal = Decimal("0")

    win_rate: Decimal | None = None
    fill_rate: Decimal | None = None
    roi: Decimal | None = None
    profit_factor: Decimal | None = None
    max_drawdown: Decimal = Decimal("0")

    def calculate(self, trades: list[BacktestTrade]) -> None:
        """Calculate metrics from a list of trades."""
        self.total_signals = len(trades)
        self.orders_placed = len(trades)
        self.orders_filled = sum(1 for t in trades if t.filled)
        self.winning_trades = sum(1 for t in trades if t.filled and t.won)
        self.losing_trades = sum(1 for t in trades if t.filled and not t.won)

        for trade in trades:
            if trade.filled:
                self.total_invested += trade.cost
                self.total_payout += trade.payout
                if trade.pnl > 0:
                    self.gross_profit += trade.pnl
                else:
                    self.gross_loss += abs(trade.pnl)

        self.net_pnl = self.total_payout - self.total_invested

        # Calculate rates
        if self.orders_filled > 0:
            self.win_rate = Decimal(self.winning_trades) / Decimal(self.orders_filled)

        if self.total_signals > 0:
            self.fill_rate = Decimal(self.orders_filled) / Decimal(self.total_signals)

        if self.total_invested > 0:
            self.roi = self.net_pnl / self.total_invested

        if self.gross_loss > 0:
            self.profit_factor = self.gross_profit / self.gross_loss

        # Calculate max drawdown
        self._calculate_drawdown(trades)

    def _calculate_drawdown(self, trades: list[BacktestTrade]) -> None:
        """Calculate maximum drawdown from equity curve."""
        equity = Decimal("0")
        peak = Decimal("0")
        max_dd = Decimal("0")

        for trade in trades:
            if trade.filled:
                equity += trade.pnl
                # Only track peak when equity is positive
                if equity > peak:
                    peak = equity
                # Only measure drawdown from positive peaks
                if peak > Decimal("0"):
                    drawdown = peak - equity
                    if drawdown > max_dd:
                        max_dd = drawdown

        self.max_drawdown = max_dd


class BacktestRun(BaseModel):
    """Complete backtest run with results."""

    strategy_name: str
    strategy_params: dict = Field(default_factory=dict)
    start_date: datetime
    end_date: datetime
    assets: list[str] = Field(default_factory=list)
    timeframes: list[str] = Field(default_factory=list)
    skew_threshold: Decimal = Decimal("0.75")

    # Results
    trades: list[BacktestTrade] = Field(default_factory=list)
    metrics: BacktestMetrics = Field(default_factory=BacktestMetrics)

    # Execution metadata
    executed_at: datetime = Field(default_factory=datetime.utcnow)
    duration_seconds: float = 0.0
