"""Backtest metrics calculations."""

from decimal import Decimal

from pylo.backtest.models import BacktestMetrics, BacktestTrade


def calculate_metrics(trades: list[BacktestTrade]) -> BacktestMetrics:
    """
    Calculate comprehensive metrics from a list of trades.

    Args:
        trades: List of backtest trades

    Returns:
        BacktestMetrics with all calculated values
    """
    metrics = BacktestMetrics()
    metrics.calculate(trades)
    return metrics


def calculate_equity_curve(trades: list[BacktestTrade]) -> list[tuple[int, Decimal]]:
    """
    Calculate equity curve from trades.

    Args:
        trades: List of trades (sorted by time)

    Returns:
        List of (trade_index, cumulative_pnl) tuples
    """
    curve = []
    cumulative = Decimal("0")

    for i, trade in enumerate(trades):
        if trade.filled:
            cumulative += trade.pnl
        curve.append((i, cumulative))

    return curve


def calculate_drawdown_curve(trades: list[BacktestTrade]) -> list[tuple[int, Decimal]]:
    """
    Calculate drawdown curve from trades.

    Args:
        trades: List of trades (sorted by time)

    Returns:
        List of (trade_index, drawdown) tuples
    """
    equity = Decimal("0")
    peak = Decimal("0")
    curve = []

    for i, trade in enumerate(trades):
        if trade.filled:
            equity += trade.pnl
        if equity > peak:
            peak = equity
        drawdown = peak - equity
        curve.append((i, drawdown))

    return curve


def calculate_by_asset(trades: list[BacktestTrade]) -> dict[str, BacktestMetrics]:
    """
    Calculate metrics broken down by asset.

    Args:
        trades: List of trades

    Returns:
        Dict mapping asset to metrics
    """
    # Group trades by asset (extracted from market name)
    by_asset: dict[str, list[BacktestTrade]] = {}

    for trade in trades:
        # Extract asset from market name
        name = trade.market_name or ""
        name_lower = name.lower()

        if "bitcoin" in name_lower or "btc" in name_lower:
            asset = "BTC"
        elif "ethereum" in name_lower or "eth" in name_lower:
            asset = "ETH"
        elif "solana" in name_lower or "sol" in name_lower:
            asset = "SOL"
        elif "xrp" in name_lower or "ripple" in name_lower:
            asset = "XRP"
        else:
            asset = "OTHER"

        if asset not in by_asset:
            by_asset[asset] = []
        by_asset[asset].append(trade)

    # Calculate metrics for each asset
    result = {}
    for asset, asset_trades in by_asset.items():
        metrics = BacktestMetrics()
        metrics.calculate(asset_trades)
        result[asset] = metrics

    return result


def calculate_by_skew_level(trades: list[BacktestTrade]) -> dict[str, BacktestMetrics]:
    """
    Calculate metrics broken down by skew level.

    Args:
        trades: List of trades

    Returns:
        Dict mapping skew range to metrics
    """
    ranges = [
        ("0.75-0.80", Decimal("0.75"), Decimal("0.80")),
        ("0.80-0.85", Decimal("0.80"), Decimal("0.85")),
        ("0.85-0.90", Decimal("0.85"), Decimal("0.90")),
        ("0.90-0.95", Decimal("0.90"), Decimal("0.95")),
        ("0.95-1.00", Decimal("0.95"), Decimal("1.00")),
    ]

    by_range: dict[str, list[BacktestTrade]] = {r[0]: [] for r in ranges}

    for trade in trades:
        for range_name, low, high in ranges:
            if low <= trade.skew_magnitude < high:
                by_range[range_name].append(trade)
                break

    result = {}
    for range_name, range_trades in by_range.items():
        if range_trades:
            metrics = BacktestMetrics()
            metrics.calculate(range_trades)
            result[range_name] = metrics

    return result


def calculate_by_time_to_expiry(trades: list[BacktestTrade]) -> dict[str, BacktestMetrics]:
    """
    Calculate metrics broken down by time to expiry.

    Args:
        trades: List of trades

    Returns:
        Dict mapping time range to metrics
    """
    ranges = [
        ("0-60s", 0, 60),
        ("60-120s", 60, 120),
        ("120-180s", 120, 180),
        ("180-300s", 180, 300),
    ]

    by_range: dict[str, list[BacktestTrade]] = {r[0]: [] for r in ranges}

    for trade in trades:
        for range_name, low, high in ranges:
            if low <= trade.time_to_expiry_seconds < high:
                by_range[range_name].append(trade)
                break

    result = {}
    for range_name, range_trades in by_range.items():
        if range_trades:
            metrics = BacktestMetrics()
            metrics.calculate(range_trades)
            result[range_name] = metrics

    return result


def calculate_skew_accuracy(trades: list[BacktestTrade]) -> dict:
    """
    Calculate how often the skewed side actually won.

    This helps evaluate if the market is correctly predicting outcomes.

    Args:
        trades: List of trades

    Returns:
        Dict with accuracy statistics
    """
    total = len(trades)
    skew_won = sum(1 for t in trades if t.skewed_side == t.winning_side)
    skew_lost = total - skew_won

    return {
        "total_markets": total,
        "skew_won": skew_won,
        "skew_lost": skew_lost,
        "skew_accuracy": Decimal(skew_won) / Decimal(total) if total > 0 else None,
        "contrarian_opportunity": Decimal(skew_lost) / Decimal(total) if total > 0 else None,
    }


def compare_strategies(runs: dict[str, BacktestMetrics]) -> dict:
    """
    Compare metrics across multiple strategy runs.

    Args:
        runs: Dict mapping strategy name to metrics

    Returns:
        Comparison summary
    """
    comparison = {}

    for name, metrics in runs.items():
        comparison[name] = {
            "win_rate": float(metrics.win_rate) if metrics.win_rate else None,
            "fill_rate": float(metrics.fill_rate) if metrics.fill_rate else None,
            "roi": float(metrics.roi) if metrics.roi else None,
            "profit_factor": float(metrics.profit_factor) if metrics.profit_factor else None,
            "total_pnl": float(metrics.net_pnl),
            "max_drawdown": float(metrics.max_drawdown),
            "trades": metrics.orders_filled,
        }

    return comparison
