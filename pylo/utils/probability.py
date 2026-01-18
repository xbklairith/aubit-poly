"""Probability calculation utilities."""

from decimal import Decimal


def implied_probability_from_price(price: Decimal, max_payout: Decimal = Decimal("1")) -> Decimal:
    """
    Calculate implied probability from a binary option/prediction market price.

    For binary markets: probability = price / max_payout

    Args:
        price: Current market price
        max_payout: Maximum payout (typically $1.00)

    Returns:
        Implied probability as decimal (0-1)
    """
    if max_payout == 0:
        return Decimal("0")
    return price / max_payout


def price_from_probability(probability: Decimal, max_payout: Decimal = Decimal("1")) -> Decimal:
    """
    Calculate fair price from a probability.

    Args:
        probability: Probability as decimal (0-1)
        max_payout: Maximum payout

    Returns:
        Fair price
    """
    return probability * max_payout


def calculate_kelly_fraction(
    win_probability: Decimal,
    win_payout: Decimal,
    loss_amount: Decimal = Decimal("1"),
) -> Decimal:
    """
    Calculate Kelly Criterion bet sizing.

    Kelly fraction = (bp - q) / b

    Where:
    - b = odds received on the bet (win_payout / loss_amount)
    - p = probability of winning
    - q = probability of losing (1 - p)

    Args:
        win_probability: Probability of winning
        win_payout: Amount won if successful (relative to bet)
        loss_amount: Amount lost if unsuccessful (relative to bet)

    Returns:
        Optimal fraction of bankroll to bet (can be negative if edge is negative)
    """
    if loss_amount == 0:
        return Decimal("0")

    b = win_payout / loss_amount
    p = win_probability
    q = Decimal("1") - p

    if b == 0:
        return Decimal("0")

    kelly = (b * p - q) / b
    return kelly


def calculate_ev(
    win_probability: Decimal,
    win_amount: Decimal,
    loss_amount: Decimal,
) -> Decimal:
    """
    Calculate expected value of a bet.

    EV = (P(win) * win_amount) - (P(loss) * loss_amount)

    Args:
        win_probability: Probability of winning
        win_amount: Amount won if successful
        loss_amount: Amount lost if unsuccessful

    Returns:
        Expected value per bet
    """
    loss_probability = Decimal("1") - win_probability
    return (win_probability * win_amount) - (loss_probability * loss_amount)


def arbitrage_profit(
    yes_price: Decimal,
    no_price: Decimal,
    total_investment: Decimal = Decimal("1"),
    fee_rate: Decimal = Decimal("0"),
) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    """
    Calculate arbitrage profit for a binary market with proper proportional allocation.

    For risk-free arbitrage, allocation must be proportional to prices to guarantee
    the same payout regardless of outcome.

    Args:
        yes_price: Price of YES outcome (should be ask price for buying)
        no_price: Price of NO outcome (should be ask price for buying)
        total_investment: Total amount to invest
        fee_rate: Trading fee as decimal (e.g., 0.01 = 1%)

    Returns:
        Tuple of (profit_after_fees, yes_allocation, no_allocation, gross_profit)
    """
    total_cost = yes_price + no_price

    if total_cost >= Decimal("1"):
        return Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0")

    # Gross profit before fees
    profit_per_dollar = Decimal("1") - total_cost
    gross_profit = profit_per_dollar * total_investment

    # CORRECT: Proportional allocation for guaranteed equal payout
    # This ensures that regardless of outcome, the payout is the same
    # Example: YES=$0.40, NO=$0.55, total=$0.95
    # YES allocation = 0.40/0.95 = 42.1% -> buys 1.053 shares
    # NO allocation = 0.55/0.95 = 57.9% -> buys 1.053 shares
    # Either outcome pays 1.053 * $1 = $1.053 (guaranteed)
    yes_allocation = total_investment * (yes_price / total_cost)
    no_allocation = total_investment * (no_price / total_cost)

    # Calculate fees on both legs
    total_fees = (yes_allocation + no_allocation) * fee_rate
    profit_after_fees = gross_profit - total_fees

    return profit_after_fees, yes_allocation, no_allocation, gross_profit


def normalize_probabilities(probabilities: list[Decimal]) -> list[Decimal]:
    """
    Normalize a list of probabilities to sum to 1.

    Args:
        probabilities: List of probability estimates

    Returns:
        Normalized probabilities that sum to 1
    """
    total = sum(probabilities)
    if total == 0:
        return [Decimal("0")] * len(probabilities)
    return [p / total for p in probabilities]


def probability_from_odds(odds: Decimal) -> Decimal:
    """
    Convert decimal odds to implied probability.

    probability = 1 / odds

    Args:
        odds: Decimal odds (e.g., 2.0 for even money)

    Returns:
        Implied probability
    """
    if odds == 0:
        return Decimal("0")
    return Decimal("1") / odds


def odds_from_probability(probability: Decimal) -> Decimal:
    """
    Convert probability to decimal odds.

    odds = 1 / probability

    Args:
        probability: Probability as decimal (0-1)

    Returns:
        Decimal odds
    """
    if probability == 0:
        return Decimal("0")
    return Decimal("1") / probability


def kelly_for_binary_market(
    true_probability: Decimal,
    market_price: Decimal,
    fee_rate: Decimal = Decimal("0"),
) -> Decimal:
    """
    Calculate Kelly Criterion for a binary market (YES/NO at $1 payout).

    For buying at price P with true probability p:
    Kelly = (p - P_effective) / (1 - P_effective)

    Where P_effective accounts for fees on winnings.

    Args:
        true_probability: Estimated true probability of winning
        market_price: Current market price (0-1)
        fee_rate: Fee rate applied to winnings (e.g., 0.02 = 2%)

    Returns:
        Optimal fraction of bankroll to bet (0 if no edge)
    """
    if market_price >= Decimal("1") or market_price <= Decimal("0"):
        return Decimal("0")

    # Calculate break-even probability accounting for fees
    # To break even: p × (1 - fee) × $1 = price
    # So: p_breakeven = price / (1 - fee)
    effective_price = market_price / (Decimal("1") - fee_rate)

    # No edge if true probability is below break-even
    if true_probability <= effective_price:
        return Decimal("0")

    # Kelly = (p - P_eff) / (1 - P_eff)
    kelly = (true_probability - effective_price) / (Decimal("1") - effective_price)

    return max(Decimal("0"), kelly)


def edge_expected_value(
    true_probability: Decimal,
    market_price: Decimal,
    bet_amount: Decimal = Decimal("1"),
    fee_rate: Decimal = Decimal("0"),
) -> Decimal:
    """
    Calculate expected value of betting on a binary outcome.

    EV = P(win) × payout - P(lose) × cost
    Where payout = $1 - market_price (net profit on win)
    And cost = market_price (total loss on lose)

    Accounting for fees on winnings.

    Args:
        true_probability: Estimated true probability of winning
        market_price: Current market price to buy
        bet_amount: Amount to bet (in USD)
        fee_rate: Fee rate on winnings

    Returns:
        Expected value per bet (positive = profitable)
    """
    # Win: get $1 payout, paid market_price, minus fees on profit
    profit_on_win = Decimal("1") - market_price
    fee_on_win = profit_on_win * fee_rate
    net_win = profit_on_win - fee_on_win

    # Lose: lose the market_price paid
    loss_on_lose = market_price

    # EV = P(win) × net_win - P(lose) × loss
    ev = (true_probability * net_win) - ((Decimal("1") - true_probability) * loss_on_lose)

    return ev * bet_amount


def break_even_probability(
    market_price: Decimal,
    fee_rate: Decimal = Decimal("0"),
) -> Decimal:
    """
    Calculate break-even probability for a binary market bet.

    To break even: P(true) > market_price / (1 - fee_rate)

    Args:
        market_price: Current market price
        fee_rate: Fee rate on winnings

    Returns:
        Minimum probability needed to break even
    """
    if fee_rate >= Decimal("1"):
        return Decimal("1")  # Can never break even with 100%+ fees

    return market_price / (Decimal("1") - fee_rate)


def calculate_edge(
    true_probability: Decimal,
    market_price: Decimal,
) -> Decimal:
    """
    Calculate raw edge (probability gap).

    Edge = P(true) - P(market)

    Args:
        true_probability: Estimated true probability
        market_price: Market implied probability (price)

    Returns:
        Edge (positive = favorable, negative = unfavorable)
    """
    return true_probability - market_price
