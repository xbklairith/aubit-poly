"""Yahoo Finance client for stock and options data."""

import logging
from datetime import datetime
from decimal import Decimal

from pylo.models.market import Platform, StockData

logger = logging.getLogger(__name__)


class YahooFinanceClient:
    """Client for Yahoo Finance data using yfinance library."""

    name = "yahoo"

    def __init__(self) -> None:
        """Initialize the Yahoo Finance client."""
        self._connected = False
        self.logger = logging.getLogger(f"{__name__}.{self.name}")

    async def connect(self) -> None:
        """Mark as connected (yfinance doesn't need explicit connection)."""
        self._connected = True
        self.logger.info("Yahoo Finance client ready")

    async def disconnect(self) -> None:
        """Mark as disconnected."""
        self._connected = False
        self.logger.info("Yahoo Finance client disconnected")

    def get_stock_price(self, symbol: str) -> StockData | None:
        """
        Get current stock price.

        Note: This is synchronous due to yfinance limitations.
        Consider running in executor for async context.

        Args:
            symbol: Stock ticker (e.g., "AAPL")

        Returns:
            StockData object or None
        """
        try:
            import yfinance as yf

            ticker = yf.Ticker(symbol)
            info = ticker.info

            price = info.get("regularMarketPrice") or info.get("currentPrice")
            if price is None:
                self.logger.warning(f"No price data for {symbol}")
                return None

            return StockData(
                symbol=symbol,
                platform=Platform.YAHOO,
                price=Decimal(str(price)),
                timestamp=datetime.utcnow(),
            )

        except Exception as e:
            self.logger.error(f"Failed to fetch stock price for {symbol}: {e}")
            return None

    def get_options_chain(self, symbol: str, expiry: str | None = None) -> list[dict]:
        """
        Get options chain for a stock.

        Args:
            symbol: Stock ticker
            expiry: Optional expiry date string

        Returns:
            List of option contract data
        """
        try:
            import yfinance as yf

            ticker = yf.Ticker(symbol)

            # Get available expiry dates
            if expiry:
                options = ticker.option_chain(expiry)
            else:
                # Get nearest expiry
                expiries = ticker.options
                if not expiries:
                    return []
                options = ticker.option_chain(expiries[0])

            # Combine calls and puts
            calls = options.calls.to_dict("records") if hasattr(options, "calls") else []
            puts = options.puts.to_dict("records") if hasattr(options, "puts") else []

            # Mark call/put type
            for c in calls:
                c["optionType"] = "call"
            for p in puts:
                p["optionType"] = "put"

            return calls + puts

        except Exception as e:
            self.logger.error(f"Failed to fetch options for {symbol}: {e}")
            return []

    def get_option_implied_probability(
        self,
        symbol: str,
        target_price: Decimal,
        expiry: str,
    ) -> Decimal | None:
        """
        Estimate probability of stock reaching target price.

        Uses options delta as probability proxy.

        Args:
            symbol: Stock ticker
            target_price: Target stock price
            expiry: Expiry date string

        Returns:
            Estimated probability (0-1) or None
        """
        try:
            import yfinance as yf

            ticker = yf.Ticker(symbol)
            options = ticker.option_chain(expiry)

            if not hasattr(options, "calls"):
                return None

            calls = options.calls

            # Find call with strike closest to target
            calls["strike_diff"] = abs(calls["strike"] - float(target_price))
            closest = calls.loc[calls["strike_diff"].idxmin()]

            # Use delta as probability proxy (if available)
            # Otherwise, use (stock_price - strike) / stock_price as rough estimate
            if "delta" in closest and closest["delta"]:
                return Decimal(str(closest["delta"]))

            # Fallback: use price ratio
            current_price = self.get_stock_price(symbol)
            if current_price:
                # Simple heuristic based on moneyness
                moneyness = current_price.price / target_price
                # Rough probability estimate
                prob = max(Decimal("0"), min(Decimal("1"), moneyness))
                return prob

            return None

        except Exception as e:
            self.logger.error(f"Failed to calculate probability: {e}")
            return None

    async def __aenter__(self) -> "YahooFinanceClient":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        """Async context manager exit."""
        await self.disconnect()
