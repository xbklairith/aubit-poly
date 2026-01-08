"""Binance exchange client for spot, futures, and options data."""

import logging
from datetime import datetime
from decimal import Decimal

import httpx
from asyncio_throttle import Throttler

from pylo.config.settings import get_settings
from pylo.models.market import CryptoPrice, Platform

logger = logging.getLogger(__name__)

# Binance API endpoints
SPOT_API = "https://api.binance.com/api/v3"
FUTURES_API = "https://fapi.binance.com/fapi/v1"
OPTIONS_API = "https://eapi.binance.com/eapi/v1"

# Rate limits (requests per second) - Binance allows 1200/min = 20/sec
BINANCE_RATE_LIMIT = 15  # Conservative limit


class BinanceClient:
    """Client for Binance spot, futures, and options APIs."""

    name = "binance"

    def __init__(self) -> None:
        """Initialize the Binance client."""
        self.settings = get_settings()
        self._client: httpx.AsyncClient | None = None
        self._connected = False
        self.logger = logging.getLogger(f"{__name__}.{self.name}")
        self._throttler = Throttler(rate_limit=BINANCE_RATE_LIMIT, period=1.0)

    async def connect(self) -> None:
        """Initialize HTTP client."""
        headers = {"Accept": "application/json"}

        # Add API key if available (for authenticated endpoints)
        if self.settings.has_binance_credentials:
            headers["X-MBX-APIKEY"] = self.settings.binance_api_key.get_secret_value()

        self._client = httpx.AsyncClient(
            timeout=30.0,
            headers=headers,
        )
        self._connected = True
        self.logger.info("Connected to Binance API")

    async def disconnect(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
        self._connected = False
        self.logger.info("Disconnected from Binance API")

    async def get_spot_price(self, symbol: str = "BTCUSDT") -> CryptoPrice | None:
        """
        Get current spot price.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT")

        Returns:
            CryptoPrice object or None
        """
        if not self._client:
            raise RuntimeError("Client not connected")

        try:
            async with self._throttler:
                response = await self._client.get(
                    f"{SPOT_API}/ticker/price",
                    params={"symbol": symbol},
                )
                response.raise_for_status()
                data = response.json()

            return CryptoPrice(
                symbol=symbol,
                platform=Platform.BINANCE,
                price=Decimal(data["price"]),
                timestamp=datetime.utcnow(),
            )

        except httpx.HTTPError as e:
            self.logger.error(f"Failed to fetch spot price: {e}")
            return None

    async def get_futures_price(self, symbol: str = "BTCUSDT") -> CryptoPrice | None:
        """
        Get current futures price.

        Args:
            symbol: Futures pair (e.g., "BTCUSDT")

        Returns:
            CryptoPrice object or None
        """
        if not self._client:
            raise RuntimeError("Client not connected")

        try:
            async with self._throttler:
                response = await self._client.get(
                    f"{FUTURES_API}/ticker/price",
                    params={"symbol": symbol},
                )
                response.raise_for_status()
                data = response.json()

            return CryptoPrice(
                symbol=f"{symbol}_PERP",
                platform=Platform.BINANCE,
                price=Decimal(data["price"]),
                timestamp=datetime.utcnow(),
            )

        except httpx.HTTPError as e:
            self.logger.error(f"Failed to fetch futures price: {e}")
            return None

    async def get_options(self, underlying: str = "BTC") -> list[dict]:
        """
        Get available options contracts.

        Args:
            underlying: Underlying asset (e.g., "BTC", "ETH")

        Returns:
            List of option contract data
        """
        if not self._client:
            raise RuntimeError("Client not connected")

        try:
            async with self._throttler:
                response = await self._client.get(
                    f"{OPTIONS_API}/exchangeInfo",
                )
                response.raise_for_status()
                data = response.json()

            # Binance uses "BTCUSDT" format for underlying, not just "BTC"
            underlying_usdt = f"{underlying.upper()}USDT"

            # Filter for the specified underlying
            options = [
                opt
                for opt in data.get("optionSymbols", [])
                if opt.get("underlying") == underlying_usdt
            ]

            self.logger.debug(f"Found {len(options)} {underlying} options")
            return options

        except httpx.HTTPError as e:
            self.logger.error(f"Failed to fetch options: {e}")
            return []

    async def get_option_price(self, symbol: str) -> CryptoPrice | None:
        """
        Get current option price with Greeks.

        Args:
            symbol: Option symbol (e.g., "BTC-250131-100000-C")

        Returns:
            CryptoPrice with implied data or None
        """
        if not self._client:
            raise RuntimeError("Client not connected")

        try:
            # Use /mark endpoint which includes Greeks (delta)
            async with self._throttler:
                response = await self._client.get(
                    f"{OPTIONS_API}/mark",
                    params={"symbol": symbol},
                )
                response.raise_for_status()
                data = response.json()

            # Handle list response (Binance returns list for some queries)
            if isinstance(data, list):
                if not data:
                    self.logger.debug(f"No mark data for {symbol}")
                    return None
                data = data[0]

            # Parse option details from symbol
            parts = symbol.split("-")
            strike = Decimal(parts[2]) if len(parts) >= 3 else None
            option_type = parts[3] if len(parts) >= 4 else None

            # Use mark price as the option price
            mark_price = Decimal(str(data.get("markPrice", "0")))

            # Delta from Greeks (probability proxy)
            delta = data.get("delta")
            implied_prob = None
            if delta and option_type:
                delta_val = abs(Decimal(str(delta)))
                # For calls, delta ~ probability of ITM
                # For puts, 1 - |delta| ~ probability of ITM for opposite
                implied_prob = (
                    delta_val if option_type.upper() == "C" else Decimal("1") - delta_val
                )

            return CryptoPrice(
                symbol=symbol,
                platform=Platform.BINANCE,
                price=mark_price,
                timestamp=datetime.utcnow(),
                strike=strike,
                option_type="call" if option_type == "C" else "put",
                implied_probability=implied_prob,
            )

        except httpx.HTTPError as e:
            self.logger.error(f"Failed to fetch option price: {e}")
            return None

    async def get_price_probability(
        self,
        underlying: str,
        target_price: Decimal,
        expiry_date: str,
    ) -> Decimal | None:
        """
        Estimate probability of an asset reaching target price by expiry.

        Uses options delta as a proxy for probability.

        Args:
            underlying: Asset symbol (BTC, ETH)
            target_price: Target price
            expiry_date: Option expiry in format "YYMMDD"

        Returns:
            Estimated probability (0-1) or None
        """
        from datetime import datetime

        # Normalize underlying to uppercase
        underlying = underlying.upper()
        if underlying not in ("BTC", "ETH"):
            self.logger.warning(f"Unsupported underlying: {underlying}")
            return None

        # Get all options
        options = await self.get_options(underlying)
        if not options:
            self.logger.warning(f"No {underlying} options available")
            return None

        # Parse target expiry date
        try:
            target_date = datetime.strptime(expiry_date, "%y%m%d")
        except ValueError:
            self.logger.warning(f"Invalid expiry date format: {expiry_date}")
            return None

        # Get unique expiries and find closest one
        expiry_map: dict[str, datetime] = {}
        for opt in options:
            symbol = opt.get("symbol", "")
            parts = symbol.split("-")
            if len(parts) >= 2:
                exp_str = parts[1]
                try:
                    exp_date = datetime.strptime(exp_str, "%y%m%d")
                    expiry_map[exp_str] = exp_date
                except ValueError:
                    continue

        if not expiry_map:
            self.logger.warning(f"No valid expiries found for {underlying}")
            return None

        # Find closest expiry (prefer on or before target date)
        closest_expiry = min(
            expiry_map.keys(),
            key=lambda x: abs((expiry_map[x] - target_date).days)
        )
        days_diff = abs((expiry_map[closest_expiry] - target_date).days)

        # If more than 7 days apart, probably not useful
        if days_diff > 7:
            self.logger.debug(
                f"Closest {underlying} expiry {closest_expiry} is {days_diff} days from {expiry_date}"
            )
            return None

        # Filter options for this expiry
        relevant_options = [
            opt for opt in options
            if closest_expiry in opt.get("symbol", "") and opt.get("strikePrice")
        ]

        if not relevant_options:
            return None

        # Find closest strike to target price
        closest = min(
            relevant_options,
            key=lambda x: abs(Decimal(str(x.get("strikePrice", 0))) - target_price),
        )

        strike = int(float(closest["strikePrice"]))
        strike_diff = abs(Decimal(str(strike)) - target_price)

        # If strike is too far from target (>20%), not useful
        if target_price > 0 and strike_diff / target_price > Decimal("0.20"):
            self.logger.debug(
                f"Closest strike {strike} is too far from target {target_price}"
            )
            return None

        # Get the call option price for that strike
        call_symbol = f"{underlying}-{closest_expiry}-{strike}-C"
        option_data = await self.get_option_price(call_symbol)

        if option_data and option_data.implied_probability:
            self.logger.info(
                f"Got {underlying} implied probability {option_data.implied_probability:.1%} "
                f"from {call_symbol}"
            )
            return option_data.implied_probability

        return None

    async def get_btc_price_probability(
        self,
        target_price: Decimal,
        expiry_date: str,
    ) -> Decimal | None:
        """
        Estimate probability of BTC reaching target price by expiry.

        Convenience wrapper around get_price_probability for BTC.
        """
        return await self.get_price_probability("BTC", target_price, expiry_date)

    async def get_eth_price_probability(
        self,
        target_price: Decimal,
        expiry_date: str,
    ) -> Decimal | None:
        """
        Estimate probability of ETH reaching target price by expiry.

        Convenience wrapper around get_price_probability for ETH.
        """
        return await self.get_price_probability("ETH", target_price, expiry_date)

    async def __aenter__(self) -> "BinanceClient":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        """Async context manager exit."""
        await self.disconnect()
