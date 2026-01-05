"""Binance exchange client for spot, futures, and options data."""

import logging
from datetime import datetime
from decimal import Decimal

import httpx

from src.config.settings import get_settings
from src.models.market import CryptoPrice, Platform

logger = logging.getLogger(__name__)

# Binance API endpoints
SPOT_API = "https://api.binance.com/api/v3"
FUTURES_API = "https://fapi.binance.com/fapi/v1"
OPTIONS_API = "https://eapi.binance.com/eapi/v1"


class BinanceClient:
    """Client for Binance spot, futures, and options APIs."""

    name = "binance"

    def __init__(self) -> None:
        """Initialize the Binance client."""
        self.settings = get_settings()
        self._client: httpx.AsyncClient | None = None
        self._connected = False
        self.logger = logging.getLogger(f"{__name__}.{self.name}")

    async def connect(self) -> None:
        """Initialize HTTP client."""
        headers = {"Accept": "application/json"}

        # Add API key if available (for authenticated endpoints)
        if self.settings.has_binance_credentials:
            headers["X-MBX-APIKEY"] = self.settings.binance_api_key

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
            response = await self._client.get(
                f"{OPTIONS_API}/exchangeInfo",
            )
            response.raise_for_status()
            data = response.json()

            # Filter for the specified underlying
            options = [
                opt
                for opt in data.get("optionSymbols", [])
                if opt.get("underlying") == underlying
            ]

            return options

        except httpx.HTTPError as e:
            self.logger.error(f"Failed to fetch options: {e}")
            return []

    async def get_option_price(self, symbol: str) -> CryptoPrice | None:
        """
        Get current option price.

        Args:
            symbol: Option symbol (e.g., "BTC-250131-100000-C")

        Returns:
            CryptoPrice with implied data or None
        """
        if not self._client:
            raise RuntimeError("Client not connected")

        try:
            response = await self._client.get(
                f"{OPTIONS_API}/ticker",
                params={"symbol": symbol},
            )
            response.raise_for_status()
            data = response.json()

            # Parse option details from symbol
            parts = symbol.split("-")
            strike = Decimal(parts[2]) if len(parts) >= 3 else None
            option_type = parts[3] if len(parts) >= 4 else None

            # Use mark price as the option price
            mark_price = Decimal(str(data.get("markPrice", "0")))

            # Delta can be used as rough probability proxy
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

    async def get_btc_price_probability(
        self,
        target_price: Decimal,
        expiry_date: str,
    ) -> Decimal | None:
        """
        Estimate probability of BTC reaching target price by expiry.

        Uses options delta as a proxy for probability.

        Args:
            target_price: Target BTC price
            expiry_date: Option expiry in format "YYMMDD"

        Returns:
            Estimated probability (0-1) or None
        """
        # Find the closest strike option
        options = await self.get_options("BTC")

        # Filter for the specific expiry
        relevant_options = [
            opt
            for opt in options
            if expiry_date in opt.get("symbol", "")
            and opt.get("strikePrice")
        ]

        if not relevant_options:
            self.logger.warning(f"No options found for expiry {expiry_date}")
            return None

        # Find closest strike to target
        closest = min(
            relevant_options,
            key=lambda x: abs(Decimal(str(x.get("strikePrice", 0))) - target_price),
        )

        # Get the call option price for that strike
        call_symbol = f"BTC-{expiry_date}-{int(closest['strikePrice'])}-C"
        option_data = await self.get_option_price(call_symbol)

        if option_data and option_data.implied_probability:
            return option_data.implied_probability

        return None

    async def __aenter__(self) -> "BinanceClient":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        """Async context manager exit."""
        await self.disconnect()
