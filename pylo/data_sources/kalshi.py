"""Kalshi API client."""

import contextlib
import logging
from datetime import datetime
from decimal import Decimal

import httpx
from asyncio_throttle import Throttler

from pylo.config.settings import get_settings
from pylo.data_sources.base import BaseDataSource
from pylo.models.market import Market, MarketOutcome, Platform

logger = logging.getLogger(__name__)

KALSHI_API_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Rate limits (requests per second)
KALSHI_RATE_LIMIT = 10  # Conservative limit


class KalshiClient(BaseDataSource):
    """Client for Kalshi's REST API."""

    name = "kalshi"

    def __init__(self) -> None:
        """Initialize the Kalshi client."""
        super().__init__()
        self.settings = get_settings()
        self._client: httpx.AsyncClient | None = None
        self._throttler = Throttler(rate_limit=KALSHI_RATE_LIMIT, period=1.0)

    async def connect(self) -> None:
        """Initialize HTTP client with authentication."""
        headers = {"Accept": "application/json"}

        # Add auth if credentials available
        if self.settings.has_kalshi_credentials:
            headers["Authorization"] = f"Bearer {self.settings.kalshi_api_key.get_secret_value()}"

        self._client = httpx.AsyncClient(
            timeout=30.0,
            headers=headers,
        )
        self._connected = True
        self.logger.info("Connected to Kalshi API")

    async def disconnect(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
        self._connected = False
        self.logger.info("Disconnected from Kalshi API")

    async def get_markets(
        self,
        limit: int = 100,
        cursor: str | None = None,
        status: str = "open",
    ) -> list[Market]:
        """
        Fetch markets from Kalshi API.

        Args:
            limit: Maximum number of markets to fetch
            cursor: Pagination cursor
            status: Market status filter (open, closed, settled)

        Returns:
            List of Market objects
        """
        if not self._client:
            raise RuntimeError("Client not connected. Call connect() first.")

        params: dict = {
            "limit": limit,
            "status": status,
        }
        if cursor:
            params["cursor"] = cursor

        try:
            async with self._throttler:
                response = await self._client.get(
                    f"{KALSHI_API_URL}/markets",
                    params=params,
                )
                response.raise_for_status()
                data = response.json()

            markets = []
            for item in data.get("markets", []):
                market = self._parse_market(item)
                if market:
                    markets.append(market)

            self.logger.info(f"Fetched {len(markets)} markets from Kalshi")
            return markets

        except httpx.HTTPError as e:
            self.logger.error(f"Failed to fetch markets: {e}")
            return []

    async def get_market(self, market_id: str) -> Market | None:
        """Fetch a specific market by ticker."""
        if not self._client:
            raise RuntimeError("Client not connected. Call connect() first.")

        try:
            async with self._throttler:
                response = await self._client.get(f"{KALSHI_API_URL}/markets/{market_id}")
                response.raise_for_status()
                data = response.json()
            return self._parse_market(data.get("market", {}))

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                self.logger.warning(f"Market not found: {market_id}")
                return None
            raise

    def _parse_market(self, data: dict) -> Market | None:
        """Parse API response into Market model."""
        try:
            # Kalshi uses yes_bid/yes_ask in cents (0-100), or _dollars versions
            # Use mid price (average of bid and ask), fallback to last_price
            yes_bid = Decimal(str(data.get("yes_bid", 0)))
            yes_ask = Decimal(str(data.get("yes_ask", 0)))
            no_bid = Decimal(str(data.get("no_bid", 0)))
            no_ask = Decimal(str(data.get("no_ask", 0)))

            # Calculate mid prices if both bid and ask exist
            if yes_bid > 0 and yes_ask > 0 and yes_ask < 100:
                yes_price = (yes_bid + yes_ask) / 2 / 100  # Convert cents to dollars
            elif data.get("last_price"):
                yes_price = Decimal(str(data.get("last_price", 0))) / 100
            else:
                yes_price = Decimal("0")

            if no_bid > 0 and no_ask > 0 and no_ask < 100:
                no_price = (no_bid + no_ask) / 2 / 100
            else:
                no_price = Decimal("1") - yes_price  # Infer from yes price

            outcomes = [
                MarketOutcome(
                    id=f"{data.get('ticker', '')}_yes",
                    name="YES",
                    price=yes_price,
                    best_bid=yes_bid / 100 if yes_bid else None,
                    best_ask=yes_ask / 100 if yes_ask and yes_ask < 100 else None,
                ),
                MarketOutcome(
                    id=f"{data.get('ticker', '')}_no",
                    name="NO",
                    price=no_price,
                    best_bid=no_bid / 100 if no_bid else None,
                    best_ask=no_ask / 100 if no_ask and no_ask < 100 else None,
                ),
            ]

            # Parse dates
            end_date = None
            if data.get("close_time"):
                with contextlib.suppress(ValueError, TypeError):
                    end_date = datetime.fromisoformat(
                        data["close_time"].replace("Z", "+00:00")
                    )

            market = Market(
                id=data.get("ticker", ""),
                platform=Platform.KALSHI,
                name=data.get("title", "Unknown"),
                description=data.get("rules_primary", ""),
                category=data.get("category", ""),
                outcomes=outcomes,
                volume_24h=Decimal(str(data.get("volume_24h", "0"))),
                end_date=end_date,
                resolved=data.get("status") == "settled",
                resolution=data.get("result"),
                url=f"https://kalshi.com/markets/{data.get('ticker', '')}",
                raw=data,
            )

            return market

        except Exception as e:
            self.logger.error(f"Failed to parse Kalshi market: {e}")
            return None

    async def get_events(self, limit: int = 50) -> list[dict]:
        """
        Fetch events (categories of markets).

        Args:
            limit: Maximum events to fetch

        Returns:
            List of event data
        """
        if not self._client:
            raise RuntimeError("Client not connected. Call connect() first.")

        try:
            async with self._throttler:
                response = await self._client.get(
                    f"{KALSHI_API_URL}/events",
                    params={"limit": limit},
                )
                response.raise_for_status()
                return response.json().get("events", [])

        except httpx.HTTPError as e:
            self.logger.error(f"Failed to fetch events: {e}")
            return []
