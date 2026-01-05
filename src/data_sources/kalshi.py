"""Kalshi API client."""

import contextlib
import logging
from datetime import datetime
from decimal import Decimal

import httpx

from src.config.settings import get_settings
from src.data_sources.base import BaseDataSource
from src.models.market import Market, MarketOutcome, Platform

logger = logging.getLogger(__name__)

KALSHI_API_URL = "https://api.elections.kalshi.com/trade-api/v2"


class KalshiClient(BaseDataSource):
    """Client for Kalshi's REST API."""

    name = "kalshi"

    def __init__(self) -> None:
        """Initialize the Kalshi client."""
        super().__init__()
        self.settings = get_settings()
        self._client: httpx.AsyncClient | None = None

    async def connect(self) -> None:
        """Initialize HTTP client with authentication."""
        headers = {"Accept": "application/json"}

        # Add auth if credentials available
        if self.settings.has_kalshi_credentials:
            headers["Authorization"] = f"Bearer {self.settings.kalshi_api_key}"

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
            # Kalshi uses yes_price and no_price directly
            yes_price = Decimal(str(data.get("yes_price", 0))) / 100  # Cents to dollars
            no_price = Decimal(str(data.get("no_price", 0))) / 100

            outcomes = [
                MarketOutcome(
                    id=f"{data.get('ticker', '')}_yes",
                    name="YES",
                    price=yes_price,
                ),
                MarketOutcome(
                    id=f"{data.get('ticker', '')}_no",
                    name="NO",
                    price=no_price,
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
            response = await self._client.get(
                f"{KALSHI_API_URL}/events",
                params={"limit": limit},
            )
            response.raise_for_status()
            return response.json().get("events", [])

        except httpx.HTTPError as e:
            self.logger.error(f"Failed to fetch events: {e}")
            return []
