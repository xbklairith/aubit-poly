"""Polymarket API client."""

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

# Polymarket API endpoints
GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"

# Rate limits (requests per second)
POLYMARKET_RATE_LIMIT = 10  # Conservative limit


class PolymarketClient(BaseDataSource):
    """Client for Polymarket's Gamma and CLOB APIs."""

    name = "polymarket"

    def __init__(self) -> None:
        """Initialize the Polymarket client."""
        super().__init__()
        self.settings = get_settings()
        self._client: httpx.AsyncClient | None = None
        self._throttler = Throttler(rate_limit=POLYMARKET_RATE_LIMIT, period=1.0)

    async def connect(self) -> None:
        """Initialize HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=30.0,
                headers={"Accept": "application/json"},
            )
        self._connected = True
        self.logger.info("Connected to Polymarket API")

    async def disconnect(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
        self._connected = False
        self.logger.info("Disconnected from Polymarket API")

    async def get_markets(
        self,
        limit: int = 100,
        offset: int = 0,
        active: bool = True,
    ) -> list[Market]:
        """
        Fetch markets from Polymarket's Gamma API.

        Args:
            limit: Maximum number of markets to fetch
            offset: Pagination offset
            active: Only fetch active (non-resolved) markets

        Returns:
            List of Market objects
        """
        if not self._client:
            raise RuntimeError("Client not connected. Call connect() first.")

        params = {
            "limit": limit,
            "offset": offset,
        }

        # Use closed=false to get actually open markets (active=true doesn't work properly)
        if active:
            params["closed"] = "false"

        try:
            async with self._throttler:
                response = await self._client.get(
                    f"{GAMMA_API_URL}/markets",
                    params=params,
                )
                response.raise_for_status()
                data = response.json()

            markets = []
            for item in data:
                market = self._parse_market(item)
                if market:
                    markets.append(market)

            self.logger.info(f"Fetched {len(markets)} markets from Polymarket")
            return markets

        except httpx.HTTPError as e:
            self.logger.error(f"Failed to fetch markets: {e}")
            return []

    async def get_market(self, market_id: str) -> Market | None:
        """
        Fetch a specific market by condition ID.

        Args:
            market_id: The market's condition ID

        Returns:
            Market object or None if not found
        """
        if not self._client:
            raise RuntimeError("Client not connected. Call connect() first.")

        try:
            async with self._throttler:
                response = await self._client.get(f"{GAMMA_API_URL}/markets/{market_id}")
                response.raise_for_status()
                data = response.json()
            return self._parse_market(data)

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                self.logger.warning(f"Market not found: {market_id}")
                return None
            raise

    async def get_prices(self, token_id: str) -> dict | None:
        """
        Fetch current prices from CLOB API.

        Args:
            token_id: The token ID for price lookup

        Returns:
            Price data dict or None
        """
        if not self._client:
            raise RuntimeError("Client not connected. Call connect() first.")

        try:
            async with self._throttler:
                response = await self._client.get(
                    f"{CLOB_API_URL}/prices",
                    params={"token_id": token_id},
                )
                response.raise_for_status()
                return response.json()

        except httpx.HTTPError as e:
            self.logger.error(f"Failed to fetch prices for {token_id}: {e}")
            return None

    async def get_orderbook(self, token_id: str) -> dict | None:
        """
        Fetch order book from CLOB API.

        Args:
            token_id: The token ID

        Returns:
            Order book data or None
        """
        if not self._client:
            raise RuntimeError("Client not connected. Call connect() first.")

        try:
            async with self._throttler:
                response = await self._client.get(
                    f"{CLOB_API_URL}/book",
                    params={"token_id": token_id},
                )
                response.raise_for_status()
                return response.json()

        except httpx.HTTPError as e:
            self.logger.error(f"Failed to fetch orderbook for {token_id}: {e}")
            return None

    def _parse_market(self, data: dict) -> Market | None:
        """Parse API response into Market model."""
        import json

        try:
            # Extract outcomes - try multiple formats
            outcomes = []

            # Format 1: tokens array (older API format)
            tokens = data.get("tokens", [])
            if tokens:
                for token in tokens:
                    outcome = MarketOutcome(
                        id=token.get("token_id", ""),
                        name=token.get("outcome", "Unknown"),
                        price=Decimal(str(token.get("price", "0"))),
                    )
                    outcomes.append(outcome)

            # Format 2: outcomes + outcomePrices as JSON strings (current API format)
            if not outcomes:
                outcome_names = data.get("outcomes", "[]")
                outcome_prices = data.get("outcomePrices", "[]")

                # Parse JSON strings
                if isinstance(outcome_names, str):
                    outcome_names = json.loads(outcome_names)
                if isinstance(outcome_prices, str):
                    outcome_prices = json.loads(outcome_prices)

                clob_ids = data.get("clobTokenIds", "[]")
                if isinstance(clob_ids, str):
                    clob_ids = json.loads(clob_ids)

                for i, name in enumerate(outcome_names):
                    price = Decimal(str(outcome_prices[i])) if i < len(outcome_prices) else Decimal("0")
                    token_id = clob_ids[i] if i < len(clob_ids) else ""
                    outcome = MarketOutcome(
                        id=token_id,
                        name=name,
                        price=price,
                    )
                    outcomes.append(outcome)

            # Parse dates - handle both camelCase and snake_case
            end_date = None
            end_date_str = data.get("endDate") or data.get("end_date_iso") or data.get("endDateIso")
            if end_date_str:
                with contextlib.suppress(ValueError, TypeError):
                    end_date = datetime.fromisoformat(
                        end_date_str.replace("Z", "+00:00")
                    )

            # Build market object
            market = Market(
                id=data.get("condition_id", data.get("id", "")),
                platform=Platform.POLYMARKET,
                name=data.get("question", data.get("title", "Unknown")),
                description=data.get("description", ""),
                category=data.get("category", ""),
                outcomes=outcomes,
                volume_24h=Decimal(str(data.get("volume_24hr", "0"))),
                liquidity=Decimal(str(data.get("liquidity", "0"))),
                end_date=end_date,
                resolved=data.get("closed", False) or data.get("resolved", False),
                resolution=data.get("outcome"),
                url=f"https://polymarket.com/event/{data.get('slug', '')}",
                raw=data,
            )

            return market

        except Exception as e:
            self.logger.error(f"Failed to parse market: {e}")
            return None

    async def get_crypto_markets(self) -> list[Market]:
        """
        Fetch crypto-related markets from events API.

        Returns:
            List of crypto Market objects
        """
        if not self._client:
            raise RuntimeError("Client not connected. Call connect() first.")

        crypto_keywords = [
            "bitcoin", "btc", "ethereum", "eth", "crypto", "solana",
            "token", "defi", "coin"
        ]

        try:
            async with self._throttler:
                response = await self._client.get(
                    f"{GAMMA_API_URL}/events",
                    params={"closed": "false", "limit": 500},
                )
                response.raise_for_status()
                events = response.json()

            markets = []
            for event in events:
                title = event.get("title", "").lower()
                slug = event.get("slug", "").lower()

                # Check if crypto-related
                is_crypto = any(kw in title or kw in slug for kw in crypto_keywords)
                # Exclude false positives
                if "minister" in title or "president" in title or "cabinet" in title:
                    is_crypto = False

                if is_crypto:
                    for m in event.get("markets", []):
                        market = self._parse_market(m)
                        if market:
                            markets.append(market)

            self.logger.info(f"Fetched {len(markets)} crypto markets from Polymarket")
            return markets

        except httpx.HTTPError as e:
            self.logger.error(f"Failed to fetch crypto markets: {e}")
            return []

    async def search_markets(self, query: str, limit: int = 20) -> list[Market]:
        """
        Search markets by keyword.

        Args:
            query: Search query
            limit: Maximum results

        Returns:
            List of matching markets
        """
        if not self._client:
            raise RuntimeError("Client not connected. Call connect() first.")

        try:
            async with self._throttler:
                response = await self._client.get(
                    f"{GAMMA_API_URL}/markets",
                    params={
                        "search": query,
                        "limit": limit,
                        "active": "true",
                    },
                )
                response.raise_for_status()
                data = response.json()

            return [m for item in data if (m := self._parse_market(item))]

        except httpx.HTTPError as e:
            self.logger.error(f"Search failed: {e}")
            return []
