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

# Known crypto series IDs for Up/Down markets (matching Rust gamma.rs)
CRYPTO_SERIES_15M = {
    "BTC": "10192",  # BTC Up or Down 15m
    "ETH": "10191",  # ETH Up or Down 15m
    "SOL": "10423",  # SOL Up or Down 15m
    "XRP": "10422",  # XRP Up or Down 15m
}

CRYPTO_SERIES_1H = {
    "BTC": "10114",  # BTC Up or Down 1h
    "ETH": "10117",  # ETH Up or Down 1h
    "SOL": "10122",  # SOL Up or Down 1h
    "XRP": "10123",  # XRP Up or Down 1h
}

CRYPTO_SERIES_4H = {
    "BTC": "10194",  # BTC Up or Down 4h
    "ETH": "10195",  # ETH Up or Down 4h
    "SOL": "10425",  # SOL Up or Down 4h
    "XRP": "10426",  # XRP Up or Down 4h
}

CRYPTO_SERIES_DAILY = {
    "BTC": "10115",  # BTC Up or Down Daily
    "ETH": "10118",  # ETH Up or Down Daily
    "SOL": "10121",  # SOL Up or Down Daily
    "XRP": "10124",  # XRP Up or Down Daily
}


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

    async def get_closed_markets(
        self,
        limit: int = 100,
        offset: int = 0,
        asset_filter: list[str] | None = None,
        timeframe_filter: str | None = None,
    ) -> list[Market]:
        """
        Fetch closed (resolved) markets from Gamma API.

        Args:
            limit: Maximum number of markets to fetch
            offset: Pagination offset
            asset_filter: Optional list of assets to filter (e.g., ['BTC', 'ETH'])
            timeframe_filter: Optional timeframe filter (e.g., '15m', '1h')

        Returns:
            List of resolved Market objects
        """
        if not self._client:
            raise RuntimeError("Client not connected. Call connect() first.")

        params = {
            "limit": limit,
            "offset": offset,
            "closed": "true",
        }

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
                if market and market.resolved:
                    # Apply filters
                    if asset_filter:
                        market_slug = market.raw.get("slug", "").lower() if market.raw else ""
                        market_name = market.name.lower()
                        has_asset = any(
                            asset.lower() in market_slug or asset.lower() in market_name
                            for asset in asset_filter
                        )
                        if not has_asset:
                            continue

                    if timeframe_filter:
                        market_name = market.name.lower()
                        if timeframe_filter.lower() not in market_name:
                            # Check for "15 minute" vs "15m" variations
                            tf_map = {"15m": ["15 minute", "15-minute", "15min"]}
                            variations = tf_map.get(timeframe_filter.lower(), [])
                            if not any(v in market_name for v in variations):
                                continue

                    markets.append(market)

            self.logger.info(f"Fetched {len(markets)} closed markets from Polymarket")
            return markets

        except httpx.HTTPError as e:
            self.logger.error(f"Failed to fetch closed markets: {e}")
            return []

    async def get_price_history(
        self,
        token_id: str,
        start_ts: int | None = None,
        end_ts: int | None = None,
        fidelity: int = 60,
    ) -> list[dict]:
        """
        Fetch historical price data from CLOB API.

        Args:
            token_id: The token ID (YES or NO token)
            start_ts: Start timestamp (Unix seconds)
            end_ts: End timestamp (Unix seconds)
            fidelity: Data granularity in seconds (default 60 = 1 minute)

        Returns:
            List of price history points: [{"t": timestamp, "p": price}, ...]
        """
        if not self._client:
            raise RuntimeError("Client not connected. Call connect() first.")

        params: dict = {"token_id": token_id, "fidelity": fidelity}
        if start_ts:
            params["startTs"] = start_ts
        if end_ts:
            params["endTs"] = end_ts

        try:
            async with self._throttler:
                response = await self._client.get(
                    f"{CLOB_API_URL}/prices-history",
                    params=params,
                )
                response.raise_for_status()
                data = response.json()

            # API returns {"history": [{"t": ts, "p": price}, ...]}
            return data.get("history", [])

        except httpx.HTTPError as e:
            self.logger.error(f"Failed to fetch price history for {token_id}: {e}")
            return []

    async def get_all_closed_crypto_markets(
        self,
        days: int = 30,
        assets: list[str] | None = None,
        timeframe: str = "15m",
    ) -> list[Market]:
        """
        Fetch all closed crypto up/down markets for backtesting.

        Paginates through all available closed markets matching the filters.

        Args:
            days: Number of days to look back
            assets: List of crypto assets (default: ['BTC', 'ETH', 'SOL', 'XRP'])
            timeframe: Market timeframe (default: '15m')

        Returns:
            List of resolved crypto markets
        """
        from datetime import UTC, timedelta

        if assets is None:
            assets = ["BTC", "ETH", "SOL", "XRP"]

        cutoff_date = datetime.now(UTC) - timedelta(days=days)
        all_markets: list[Market] = []
        offset = 0
        batch_size = 100

        while True:
            markets = await self.get_closed_markets(
                limit=batch_size,
                offset=offset,
                asset_filter=assets,
                timeframe_filter=timeframe,
            )

            if not markets:
                break

            # Filter by date and add to results
            for market in markets:
                if market.end_date and market.end_date >= cutoff_date:
                    all_markets.append(market)

            # Check if we should continue pagination
            if len(markets) < batch_size:
                break

            offset += batch_size

            # Safety limit
            if offset > 10000:
                self.logger.warning("Reached safety limit of 10000 markets")
                break

        self.logger.info(
            f"Found {len(all_markets)} closed {timeframe} crypto markets in last {days} days"
        )
        return all_markets

    async def get_closed_events_by_series(
        self,
        series_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """
        Fetch closed events for a specific series from the Gamma API.

        Args:
            series_id: The series ID (e.g., "10192" for BTC 15m)
            limit: Maximum number of events to fetch
            offset: Pagination offset

        Returns:
            List of raw event dicts with nested markets
        """
        if not self._client:
            raise RuntimeError("Client not connected. Call connect() first.")

        params = {
            "series_id": series_id,
            "closed": "true",
            "limit": limit,
            "offset": offset,
        }

        try:
            async with self._throttler:
                response = await self._client.get(
                    f"{GAMMA_API_URL}/events",
                    params=params,
                )
                response.raise_for_status()
                return response.json()

        except httpx.HTTPError as e:
            self.logger.error(f"Failed to fetch events for series {series_id}: {e}")
            return []

    async def get_all_closed_markets_by_series(
        self,
        days: int = 30,
        assets: list[str] | None = None,
        timeframe: str = "15m",
    ) -> list[Market]:
        """
        Fetch all closed crypto Up/Down markets using series_id queries.

        This is the preferred method for fetching historical data because
        it uses series_id to target specific market types, avoiding the
        issue of the API returning unrelated old markets.

        Note: Events are returned oldest first, so we start from a high offset
        and work backwards to get recent events first.

        Args:
            days: Number of days to look back
            assets: List of crypto assets (default: ['BTC', 'ETH', 'SOL', 'XRP'])
            timeframe: Market timeframe ('15m', '1h', '4h', 'daily')

        Returns:
            List of resolved crypto markets
        """
        from datetime import UTC, timedelta
        import json

        if assets is None:
            assets = ["BTC", "ETH", "SOL", "XRP"]

        # Select the right series mapping based on timeframe
        series_map = {
            "15m": CRYPTO_SERIES_15M,
            "1h": CRYPTO_SERIES_1H,
            "4h": CRYPTO_SERIES_4H,
            "daily": CRYPTO_SERIES_DAILY,
        }

        series_ids = series_map.get(timeframe, CRYPTO_SERIES_15M)
        cutoff_date = datetime.now(UTC) - timedelta(days=days)
        all_markets: list[Market] = []

        for asset in assets:
            series_id = series_ids.get(asset)
            if not series_id:
                self.logger.warning(f"No series ID for {asset} {timeframe}")
                continue

            self.logger.info(f"Fetching closed {asset} {timeframe} markets (series={series_id})...")

            # Start from a high offset to get recent events (events are sorted oldest first)
            # For 15m markets, there are ~96 events per day, so 30 days = ~2880 events
            # BTC/ETH have more history (start ~11000), SOL/XRP are newer (start ~7000)
            if timeframe == "15m":
                start_offset = 11000 if asset in ("BTC", "ETH") else 7000
            else:
                start_offset = 1000
            offset = start_offset
            batch_size = 100
            asset_count = 0
            empty_count = 0

            while True:
                events = await self.get_closed_events_by_series(
                    series_id=series_id,
                    limit=batch_size,
                    offset=offset,
                )

                if not events:
                    # Try going backwards if we started too high
                    if offset >= start_offset and start_offset > 0:
                        start_offset = max(0, start_offset - 1000)
                        offset = start_offset
                        self.logger.debug(f"No events at offset {offset + batch_size}, trying {offset}")
                        empty_count += 1
                        if empty_count > 10:
                            break
                        continue
                    break

                reached_cutoff = False
                for event in events:
                    # Parse end_date from event
                    end_date_str = event.get("endDate")
                    event_end_date = None
                    if end_date_str:
                        try:
                            event_end_date = datetime.fromisoformat(
                                end_date_str.replace("Z", "+00:00")
                            )
                        except (ValueError, TypeError):
                            pass

                    # Skip if too old
                    if event_end_date and event_end_date < cutoff_date:
                        reached_cutoff = True
                        continue

                    # Process nested markets in the event
                    for market_data in event.get("markets", []):
                        # Parse resolution from outcomePrices if outcome is not set
                        # Format: ["1", "0"] means first outcome won, ["0", "1"] means second won
                        outcome = market_data.get("outcome")
                        if not outcome:
                            outcome_prices_str = market_data.get("outcomePrices", "[]")
                            outcomes_str = market_data.get("outcomes", "[]")
                            try:
                                if isinstance(outcome_prices_str, str):
                                    outcome_prices = json.loads(outcome_prices_str)
                                else:
                                    outcome_prices = outcome_prices_str
                                if isinstance(outcomes_str, str):
                                    outcomes = json.loads(outcomes_str)
                                else:
                                    outcomes = outcomes_str

                                # Find which outcome has price "1" (the winner)
                                for i, price in enumerate(outcome_prices):
                                    if price == "1" or price == 1:
                                        if i < len(outcomes):
                                            outcome = outcomes[i]
                                            market_data["outcome"] = outcome
                                        break
                            except (json.JSONDecodeError, TypeError, IndexError):
                                pass

                        market = self._parse_market(market_data)
                        if market and market.resolved:
                            all_markets.append(market)
                            asset_count += 1

                # If we've gone past the cutoff date, stop
                if reached_cutoff:
                    break

                # Check if we should continue pagination
                if len(events) < batch_size:
                    break

                offset += batch_size

                # Safety limit - allow up to 10000 events per asset
                if offset > start_offset + 10000:
                    self.logger.warning(f"Reached safety limit for {asset} {timeframe}")
                    break

            self.logger.info(f"Found {asset_count} closed {asset} {timeframe} markets")

        self.logger.info(
            f"Total: {len(all_markets)} closed {timeframe} crypto markets in last {days} days"
        )
        return all_markets
