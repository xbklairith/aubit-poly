"""Limitless Exchange API client.

Limitless is a Polymarket fork on Base L2 with:
- No KYC required (global access)
- Hourly crypto markets (15m coming soon)
- $750M+ volume traded
- 0% trading fees
- WebSocket orderbook support
"""

import contextlib
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import httpx
from asyncio_throttle import Throttler

from pylo.config.settings import get_settings
from pylo.data_sources.base import BaseDataSource
from pylo.models.market import Market, MarketOutcome, Platform

logger = logging.getLogger(__name__)

LIMITLESS_API_URL = "https://api.limitless.exchange"
LIMITLESS_WS_URL = "wss://ws.limitless.exchange/markets"

# Rate limits (requests per second)
LIMITLESS_RATE_LIMIT = 10  # Conservative limit


@dataclass
class LimitlessMarket:
    """Parsed Limitless market with extracted metadata."""

    slug: str
    asset: str  # BTC, ETH, SOL
    timeframe: str  # 1h, 15m
    end_time: datetime
    yes_position_id: str
    no_position_id: str
    yes_best_bid: Decimal | None
    yes_best_ask: Decimal | None
    no_best_bid: Decimal | None
    no_best_ask: Decimal | None
    title: str = ""
    market_type: str = "single-clob"  # single-clob, group-negrisk, AMM
    exchange_address: str = ""


class LimitlessClient(BaseDataSource):
    """Client for Limitless Exchange REST API."""

    name = "limitless"

    def __init__(self) -> None:
        """Initialize the Limitless client."""
        super().__init__()
        self.settings = get_settings()
        self._client: httpx.AsyncClient | None = None
        self._throttler = Throttler(rate_limit=LIMITLESS_RATE_LIMIT, period=1.0)
        self._api_url = self.settings.limitless_api_url

    async def connect(self) -> None:
        """Initialize HTTP client."""
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        self._client = httpx.AsyncClient(
            timeout=30.0,
            headers=headers,
            base_url=self._api_url,
        )
        self._connected = True
        self.logger.info("Connected to Limitless API")

    async def disconnect(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
        self._connected = False
        self.logger.info("Disconnected from Limitless API")

    async def get_markets(
        self,
        limit: int = 100,
        market_type: str | None = "clob",
    ) -> list[Market]:
        """
        Fetch active markets from Limitless API.

        Args:
            limit: Maximum number of markets to fetch
            market_type: Filter by market type (clob, amm)
                        Only clob markets have orderbooks.

        Returns:
            List of Market objects
        """
        if not self._client:
            raise RuntimeError("Client not connected. Call connect() first.")

        try:
            async with self._throttler:
                response = await self._client.get("/markets/active")
                response.raise_for_status()
                data = response.json()

            markets = []

            # API returns dict keyed by category, each value is a list of markets
            if isinstance(data, dict):
                items = []
                for category_markets in data.values():
                    if isinstance(category_markets, list):
                        items.extend(category_markets)
            else:
                items = data

            for item in items[:limit]:
                # Filter by market type if specified (only clob markets have orderbooks)
                if market_type and item.get("tradeType") != market_type:
                    continue

                market = self._parse_market(item)
                if market:
                    markets.append(market)

            self.logger.info(f"Fetched {len(markets)} CLOB markets from Limitless")
            return markets

        except httpx.HTTPError as e:
            self.logger.error(f"Failed to fetch markets: {e}")
            return []

    async def get_market(self, market_id: str) -> Market | None:
        """Fetch a specific market by slug."""
        if not self._client:
            raise RuntimeError("Client not connected. Call connect() first.")

        try:
            async with self._throttler:
                response = await self._client.get(f"/markets/{market_id}")
                response.raise_for_status()
                data = response.json()
            return self._parse_market(data)

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                self.logger.warning(f"Market not found: {market_id}")
                return None
            raise

    async def get_orderbook(self, slug: str) -> dict | None:
        """
        Fetch orderbook for a specific market.

        Args:
            slug: The market slug (e.g., "btc-hourly-up-down")

        Returns:
            Orderbook data with bids and asks, or None on error.
            Note: AMM markets return 400 error.
        """
        if not self._client:
            raise RuntimeError("Client not connected. Call connect() first.")

        try:
            async with self._throttler:
                response = await self._client.get(f"/markets/{slug}/orderbook")
                response.raise_for_status()
                data = response.json()

            return {
                "yes": self._parse_orderbook_side(data.get("yes", {})),
                "no": self._parse_orderbook_side(data.get("no", {})),
                "slug": slug,
            }

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 400:
                # AMM markets don't have orderbooks
                self.logger.debug(f"No orderbook for {slug} (likely AMM market)")
                return None
            if e.response.status_code == 404:
                self.logger.warning(f"Orderbook not found: {slug}")
                return None
            self.logger.error(f"Failed to fetch orderbook: {e}")
            return None
        except httpx.HTTPError as e:
            self.logger.error(f"Failed to fetch orderbook for {slug}: {e}")
            return None

    def _parse_market(self, data: dict) -> Market | None:
        """Parse API response into Market model."""
        try:
            slug = data.get("slug", "")
            if not slug:
                return None

            # Extract asset and timeframe from slug/title
            title = data.get("title", "")
            asset, timeframe = self._extract_asset_timeframe(slug, title)

            # Parse position IDs from tokens object
            tokens = data.get("tokens", {})
            yes_position_id = str(tokens.get("yes", ""))
            no_position_id = str(tokens.get("no", ""))

            # Parse prices from prices array [yes_price, no_price]
            prices = data.get("prices", [0.5, 0.5])
            yes_price = Decimal(str(prices[0])) if len(prices) > 0 else Decimal("0.5")
            no_price = Decimal(str(prices[1])) if len(prices) > 1 else Decimal("0.5")

            outcomes = [
                MarketOutcome(
                    id=yes_position_id or f"{slug}_yes",
                    name="YES",
                    price=yes_price,
                    best_bid=None,  # Need orderbook fetch for bid/ask
                    best_ask=yes_price,  # Use price as approximate ask
                ),
                MarketOutcome(
                    id=no_position_id or f"{slug}_no",
                    name="NO",
                    price=no_price,
                    best_bid=None,
                    best_ask=no_price,
                ),
            ]

            # Parse end time from expirationDate or createdAt
            end_date = None
            expiration_str = data.get("expirationDate", "")
            if expiration_str:
                # Format: "Jan 26, 2026" - parse it
                with contextlib.suppress(ValueError, TypeError):
                    end_date = datetime.strptime(expiration_str, "%b %d, %Y")
                    # Set to end of day UTC
                    end_date = end_date.replace(hour=23, minute=59, second=59)

            # Try ISO format fields
            if not end_date:
                for time_field in ["endTime", "end_time", "closeTime", "close_time", "createdAt"]:
                    if data.get(time_field):
                        with contextlib.suppress(ValueError, TypeError):
                            end_date = datetime.fromisoformat(
                                str(data[time_field]).replace("Z", "+00:00")
                            )
                            break

            # Parse liquidity/volume
            liquidity = Decimal("0")
            volume_str = data.get("volume", "0")
            if volume_str:
                with contextlib.suppress(ValueError, TypeError):
                    liquidity = Decimal(str(volume_str))

            # Get categories
            categories = data.get("categories", [])
            category = categories[0] if categories else data.get("tags", ["crypto"])[0] if data.get("tags") else "crypto"

            market = Market(
                id=slug,
                platform=Platform.LIMITLESS,
                name=title or slug,
                description=data.get("description", ""),
                category=category,
                outcomes=outcomes,
                volume_24h=liquidity,
                liquidity=liquidity,
                end_date=end_date,
                resolved=data.get("status") == "RESOLVED",
                resolution=data.get("result"),
                url=f"https://limitless.exchange/markets/{slug}",
                raw=data,
            )

            return market

        except Exception as e:
            self.logger.error(f"Failed to parse Limitless market: {e}")
            return None

    def _extract_asset_timeframe(self, slug: str, title: str) -> tuple[str, str]:
        """
        Extract asset and timeframe from slug/title.

        Examples:
            "btc-hourly-up-down" -> ("BTC", "1h")
            "eth-15m-above-1234" -> ("ETH", "15m")
            "sol-1h-volatility" -> ("SOL", "1h")
        """
        slug_upper = slug.upper()
        title_upper = title.upper()

        # Extract asset
        asset = "UNKNOWN"
        for a in ["BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "MATIC", "DOT"]:
            if a in slug_upper or a in title_upper:
                asset = a
                break

        # Also check for full names
        asset_names = {
            "BITCOIN": "BTC",
            "ETHEREUM": "ETH",
            "SOLANA": "SOL",
            "RIPPLE": "XRP",
            "DOGECOIN": "DOGE",
            "CARDANO": "ADA",
        }
        for name, symbol in asset_names.items():
            if name in title_upper:
                asset = symbol
                break

        # Extract timeframe
        timeframe = "unknown"
        slug_lower = slug.lower()
        title_lower = title.lower()

        if "hourly" in slug_lower or "hourly" in title_lower or "-1h-" in slug_lower:
            timeframe = "1h"
        elif "15m" in slug_lower or "15 min" in title_lower or "15-minute" in title_lower:
            timeframe = "15m"
        elif "daily" in slug_lower or "24h" in title_lower or "24 hour" in title_lower:
            timeframe = "daily"
        elif "weekly" in slug_lower or "weekly" in title_lower:
            timeframe = "weekly"
        else:
            # Try to extract from patterns like "1h", "4h", etc.
            time_match = re.search(r"(\d+)h", slug_lower)
            if time_match:
                timeframe = f"{time_match.group(1)}h"
            else:
                # Detect hourly markets from expiry time pattern: "14:00 UTC", "15:00 UTC"
                # If time ends in :00, it's likely an hourly market
                hour_match = re.search(r"(\d{1,2}):00\s*UTC", title, re.IGNORECASE)
                if hour_match:
                    timeframe = "1h"

        return asset, timeframe

    def _parse_orderbook_side(self, data: dict) -> dict:
        """
        Parse one side of the orderbook.

        Args:
            data: Orderbook side data with bids/asks arrays

        Returns:
            Dict with best price, total depth, and levels
        """
        bids = data.get("bids", [])
        asks = data.get("asks", [])

        # Combine and process levels
        all_levels = []
        total_depth = Decimal("0")

        for level in asks:  # asks for buy side
            if isinstance(level, dict):
                price = Decimal(str(level.get("price", 0)))
                quantity = Decimal(str(level.get("size", level.get("quantity", 0))))
            elif isinstance(level, list | tuple) and len(level) >= 2:
                price = Decimal(str(level[0]))
                quantity = Decimal(str(level[1]))
            else:
                continue

            if price > 0:
                all_levels.append({"price": price, "quantity": quantity})
                total_depth += quantity

        best_price = all_levels[0]["price"] if all_levels else None

        return {
            "best_price": best_price,
            "total_depth": total_depth,
            "levels": all_levels,
            "bids": bids,
            "asks": asks,
        }

    async def get_market_with_orderbook(self, slug: str) -> Market | None:
        """
        Fetch a market with orderbook depth data.

        Args:
            slug: The market slug

        Returns:
            Market with orderbook depth populated, or None
        """
        import asyncio

        market_task = self.get_market(slug)
        orderbook_task = self.get_orderbook(slug)

        market, orderbook = await asyncio.gather(market_task, orderbook_task)

        if not market:
            return None

        # Enrich market with orderbook depth
        if orderbook:
            for outcome in market.outcomes:
                if outcome.name.upper() == "YES":
                    yes_data = orderbook.get("yes", {})
                    outcome.ask_depth = yes_data.get("total_depth", Decimal("0"))
                    if yes_data.get("best_price"):
                        outcome.best_ask = yes_data["best_price"]
                elif outcome.name.upper() == "NO":
                    no_data = orderbook.get("no", {})
                    outcome.ask_depth = no_data.get("total_depth", Decimal("0"))
                    if no_data.get("best_price"):
                        outcome.best_ask = no_data["best_price"]

        return market

    async def get_crypto_markets(self, assets: list[str] | None = None) -> list[Market]:
        """
        Fetch crypto-specific markets (BTC, ETH, SOL hourly).

        Args:
            assets: Filter to specific assets (default: all crypto)

        Returns:
            List of crypto prediction markets
        """
        markets = await self.get_markets(limit=500, market_type="single-clob")

        if not assets:
            return markets

        assets_upper = [a.upper() for a in assets]
        return [m for m in markets if self._extract_asset_timeframe(m.id, m.name)[0] in assets_upper]

    def to_parsed_market(self, market: Market) -> LimitlessMarket:
        """Convert Market to LimitlessMarket dataclass for DB operations."""
        asset, timeframe = self._extract_asset_timeframe(market.id, market.name)

        yes_outcome = next((o for o in market.outcomes if o.name.upper() == "YES"), None)
        no_outcome = next((o for o in market.outcomes if o.name.upper() == "NO"), None)

        return LimitlessMarket(
            slug=market.id,
            asset=asset,
            timeframe=timeframe,
            end_time=market.end_date or datetime.utcnow(),
            yes_position_id=yes_outcome.id if yes_outcome else "",
            no_position_id=no_outcome.id if no_outcome else "",
            yes_best_bid=yes_outcome.best_bid if yes_outcome else None,
            yes_best_ask=yes_outcome.best_ask if yes_outcome else None,
            no_best_bid=no_outcome.best_bid if no_outcome else None,
            no_best_ask=no_outcome.best_ask if no_outcome else None,
            title=market.name,
            market_type=market.raw.get("market_type", "single-clob") if market.raw else "single-clob",
            exchange_address=market.raw.get("venue", {}).get("exchange", "") if market.raw else "",
        )
