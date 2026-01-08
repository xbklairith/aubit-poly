"""Market monitor for discovering and tracking Up/Down markets."""

import json
import logging
import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import httpx
from asyncio_throttle import Throttler

from pylo.bots.models import Asset, MarketType, Timeframe, UpDownMarket
from pylo.config.settings import get_settings

logger = logging.getLogger(__name__)

# API endpoints
GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"

# Rate limiting
RATE_LIMIT = 10  # requests per second

# Known Up/Down series slugs
UP_DOWN_SERIES = {
    "BTC": ["btc-up-or-down-daily", "btc-up-or-down-hourly"],
    "ETH": ["eth-up-or-down-daily", "eth-up-or-down-hourly"],
    "SOL": ["sol-up-or-down-daily", "sol-up-or-down-hourly"],
}


class MarketMonitor:
    """Discovers and monitors Polymarket Up/Down markets."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self._client: Optional[httpx.AsyncClient] = None
        self._throttler = Throttler(rate_limit=RATE_LIMIT, period=1.0)
        self._markets: dict[str, UpDownMarket] = {}

    async def __aenter__(self) -> "MarketMonitor":
        """Async context manager entry."""
        self._client = httpx.AsyncClient(timeout=30.0)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        if self._client:
            await self._client.aclose()
            self._client = None

    def clear_cache(self) -> None:
        """Clear the events cache to force fresh fetch."""
        if hasattr(self, "_cached_events"):
            delattr(self, "_cached_events")
        if hasattr(self, "_cached_events_time"):
            delattr(self, "_cached_events_time")

    async def discover_markets(self, force_refresh: bool = False) -> list[UpDownMarket]:
        """Discover all active binary markets (Up/Down, Above, Price Range, Sports)."""
        if force_refresh:
            self.clear_cache()

        all_markets: list[UpDownMarket] = []
        assets = self.settings.get_spread_bot_assets()

        # 1. Discover Up/Down markets for each asset
        for asset in assets:
            try:
                markets = await self._search_up_down_markets(asset)
                all_markets.extend(markets)
                logger.info(f"Found {len(markets)} {asset} Up/Down markets")
            except Exception as e:
                logger.error(f"Error discovering {asset} Up/Down markets: {e}")

        # 2. Discover Crypto Above markets
        try:
            above_markets = await self._search_crypto_above_markets()
            all_markets.extend(above_markets)
            if above_markets:
                logger.info(f"Found {len(above_markets)} Crypto Above markets")
        except Exception as e:
            logger.error(f"Error discovering Above markets: {e}")

        # 3. Discover Crypto Price Range markets
        try:
            price_range_markets = await self._search_crypto_price_range_markets()
            all_markets.extend(price_range_markets)
            if price_range_markets:
                logger.info(f"Found {len(price_range_markets)} Price Range markets")
        except Exception as e:
            logger.error(f"Error discovering Price Range markets: {e}")

        # 4. Sports markets - DISABLED (different structure, not YES/NO binary)
        # Sports markets have Team A vs Team B as separate markets, not complementary YES/NO
        # try:
        #     sports_markets = await self._search_sports_markets()
        #     all_markets.extend(sports_markets)
        #     if sports_markets:
        #         logger.info(f"Found {len(sports_markets)} Sports markets")
        # except Exception as e:
        #     logger.error(f"Error discovering Sports markets: {e}")

        # Cache markets
        for market in all_markets:
            self._markets[market.id] = market

        return all_markets

    async def _search_up_down_markets(self, asset: str) -> list[UpDownMarket]:
        """Search for Up/Down markets for a specific asset.

        Returns 2 nearest markets per timeframe (15m and hourly),
        filtered to only include markets expiring within max_time_to_expiry.
        """
        if not self._client:
            return []

        markets_15m: list[UpDownMarket] = []
        markets_hourly: list[UpDownMarket] = []
        max_expiry = self.settings.spread_bot_max_time_to_expiry

        # Fetch all open events with pagination
        all_events = await self._fetch_all_open_events()

        # Filter for this asset's Up/Down markets (15m and hourly only)
        for event in all_events:
            if self._is_up_down_event_15m_or_hourly(event, asset):
                event_markets = self._parse_event_markets(event, asset)
                for market in event_markets:
                    # Skip expired markets
                    if market.is_expired:
                        continue
                    # Skip markets expiring too far in the future
                    if market.time_to_expiry > max_expiry:
                        continue
                    # Categorize by timeframe
                    if market.timeframe == Timeframe.FIFTEEN_MIN:
                        markets_15m.append(market)
                    else:
                        markets_hourly.append(market)

        # Sort each by expiry time and take nearest 2
        markets_15m.sort(key=lambda m: m.end_time)
        markets_hourly.sort(key=lambda m: m.end_time)

        # Take 2 nearest from each timeframe
        selected = markets_15m[:2] + markets_hourly[:2]

        # Deduplicate by market ID
        seen = set()
        unique_markets = []
        for market in selected:
            if market.id not in seen:
                seen.add(market.id)
                unique_markets.append(market)

        # Sort by expiry time (nearest first)
        unique_markets.sort(key=lambda m: m.end_time)
        return unique_markets

    async def _fetch_all_open_events(self) -> list[dict]:
        """Fetch all open events with pagination (cached)."""
        if not self._client:
            return []

        # Use cached events if available and recent
        cache_key = "_cached_events"
        cache_time_key = "_cached_events_time"

        if hasattr(self, cache_key):
            cache_age = (datetime.now(timezone.utc) - getattr(self, cache_time_key)).total_seconds()
            if cache_age < 60:  # Cache for 60 seconds
                return getattr(self, cache_key)

        all_events = []
        offset = 0
        max_events = 5000  # Reasonable limit

        while offset < max_events:
            try:
                async with self._throttler:
                    response = await self._client.get(
                        f"{GAMMA_API_URL}/events",
                        params={
                            "closed": "false",
                            "limit": 500,
                            "offset": offset,
                        },
                    )

                if response.status_code != 200:
                    break

                events = response.json()
                if not events:
                    break

                all_events.extend(events)
                offset += 500

                if len(events) < 500:
                    break

            except Exception as e:
                logger.debug(f"Error fetching events at offset {offset}: {e}")
                break

        # Cache the results
        setattr(self, cache_key, all_events)
        setattr(self, cache_time_key, datetime.now(timezone.utc))

        logger.debug(f"Fetched {len(all_events)} total events")
        return all_events

    def _is_up_down_event_15m_or_hourly(self, event: dict, asset: str) -> bool:
        """Check if an event is a 15-minute or hourly Up/Down market for the asset."""
        title = event.get("title", "").lower()
        slug = event.get("slug", "").lower()

        asset_patterns = {
            "BTC": ["btc", "bitcoin"],
            "ETH": ["eth", "ethereum"],
            "SOL": ["sol", "solana"],
            "XRP": ["xrp"],
        }
        names = asset_patterns.get(asset.upper(), [asset.lower()])

        # First check if it's an Up/Down market for this asset
        is_asset_match = False
        for name in names:
            if name in slug or name in title:
                is_asset_match = True
                break

        if not is_asset_match:
            return False

        # Check if it's an Up/Down market
        is_up_down = ("updown" in slug or "up-or-down" in slug or
                      ("up" in title and "down" in title))

        if not is_up_down:
            return False

        # Exclude 5-minute markets (slug contains "5m")
        if "-5m-" in slug or "updown-5m" in slug:
            return False

        # Exclude 4-hour markets
        if "-4h-" in slug or "updown-4h" in slug:
            return False

        # Exclude daily markets (pattern: "on january X" without time)
        # Daily slugs: bitcoin-up-or-down-on-january-7 (no time like "1pm")
        if "on-january" in slug or "on-february" in slug:
            # Check if it has a time component (hourly)
            if not any(t in slug for t in ["am-et", "pm-et"]):
                return False

        # Include 15-minute markets (slug contains "15m")
        if "-15m-" in slug or "updown-15m" in slug:
            return True

        # Include hourly markets (slug contains time like "1pm-et", "2am-et")
        import re
        hourly_pattern = r"\d{1,2}(am|pm)-et"
        if re.search(hourly_pattern, slug):
            return True

        return False

    def _is_up_down_event(self, event: dict, asset: str) -> bool:
        """Check if an event is an Up/Down market for the given asset (all timeframes)."""
        title = event.get("title", "").lower()
        slug = event.get("slug", "").lower()

        asset_patterns = {
            "BTC": ["btc", "bitcoin"],
            "ETH": ["eth", "ethereum"],
            "SOL": ["sol", "solana"],
            "XRP": ["xrp"],
        }
        names = asset_patterns.get(asset.upper(), [asset.lower()])

        # Check if this is an Up/Down market for the asset
        for name in names:
            # Slug patterns: btc-updown-5m, bitcoin-up-or-down, btc-up-or-down
            if (name in slug) and ("updown" in slug or "up-or-down" in slug):
                return True
            # Title patterns: "Bitcoin Up or Down", "BTC Up or Down"
            if (name in title) and ("up" in title) and ("down" in title):
                return True

        return False

    def _parse_event_markets(self, event: dict, asset: str) -> list[UpDownMarket]:
        """Parse markets from an event."""
        markets = []
        event_markets = event.get("markets", [])

        for market_data in event_markets:
            market = self._parse_market(market_data, asset)
            if market and not market.is_expired:
                markets.append(market)

        return markets

    def _is_up_down_market(self, question: str, asset: str) -> bool:
        """Check if a market question is an Up/Down market for the asset."""
        question_lower = question.lower()
        asset_lower = asset.lower()

        # Match patterns like "Bitcoin Up or Down" or "BTC Up or Down"
        asset_names = {
            "BTC": ["btc", "bitcoin"],
            "ETH": ["eth", "ethereum"],
            "SOL": ["sol", "solana"],
            "XRP": ["xrp"],
        }

        names = asset_names.get(asset.upper(), [asset_lower])

        for name in names:
            if name in question_lower and "up or down" in question_lower:
                return True

        return False

    def _parse_market(self, data: dict, asset: str) -> Optional[UpDownMarket]:
        """Parse a market from API response."""
        try:
            market_id = data.get("id", data.get("condition_id", ""))
            question = data.get("question", "")

            # Extract end date
            end_date_str = data.get("endDate", data.get("end_date_iso", ""))
            if end_date_str:
                end_time = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
            else:
                return None

            # Determine timeframe from question
            timeframe = self._extract_timeframe(question)

            # Get token IDs - handle both list and JSON string formats
            yes_token_id = ""
            no_token_id = ""

            # Try clobTokenIds first (most common format in event markets)
            clob_token_ids = data.get("clobTokenIds", [])
            if isinstance(clob_token_ids, str):
                try:
                    clob_token_ids = json.loads(clob_token_ids)
                except (json.JSONDecodeError, ValueError):
                    clob_token_ids = []

            if isinstance(clob_token_ids, list) and len(clob_token_ids) >= 2:
                yes_token_id = str(clob_token_ids[0])
                no_token_id = str(clob_token_ids[1])

            # Fallback to tokens array
            if not yes_token_id or not no_token_id:
                tokens = data.get("tokens", [])
                for token in tokens:
                    outcome = token.get("outcome", "").upper()
                    if outcome in ("YES", "UP"):
                        yes_token_id = token.get("token_id", "")
                    elif outcome in ("NO", "DOWN"):
                        no_token_id = token.get("token_id", "")

            if not yes_token_id or not no_token_id:
                logger.debug(f"No token IDs found for market {question}")
                return None

            # Get prices from outcomes - try bestAsk/bestBid first, then outcomePrices
            yes_price = Decimal(str(data.get("bestAsk", "0.5")))
            no_price = Decimal("1") - yes_price  # Default: complement

            outcome_prices = data.get("outcomePrices", "")
            if outcome_prices:
                if isinstance(outcome_prices, str):
                    try:
                        prices = json.loads(outcome_prices)
                        if len(prices) >= 2:
                            yes_price = Decimal(str(prices[0]))
                            no_price = Decimal(str(prices[1]))
                    except (json.JSONDecodeError, ValueError):
                        pass
                elif isinstance(outcome_prices, list) and len(outcome_prices) >= 2:
                    yes_price = Decimal(str(outcome_prices[0]))
                    no_price = Decimal(str(outcome_prices[1]))

            return UpDownMarket(
                id=market_id,
                name=question,
                asset=Asset(asset.upper()),
                timeframe=timeframe,
                end_time=end_time,
                yes_token_id=yes_token_id,
                no_token_id=no_token_id,
                condition_id=data.get("condition_id", market_id),
                yes_ask=yes_price,  # Will be updated with order book data
                no_ask=no_price,
                volume=Decimal(str(data.get("volume", 0) or 0)),
                liquidity=Decimal(str(data.get("liquidity", 0) or 0)),
                fetched_at=datetime.now(timezone.utc),
            )

        except Exception as e:
            logger.debug(f"Error parsing market: {e}")
            return None

    def _extract_timeframe(self, question: str) -> Timeframe:
        """Extract timeframe from market question."""
        question_lower = question.lower()

        # Check for 15-minute patterns
        # e.g., "1:30PM-1:45PM ET" or "updown-15m"
        if "15 min" in question_lower or "15min" in question_lower or "-15m" in question_lower:
            return Timeframe.FIFTEEN_MIN

        # Check for time range pattern (indicates 15-minute market)
        # e.g., "1:30PM-1:45PM" or "2:00PM-2:15PM"
        time_range_pattern = r"\d{1,2}:\d{2}(am|pm)-\d{1,2}:\d{2}(am|pm)"
        if re.search(time_range_pattern, question_lower):
            return Timeframe.FIFTEEN_MIN

        # Check for hourly patterns
        # e.g., "1PM ET", "2AM ET" (single time, not range)
        hourly_pattern = r"\d{1,2}(am|pm)\s*et"
        if re.search(hourly_pattern, question_lower):
            return Timeframe.HOURLY

        # Check for daily patterns
        if "january" in question_lower or "on january" in question_lower:
            return Timeframe.DAILY

        return Timeframe.HOURLY  # Default

    async def _search_crypto_above_markets(self) -> list[UpDownMarket]:
        """Search for Crypto 'Above' markets (e.g., 'Bitcoin above $90,000?')."""
        if not self._client:
            return []

        markets: list[UpDownMarket] = []
        max_expiry = self.settings.spread_bot_max_time_to_expiry
        all_events = await self._fetch_all_open_events()

        for event in all_events:
            slug = event.get("slug", "").lower()
            title = event.get("title", "")

            # Match patterns like "bitcoin-above-on-january-9"
            if "-above" in slug and any(
                x in slug for x in ["bitcoin", "ethereum", "solana", "xrp", "btc", "eth"]
            ):
                # Parse each market in the event (different price levels)
                for market_data in event.get("markets", []):
                    market = self._parse_binary_market(
                        market_data, MarketType.ABOVE, title
                    )
                    if market and not market.is_expired and market.time_to_expiry <= max_expiry:
                        markets.append(market)

        # Sort by expiry and take nearest ones
        markets.sort(key=lambda m: m.end_time)
        return markets[:20]  # Limit to 20 above markets

    async def _search_crypto_price_range_markets(self) -> list[UpDownMarket]:
        """Search for Crypto 'Price Range' markets (e.g., 'Bitcoin between $88K-$90K?')."""
        if not self._client:
            return []

        markets: list[UpDownMarket] = []
        max_expiry = self.settings.spread_bot_max_time_to_expiry
        all_events = await self._fetch_all_open_events()

        for event in all_events:
            slug = event.get("slug", "").lower()
            title = event.get("title", "")

            # Match patterns like "bitcoin-price-on-january-9"
            if "-price-on" in slug and any(
                x in slug for x in ["bitcoin", "ethereum", "solana", "xrp", "btc", "eth"]
            ):
                # Parse each market in the event (different price ranges)
                for market_data in event.get("markets", []):
                    market = self._parse_binary_market(
                        market_data, MarketType.PRICE_RANGE, title
                    )
                    if market and not market.is_expired and market.time_to_expiry <= max_expiry:
                        markets.append(market)

        # Sort by expiry and take nearest ones
        markets.sort(key=lambda m: m.end_time)
        return markets[:20]  # Limit to 20 price range markets

    async def _search_sports_markets(self) -> list[UpDownMarket]:
        """Search for Sports game outcome markets (Team A vs Team B)."""
        if not self._client:
            return []

        markets: list[UpDownMarket] = []
        max_expiry = self.settings.spread_bot_max_time_to_expiry
        all_events = await self._fetch_all_open_events()

        for event in all_events:
            title = event.get("title", "")
            event_markets = event.get("markets", [])

            # Match patterns like "Lakers vs Celtics" - single binary market
            if (" vs " in title.lower() or " vs. " in title.lower()) and len(event_markets) == 1:
                market_data = event_markets[0]
                outcomes = market_data.get("outcomes", [])

                # Must be exactly 2 outcomes (Team A vs Team B)
                if isinstance(outcomes, str):
                    try:
                        outcomes = json.loads(outcomes)
                    except:
                        outcomes = []

                if len(outcomes) == 2:
                    market = self._parse_binary_market(
                        market_data, MarketType.SPORTS, title
                    )
                    if market and not market.is_expired and market.time_to_expiry <= max_expiry:
                        markets.append(market)

        # Sort by expiry and take nearest ones
        markets.sort(key=lambda m: m.end_time)
        return markets[:30]  # Limit to 30 sports markets

    def _parse_binary_market(
        self, data: dict, market_type: MarketType, event_title: str
    ) -> Optional[UpDownMarket]:
        """Parse a generic binary market from API response."""
        try:
            market_id = data.get("id", data.get("condition_id", ""))
            question = data.get("question", event_title)

            # Extract end date
            end_date_str = data.get("endDate", data.get("end_date_iso", ""))
            if end_date_str:
                end_time = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
            else:
                return None

            # Determine asset from question/title
            asset = self._extract_asset(question + " " + event_title)

            # Get token IDs
            yes_token_id = ""
            no_token_id = ""

            clob_token_ids = data.get("clobTokenIds", [])
            if isinstance(clob_token_ids, str):
                try:
                    clob_token_ids = json.loads(clob_token_ids)
                except:
                    clob_token_ids = []

            if isinstance(clob_token_ids, list) and len(clob_token_ids) >= 2:
                yes_token_id = str(clob_token_ids[0])
                no_token_id = str(clob_token_ids[1])

            if not yes_token_id or not no_token_id:
                return None

            # Get initial prices
            yes_price = Decimal("0.5")
            no_price = Decimal("0.5")

            outcome_prices = data.get("outcomePrices", "")
            if outcome_prices:
                if isinstance(outcome_prices, str):
                    try:
                        prices = json.loads(outcome_prices)
                        if len(prices) >= 2:
                            yes_price = Decimal(str(prices[0]))
                            no_price = Decimal(str(prices[1]))
                    except:
                        pass
                elif isinstance(outcome_prices, list) and len(outcome_prices) >= 2:
                    yes_price = Decimal(str(outcome_prices[0]))
                    no_price = Decimal(str(outcome_prices[1]))

            return UpDownMarket(
                id=market_id,
                name=question,
                asset=asset,
                timeframe=Timeframe.DAILY if market_type != MarketType.SPORTS else Timeframe.EVENT,
                end_time=end_time,
                yes_token_id=yes_token_id,
                no_token_id=no_token_id,
                condition_id=data.get("condition_id", market_id),
                market_type=market_type,
                yes_ask=yes_price,
                no_ask=no_price,
                volume=Decimal(str(data.get("volume", 0) or 0)),
                liquidity=Decimal(str(data.get("liquidity", 0) or 0)),
                fetched_at=datetime.now(timezone.utc),
            )

        except Exception as e:
            logger.debug(f"Error parsing binary market: {e}")
            return None

    def _extract_asset(self, text: str) -> Asset:
        """Extract asset type from text."""
        text_lower = text.lower()

        if "bitcoin" in text_lower or "btc" in text_lower:
            return Asset.BTC
        elif "ethereum" in text_lower or "eth" in text_lower:
            return Asset.ETH
        elif "solana" in text_lower or "sol" in text_lower:
            return Asset.SOL
        elif "xrp" in text_lower:
            return Asset.XRP
        else:
            return Asset.SPORTS if " vs " in text_lower else Asset.OTHER

    async def update_prices(self, market: UpDownMarket) -> bool:
        """Update market prices from order book."""
        if not self._client:
            return False

        try:
            # Fetch YES order book
            yes_book = await self._fetch_orderbook(market.yes_token_id)
            if yes_book:
                asks = yes_book.get("asks", [])
                if asks:
                    # CLOB API returns asks sorted HIGH to LOW, so best ask is the MINIMUM
                    best_ask = min(asks, key=lambda x: float(x.get("price", 999)))
                    market.yes_ask = Decimal(str(best_ask.get("price", 0)))
                bids = yes_book.get("bids", [])
                if bids:
                    # Best bid is the MAXIMUM
                    best_bid = max(bids, key=lambda x: float(x.get("price", 0)))
                    market.yes_bid = Decimal(str(best_bid.get("price", 0)))

            # Fetch NO order book
            no_book = await self._fetch_orderbook(market.no_token_id)
            if no_book:
                asks = no_book.get("asks", [])
                if asks:
                    # CLOB API returns asks sorted HIGH to LOW, so best ask is the MINIMUM
                    best_ask = min(asks, key=lambda x: float(x.get("price", 999)))
                    market.no_ask = Decimal(str(best_ask.get("price", 0)))
                bids = no_book.get("bids", [])
                if bids:
                    # Best bid is the MAXIMUM
                    best_bid = max(bids, key=lambda x: float(x.get("price", 0)))
                    market.no_bid = Decimal(str(best_bid.get("price", 0)))

            market.fetched_at = datetime.now(timezone.utc)
            return True

        except Exception as e:
            logger.error(f"Error updating prices for {market.name}: {e}")
            return False

    async def _fetch_orderbook(self, token_id: str) -> Optional[dict]:
        """Fetch order book for a token."""
        if not self._client:
            return None

        try:
            async with self._throttler:
                response = await self._client.get(
                    f"{CLOB_API_URL}/book",
                    params={"token_id": token_id},
                )

            if response.status_code == 200:
                return response.json()

        except Exception as e:
            logger.debug(f"Error fetching order book: {e}")

        return None

    def get_active_markets(self) -> list[UpDownMarket]:
        """Get all cached active (non-expired) markets."""
        return [m for m in self._markets.values() if not m.is_expired]

    def get_market(self, market_id: str) -> Optional[UpDownMarket]:
        """Get a specific market by ID."""
        return self._markets.get(market_id)
