"""Binance klines (candlestick) data client for price history."""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum

import httpx
from asyncio_throttle import Throttler

from pylo.config.settings import get_settings

logger = logging.getLogger(__name__)

# Binance API endpoint
SPOT_API = "https://api.binance.com/api/v3"

# Rate limits (requests per second)
BINANCE_RATE_LIMIT = 15


class KlineInterval(str, Enum):
    """Binance kline intervals."""

    ONE_MIN = "1m"
    THREE_MIN = "3m"
    FIVE_MIN = "5m"
    FIFTEEN_MIN = "15m"
    THIRTY_MIN = "30m"
    ONE_HOUR = "1h"
    FOUR_HOUR = "4h"
    ONE_DAY = "1d"


@dataclass
class Candle:
    """A single OHLCV candlestick."""

    open_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    close_time: datetime
    quote_volume: Decimal
    trades: int

    @property
    def return_pct(self) -> Decimal:
        """Calculate percentage return for this candle."""
        if self.open == 0:
            return Decimal("0")
        return (self.close - self.open) / self.open

    @property
    def is_bullish(self) -> bool:
        """Check if candle closed higher than opened."""
        return self.close > self.open

    @property
    def body_size(self) -> Decimal:
        """Absolute size of candle body."""
        return abs(self.close - self.open)

    @property
    def range_size(self) -> Decimal:
        """Full range of candle (high - low)."""
        return self.high - self.low


class BinanceKlinesClient:
    """Client for fetching Binance kline (candlestick) data."""

    name = "binance_klines"

    def __init__(self) -> None:
        """Initialize the Binance klines client."""
        self.settings = get_settings()
        self._client: httpx.AsyncClient | None = None
        self._connected = False
        self.logger = logging.getLogger(f"{__name__}.{self.name}")
        self._throttler = Throttler(rate_limit=BINANCE_RATE_LIMIT, period=1.0)

    async def connect(self) -> None:
        """Initialize HTTP client."""
        headers = {"Accept": "application/json"}

        # Add API key if available (for higher rate limits)
        if self.settings.has_binance_credentials:
            headers["X-MBX-APIKEY"] = self.settings.binance_api_key.get_secret_value()

        self._client = httpx.AsyncClient(
            timeout=30.0,
            headers=headers,
        )
        self._connected = True
        self.logger.info("Connected to Binance Klines API")

    async def disconnect(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
        self._connected = False
        self.logger.info("Disconnected from Binance Klines API")

    async def get_klines(
        self,
        symbol: str = "BTCUSDT",
        interval: KlineInterval = KlineInterval.FIFTEEN_MIN,
        limit: int = 100,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[Candle]:
        """
        Fetch kline (candlestick) data from Binance.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT", "ETHUSDT", "SOLUSDT")
            interval: Kline interval (e.g., KlineInterval.FIFTEEN_MIN)
            limit: Number of candles to fetch (max 1000)
            start_time: Start time for historical data
            end_time: End time for historical data

        Returns:
            List of Candle objects, oldest first
        """
        if not self._client:
            raise RuntimeError("Client not connected")

        params: dict[str, str | int] = {
            "symbol": symbol.upper(),
            "interval": interval.value,
            "limit": min(limit, 1000),  # Binance max is 1000
        }

        if start_time:
            params["startTime"] = int(start_time.timestamp() * 1000)
        if end_time:
            params["endTime"] = int(end_time.timestamp() * 1000)

        try:
            async with self._throttler:
                response = await self._client.get(
                    f"{SPOT_API}/klines",
                    params=params,
                )
                response.raise_for_status()
                data = response.json()

            candles = []
            for kline in data:
                # Binance kline format:
                # [0] Open time, [1] Open, [2] High, [3] Low, [4] Close,
                # [5] Volume, [6] Close time, [7] Quote volume,
                # [8] Number of trades, [9] Taker buy base volume,
                # [10] Taker buy quote volume, [11] Ignore
                candle = Candle(
                    open_time=datetime.fromtimestamp(kline[0] / 1000, tz=UTC),
                    open=Decimal(str(kline[1])),
                    high=Decimal(str(kline[2])),
                    low=Decimal(str(kline[3])),
                    close=Decimal(str(kline[4])),
                    volume=Decimal(str(kline[5])),
                    close_time=datetime.fromtimestamp(kline[6] / 1000, tz=UTC),
                    quote_volume=Decimal(str(kline[7])),
                    trades=int(kline[8]),
                )
                candles.append(candle)

            self.logger.debug(
                f"Fetched {len(candles)} {interval.value} candles for {symbol}"
            )
            return candles

        except httpx.HTTPError as e:
            self.logger.error(f"Failed to fetch klines: {e}")
            return []

    async def get_recent_candles(
        self,
        asset: str,
        interval_minutes: int = 15,
        count: int = 10,
    ) -> list[Candle]:
        """
        Convenience method to fetch recent candles for an asset.

        Args:
            asset: Asset name (e.g., "BTC", "ETH", "SOL")
            interval_minutes: Candle interval in minutes (1, 3, 5, 15, 30, 60, 240)
            count: Number of candles to fetch

        Returns:
            List of recent Candle objects, oldest first
        """
        # Map interval minutes to KlineInterval
        interval_map = {
            1: KlineInterval.ONE_MIN,
            3: KlineInterval.THREE_MIN,
            5: KlineInterval.FIVE_MIN,
            15: KlineInterval.FIFTEEN_MIN,
            30: KlineInterval.THIRTY_MIN,
            60: KlineInterval.ONE_HOUR,
            240: KlineInterval.FOUR_HOUR,
            1440: KlineInterval.ONE_DAY,
        }

        interval = interval_map.get(interval_minutes)
        if not interval:
            self.logger.warning(
                f"Unsupported interval {interval_minutes}m, using 15m"
            )
            interval = KlineInterval.FIFTEEN_MIN

        # Build symbol (e.g., "BTCUSDT")
        symbol = f"{asset.upper()}USDT"

        return await self.get_klines(
            symbol=symbol,
            interval=interval,
            limit=count,
        )

    async def get_current_price(self, asset: str) -> Decimal | None:
        """
        Get the current price for an asset.

        Args:
            asset: Asset name (e.g., "BTC", "ETH", "SOL")

        Returns:
            Current price or None on error
        """
        if not self._client:
            raise RuntimeError("Client not connected")

        symbol = f"{asset.upper()}USDT"

        try:
            async with self._throttler:
                response = await self._client.get(
                    f"{SPOT_API}/ticker/price",
                    params={"symbol": symbol},
                )
                response.raise_for_status()
                data = response.json()

            return Decimal(data["price"])

        except httpx.HTTPError as e:
            self.logger.error(f"Failed to fetch price for {symbol}: {e}")
            return None

    async def __aenter__(self) -> "BinanceKlinesClient":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Async context manager exit."""
        await self.disconnect()
