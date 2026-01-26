"""Main arbitrage detection engine that orchestrates all detectors."""

import asyncio
import logging
from datetime import datetime

from pylo.arbitrage.cross_platform import CrossPlatformDetector
from pylo.arbitrage.hedging import HedgingDetector
from pylo.arbitrage.internal import InternalArbDetector
from pylo.config.settings import get_settings
from pylo.data_sources.kalshi import KalshiClient
from pylo.data_sources.polymarket import PolymarketClient
from pylo.models.market import Market, Platform
from pylo.models.opportunity import ArbitrageOpportunity

logger = logging.getLogger(__name__)


class ArbitrageEngine:
    """
    Main engine that orchestrates all arbitrage detection.

    Responsibilities:
    - Fetch data from all sources
    - Run all detectors
    - Deduplicate and rank opportunities
    - Trigger alerts
    """

    def __init__(self) -> None:
        """Initialize the arbitrage engine."""
        self.settings = get_settings()
        self.logger = logging.getLogger(__name__)

        # Data sources
        self.polymarket = PolymarketClient()
        self.kalshi = KalshiClient()

        # Detectors
        self.internal_detector = InternalArbDetector()
        self.cross_platform_detector = CrossPlatformDetector()
        self.hedging_detector = HedgingDetector()

        # State
        self._running = False
        self._last_scan: datetime | None = None
        self._opportunities: list[ArbitrageOpportunity] = []

    async def start(self) -> None:
        """Start the arbitrage engine."""
        self.logger.info("Starting arbitrage engine...")

        # Connect to data sources
        await self.polymarket.connect()
        await self.kalshi.connect()

        self._running = True
        self.logger.info("Arbitrage engine started")

    async def stop(self) -> None:
        """Stop the arbitrage engine."""
        self.logger.info("Stopping arbitrage engine...")
        self._running = False

        # Disconnect from data sources
        await self.polymarket.disconnect()
        await self.kalshi.disconnect()

        self.logger.info("Arbitrage engine stopped")

    async def scan_once(self) -> list[ArbitrageOpportunity]:
        """
        Run a single scan for arbitrage opportunities.

        Returns:
            List of all detected opportunities
        """
        self.logger.info("Starting arbitrage scan...")
        start_time = datetime.utcnow()

        all_opportunities: list[ArbitrageOpportunity] = []

        # Fetch markets from all sources in parallel
        markets_by_platform = await self._fetch_all_markets()

        # Run all detectors in parallel
        results = await asyncio.gather(
            self._run_internal_detection(markets_by_platform),
            self._run_cross_platform_detection(markets_by_platform),
            self._run_hedging_detection(markets_by_platform),
            return_exceptions=True,
        )

        # Collect results
        for result in results:
            if isinstance(result, list):
                all_opportunities.extend(result)
            elif isinstance(result, Exception):
                self.logger.error(f"Detector error: {result}")

        # Deduplicate and rank
        all_opportunities = self._deduplicate(all_opportunities)
        all_opportunities.sort(key=lambda x: x.profit_percentage, reverse=True)

        # Update state
        self._last_scan = datetime.utcnow()
        self._opportunities = all_opportunities

        # Log summary
        elapsed = (datetime.utcnow() - start_time).total_seconds()
        self.logger.info(
            f"Scan complete in {elapsed:.2f}s - Found {len(all_opportunities)} opportunities"
        )

        return all_opportunities

    async def run_continuous(self) -> None:
        """
        Run continuous scanning loop.

        Scans at the configured interval until stopped.
        """
        self.logger.info(f"Starting continuous scan (interval: {self.settings.scan_interval}s)")

        while self._running:
            try:
                opportunities = await self.scan_once()

                # Log top opportunities
                if opportunities:
                    self.logger.info("Top opportunities:")
                    for opp in opportunities[:5]:
                        self.logger.info(f"  {opp}")

                # Wait for next scan
                await asyncio.sleep(self.settings.scan_interval)

            except asyncio.CancelledError:
                self.logger.info("Scan loop cancelled")
                break
            except Exception as e:
                self.logger.error(f"Scan error: {e}")
                await asyncio.sleep(10)  # Brief pause on error

    async def _fetch_all_markets(self) -> dict[Platform, list[Market]]:
        """Fetch markets from all connected data sources."""
        results: dict[Platform, list[Market]] = {}

        # Fetch in parallel
        tasks = [
            self._fetch_polymarket_markets(),
            self._fetch_kalshi_markets(),
        ]

        fetched = await asyncio.gather(*tasks, return_exceptions=True)

        # Polymarket
        if isinstance(fetched[0], list):
            results[Platform.POLYMARKET] = fetched[0]
            self.logger.info(f"Fetched {len(fetched[0])} Polymarket markets")
        elif isinstance(fetched[0], Exception):
            self.logger.error(f"Polymarket fetch error: {fetched[0]}")
            results[Platform.POLYMARKET] = []

        # Kalshi
        if isinstance(fetched[1], list):
            results[Platform.KALSHI] = fetched[1]
            self.logger.info(f"Fetched {len(fetched[1])} Kalshi markets")
        elif isinstance(fetched[1], Exception):
            self.logger.error(f"Kalshi fetch error: {fetched[1]}")
            results[Platform.KALSHI] = []

        return results

    async def _fetch_polymarket_markets(self) -> list[Market]:
        """Fetch crypto/financial markets from Polymarket including 15m series."""
        try:
            markets: list[Market] = []

            # Fetch crypto markets (long-dated price targets)
            crypto = await self.polymarket.get_crypto_markets()
            markets.extend(crypto)

            # Fetch 15-minute up/down markets from specific series
            from pylo.data_sources.polymarket import CRYPTO_SERIES_15M

            for asset, series_id in CRYPTO_SERIES_15M.items():
                try:
                    events = await self.polymarket.get_closed_events_by_series(
                        series_id=series_id, limit=10, offset=0
                    )
                    # Actually we need open events, not closed - let me fetch differently
                except Exception as e:
                    self.logger.debug(f"Error fetching {asset} 15m series: {e}")

            # Fetch open 15m markets via events API
            import httpx

            async with httpx.AsyncClient() as client:
                for asset, series_id in CRYPTO_SERIES_15M.items():
                    try:
                        resp = await client.get(
                            "https://gamma-api.polymarket.com/events",
                            params={"series_id": series_id, "closed": "false", "limit": 5},
                        )
                        if resp.status_code == 200:
                            events = resp.json()
                            for event in events:
                                for m in event.get("markets", []):
                                    market = self.polymarket._parse_market(m)
                                    if market:
                                        markets.append(market)
                    except Exception as e:
                        self.logger.debug(f"Error fetching {asset} 15m: {e}")

            return markets
        except Exception as e:
            self.logger.error(f"Error fetching Polymarket markets: {e}")
            return []

    async def _fetch_kalshi_markets(self) -> list[Market]:
        """Fetch markets from Kalshi with cross-platform arbitrage potential."""
        try:
            markets: list[Market] = []

            # Fetch 15-minute crypto markets from specific series
            crypto_15m_series = ["KXBTC15M", "KXETH15M", "KXSOL15M"]

            import httpx

            async with httpx.AsyncClient() as client:
                for series in crypto_15m_series:
                    try:
                        resp = await client.get(
                            "https://api.elections.kalshi.com/trade-api/v2/markets",
                            params={"series_ticker": series, "status": "open", "limit": 10},
                        )
                        if resp.status_code == 200:
                            data = resp.json()
                            for m in data.get("markets", []):
                                market = self.kalshi._parse_market(m)
                                if market:
                                    markets.append(market)
                            self.logger.info(f"Fetched {len(data.get('markets', []))} {series} markets")
                    except Exception as e:
                        self.logger.debug(f"Error fetching {series}: {e}")

            # Also fetch general markets for other arb types
            general_markets = await self.kalshi.get_markets(limit=200, status="open")

            # Filter to categories with cross-platform potential
            relevant_categories = {
                "crypto",
                "economics",
                "financial",
                "politics",
                "elections",
                "fed",
                "inflation",
                "cpi",
                "gdp",
            }

            for m in general_markets:
                category = (m.category or "").lower()
                name = m.name.lower()

                # Check if market matches relevant categories
                is_relevant = any(cat in category or cat in name for cat in relevant_categories)

                # Also include markets with objective resolution (price targets)
                has_price_target = any(
                    kw in name for kw in ["above", "below", "over", "under", "$", "price", "rate"]
                )

                if is_relevant or has_price_target:
                    # Avoid duplicates
                    if m.id not in [mk.id for mk in markets]:
                        markets.append(m)

            self.logger.info(f"Kalshi: {len(markets)} total markets for cross-platform")
            return markets

        except Exception as e:
            self.logger.error(f"Error fetching Kalshi markets: {e}")
            return []

    async def _run_internal_detection(
        self,
        markets_by_platform: dict[Platform, list[Market]],
    ) -> list[ArbitrageOpportunity]:
        """Run internal arbitrage detection on all markets."""
        all_markets = []
        for markets in markets_by_platform.values():
            all_markets.extend(markets)

        return await self.internal_detector.scan(all_markets)

    async def _run_cross_platform_detection(
        self,
        markets_by_platform: dict[Platform, list[Market]],
    ) -> list[ArbitrageOpportunity]:
        """Run cross-platform arbitrage detection."""
        return await self.cross_platform_detector.scan(markets_by_platform)

    async def _run_hedging_detection(
        self,
        markets_by_platform: dict[Platform, list[Market]],
    ) -> list[ArbitrageOpportunity]:
        """Run hedging arbitrage detection."""
        # Fetch crypto-specific markets from Polymarket events API
        crypto_markets = await self.polymarket.get_crypto_markets()
        self.logger.info(f"Fetched {len(crypto_markets)} crypto markets for hedging")
        return await self.hedging_detector.scan(crypto_markets)

    def _deduplicate(
        self,
        opportunities: list[ArbitrageOpportunity],
    ) -> list[ArbitrageOpportunity]:
        """Remove duplicate opportunities."""
        seen: set[str] = set()
        unique: list[ArbitrageOpportunity] = []

        for opp in opportunities:
            if opp.id not in seen:
                seen.add(opp.id)
                unique.append(opp)

        return unique

    @property
    def opportunities(self) -> list[ArbitrageOpportunity]:
        """Get the most recent opportunities."""
        return self._opportunities

    @property
    def last_scan(self) -> datetime | None:
        """Get timestamp of last scan."""
        return self._last_scan

    async def __aenter__(self) -> "ArbitrageEngine":
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        """Async context manager exit."""
        await self.stop()
