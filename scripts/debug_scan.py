#!/usr/bin/env python3
"""Debug script to show detailed arbitrage scan information."""

import asyncio
import logging
from datetime import datetime
from decimal import Decimal

# Set up detailed logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)

async def main():
    from src.data_sources.polymarket import PolymarketClient
    from src.data_sources.kalshi import KalshiClient
    from src.data_sources.crypto.binance import BinanceClient
    from src.arbitrage.internal import InternalArbDetector
    from src.arbitrage.cross_platform import CrossPlatformDetector
    from src.arbitrage.hedging import HedgingDetector

    print("=" * 80)
    print("ARBITRAGE SCANNER - DETAILED DEBUG OUTPUT")
    print("=" * 80)
    print(f"Scan time: {datetime.utcnow().isoformat()}")
    print()

    # =========================================================================
    # 1. FETCH MARKETS
    # =========================================================================
    print("=" * 80)
    print("1. FETCHING MARKETS")
    print("=" * 80)

    poly_client = PolymarketClient()
    kalshi_client = KalshiClient()

    await poly_client.connect()
    await kalshi_client.connect()

    poly_markets = await poly_client.get_markets(limit=200)
    kalshi_markets = await kalshi_client.get_markets(limit=200)

    print(f"\nPolymarket: {len(poly_markets)} markets fetched")
    print(f"Kalshi: {len(kalshi_markets)} markets fetched")

    # Show sample markets
    print("\n--- Sample Polymarket Markets (first 10) ---")
    for m in poly_markets[:10]:
        yes = m.yes_price if m.yes_price else Decimal("0")
        no = m.no_price if m.no_price else Decimal("0")
        total = yes + no
        status = "[RESOLVED]" if m.resolved else "[ACTIVE]"
        print(f"  {status} {m.name[:55]}...")
        print(f"    YES: ${yes:.4f} | NO: ${no:.4f} | Total: ${total:.4f}")

    print("\n--- Sample Kalshi Markets (first 10) ---")
    for m in kalshi_markets[:10]:
        yes = m.yes_price if m.yes_price else Decimal("0")
        no = m.no_price if m.no_price else Decimal("0")
        total = yes + no
        status = "[RESOLVED]" if m.resolved else "[ACTIVE]"
        print(f"  {status} {m.name[:55]}...")
        print(f"    YES: ${yes:.4f} | NO: ${no:.4f} | Total: ${total:.4f}")
        # Show raw data for price debugging
        if m.raw:
            raw_yes = m.raw.get("yes_price", "N/A")
            raw_no = m.raw.get("no_price", "N/A")
            raw_bid = m.raw.get("yes_bid", "N/A")
            raw_ask = m.raw.get("yes_ask", "N/A")
            print(f"    Raw: yes_price={raw_yes}, no_price={raw_no}, yes_bid={raw_bid}, yes_ask={raw_ask}")

    # =========================================================================
    # 2. INTERNAL ARBITRAGE (Same platform, YES + NO < $1)
    # =========================================================================
    print("\n" + "=" * 80)
    print("2. INTERNAL ARBITRAGE DETECTION")
    print("=" * 80)

    internal_detector = InternalArbDetector()

    print("\n--- Polymarket Internal Arb ---")
    poly_internal = await internal_detector.scan(poly_markets)
    if poly_internal:
        for opp in poly_internal[:5]:
            print(f"\n  Market: {opp.market.name[:60]}...")
            print(f"  YES: ${opp.market.yes_price:.4f}, NO: ${opp.market.no_price:.4f}")
            print(f"  Total: ${opp.market.yes_price + opp.market.no_price:.4f}")
            print(f"  Profit: {opp.profit_percentage:.2%}")
    else:
        print("  No internal arbitrage found")

    print("\n--- Kalshi Internal Arb ---")
    kalshi_internal = await internal_detector.scan(kalshi_markets)
    if kalshi_internal:
        for opp in kalshi_internal[:5]:
            print(f"\n  Market: {opp.market.name[:60]}...")
            print(f"  YES: ${opp.market.yes_price:.4f}, NO: ${opp.market.no_price:.4f}")
            print(f"  Total: ${opp.market.yes_price + opp.market.no_price:.4f}")
            print(f"  Profit: {opp.profit_percentage:.2%}")
    else:
        print("  No internal arbitrage found")

    # =========================================================================
    # 3. CROSS-PLATFORM ARBITRAGE
    # =========================================================================
    print("\n" + "=" * 80)
    print("3. CROSS-PLATFORM ARBITRAGE DETECTION")
    print("=" * 80)

    from src.models.market import Platform
    cross_detector = CrossPlatformDetector()

    markets_by_platform = {
        Platform.POLYMARKET: poly_markets,
        Platform.KALSHI: kalshi_markets,
    }

    cross_opps = await cross_detector.scan(markets_by_platform)
    if cross_opps:
        for opp in cross_opps[:5]:
            print(f"\n  Event Match Found!")
            print(f"  Platform A: {opp.platform_a.value} - {opp.markets[0].name[:50]}...")
            print(f"  Platform B: {opp.platform_b.value} - {opp.markets[1].name[:50]}...")
            print(f"  Buy YES @ ${opp.price_a:.4f} on {opp.platform_a.value}")
            print(f"  Buy NO @ ${opp.price_b:.4f} on {opp.platform_b.value}")
            print(f"  Total: ${opp.price_a + opp.price_b:.4f}")
            print(f"  Profit: {opp.profit_percentage:.2%}")
    else:
        print("  No cross-platform arbitrage found")

    # =========================================================================
    # 4. HEDGING ARBITRAGE (Prediction vs Options)
    # =========================================================================
    print("\n" + "=" * 80)
    print("4. HEDGING ARBITRAGE DETECTION (Prediction Markets vs Binance Options)")
    print("=" * 80)

    # Find crypto-related markets
    all_markets = poly_markets + kalshi_markets

    print("\n--- Crypto Price Prediction Markets Found ---")
    crypto_keywords = ["btc", "bitcoin", "eth", "ethereum"]
    price_keywords = ["price", "above", "below", "reach", "hit", "$", "100k", "150k", "200k"]

    crypto_markets = []
    for m in all_markets:
        name_lower = m.name.lower()
        has_crypto = any(kw in name_lower for kw in crypto_keywords)
        has_price = any(kw in name_lower for kw in price_keywords)
        if has_crypto and has_price:
            crypto_markets.append(m)

    print(f"\nTotal crypto price markets: {len(crypto_markets)}")

    for m in crypto_markets[:20]:
        status = "RESOLVED" if m.resolved else "ACTIVE"
        end_str = m.end_date.strftime("%Y-%m-%d") if m.end_date else "No end date"
        expired = ""
        if m.end_date:
            end = m.end_date.replace(tzinfo=None) if m.end_date.tzinfo else m.end_date
            if end < datetime.utcnow():
                expired = " [EXPIRED]"

        print(f"\n  [{status}]{expired} {m.platform.value}")
        print(f"  Name: {m.name[:70]}...")
        print(f"  End: {end_str}")
        yes_price = m.yes_price if m.yes_price else Decimal("0")
        print(f"  YES: ${yes_price:.4f}")

    # Filter to active only
    active_crypto = [
        m for m in crypto_markets
        if not m.resolved and m.end_date and m.end_date.replace(tzinfo=None) > datetime.utcnow()
    ]

    print(f"\n--- Active (non-resolved, future end date): {len(active_crypto)} ---")

    if active_crypto:
        # Try to get Binance options data
        binance = BinanceClient()
        await binance.connect()

        print("\n--- Binance Options Available ---")
        btc_options = await binance.get_options("BTC")
        eth_options = await binance.get_options("ETH")

        print(f"  BTC options: {len(btc_options)}")
        print(f"  ETH options: {len(eth_options)}")

        # Show available strikes
        if btc_options:
            strikes = sorted(set(int(float(o.get("strikePrice", 0))) for o in btc_options))
            print(f"  BTC strikes: {strikes[:10]}... (showing first 10)")

        if eth_options:
            strikes = sorted(set(int(float(o.get("strikePrice", 0))) for o in eth_options))
            print(f"  ETH strikes: {strikes[:10]}... (showing first 10)")

        # Run hedging detector
        hedging_detector = HedgingDetector()
        hedging_opps = await hedging_detector.scan(active_crypto)

        if hedging_opps:
            print("\n--- Hedging Opportunities Found ---")
            for opp in hedging_opps[:5]:
                print(f"\n  Market: {opp.prediction_market.name[:60]}...")
                print(f"  Prediction Market Price: {opp.prediction_probability:.2%}")
                print(f"  Options Implied Prob: {opp.implied_probability:.2%}")
                print(f"  Discrepancy: {opp.probability_discrepancy:.2%}")
        else:
            print("\n  No hedging opportunities found")

        await binance.disconnect()
    else:
        print("  No active crypto markets to check against options")

    # =========================================================================
    # 5. SUMMARY
    # =========================================================================
    print("\n" + "=" * 80)
    print("5. SUMMARY")
    print("=" * 80)

    total_internal = len(poly_internal) + len(kalshi_internal)
    total_cross = len(cross_opps)
    total_hedging = len(active_crypto)  # Markets checked

    print(f"""
  Markets Fetched:
    - Polymarket: {len(poly_markets)}
    - Kalshi: {len(kalshi_markets)}

  Crypto Price Markets:
    - Total found: {len(crypto_markets)}
    - Active (can be hedged): {len(active_crypto)}

  Opportunities Found:
    - Internal Arbitrage: {total_internal}
    - Cross-Platform Arbitrage: {total_cross}
    - Hedging Arbitrage: 0
""")

    await poly_client.disconnect()
    await kalshi_client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
