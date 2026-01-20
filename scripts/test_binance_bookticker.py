#!/usr/bin/env python3
"""Test Binance WebSocket streams update rates.

Compares:
- bookTicker: Best bid/ask updates
- aggTrade: Aggregate trades
- kline_1m: 1-minute candlesticks (current)

Usage:
    python scripts/test_binance_bookticker.py
    python scripts/test_binance_bookticker.py --stream aggTrade
    python scripts/test_binance_bookticker.py --stream kline_1m
"""

import asyncio
import json
import time
from collections import deque
from datetime import datetime

import websockets

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
SYMBOL = "btcusdt"


async def test_bookticker():
    """Test bookTicker stream - updates on every best bid/ask change."""
    url = f"{BINANCE_WS_URL}/{SYMBOL}@bookTicker"
    print(f"\n{'='*60}")
    print(f"Testing: bookTicker stream")
    print(f"URL: {url}")
    print(f"{'='*60}\n")

    update_times = deque(maxlen=100)
    last_time = None
    count = 0

    async with websockets.connect(url) as ws:
        start = time.time()
        while time.time() - start < 30:  # Run for 30 seconds
            msg = await ws.recv()
            now = time.time()
            data = json.loads(msg)

            if last_time:
                delta_ms = (now - last_time) * 1000
                update_times.append(delta_ms)

            last_time = now
            count += 1

            # Print first 10 and then every 50th
            if count <= 10 or count % 50 == 0:
                print(
                    f"[{count:4d}] bid: ${float(data['b']):,.2f} | ask: ${float(data['a']):,.2f} | "
                    f"delta: {update_times[-1] if update_times else 0:.1f}ms"
                )

    # Stats
    if update_times:
        avg = sum(update_times) / len(update_times)
        min_delta = min(update_times)
        max_delta = max(update_times)
        print(f"\n{'='*60}")
        print(f"STATS (bookTicker):")
        print(f"  Total updates: {count}")
        print(f"  Updates/sec:   {count / 30:.1f}")
        print(f"  Avg delta:     {avg:.1f}ms")
        print(f"  Min delta:     {min_delta:.1f}ms")
        print(f"  Max delta:     {max_delta:.1f}ms")
        print(f"{'='*60}\n")


async def test_aggtrade():
    """Test aggTrade stream - aggregate trades batched ~100ms."""
    url = f"{BINANCE_WS_URL}/{SYMBOL}@aggTrade"
    print(f"\n{'='*60}")
    print(f"Testing: aggTrade stream")
    print(f"URL: {url}")
    print(f"{'='*60}\n")

    update_times = deque(maxlen=100)
    last_time = None
    count = 0

    async with websockets.connect(url) as ws:
        start = time.time()
        while time.time() - start < 30:
            msg = await ws.recv()
            now = time.time()
            data = json.loads(msg)

            if last_time:
                delta_ms = (now - last_time) * 1000
                update_times.append(delta_ms)

            last_time = now
            count += 1

            if count <= 10 or count % 100 == 0:
                print(
                    f"[{count:4d}] price: ${float(data['p']):,.2f} | qty: {float(data['q']):.4f} | "
                    f"delta: {update_times[-1] if update_times else 0:.1f}ms"
                )

    if update_times:
        avg = sum(update_times) / len(update_times)
        min_delta = min(update_times)
        max_delta = max(update_times)
        print(f"\n{'='*60}")
        print(f"STATS (aggTrade):")
        print(f"  Total updates: {count}")
        print(f"  Updates/sec:   {count / 30:.1f}")
        print(f"  Avg delta:     {avg:.1f}ms")
        print(f"  Min delta:     {min_delta:.1f}ms")
        print(f"  Max delta:     {max_delta:.1f}ms")
        print(f"{'='*60}\n")


async def test_kline():
    """Test kline_1m stream - 250ms updates."""
    url = f"{BINANCE_WS_URL}/{SYMBOL}@kline_1m"
    print(f"\n{'='*60}")
    print(f"Testing: kline_1m stream (current implementation)")
    print(f"URL: {url}")
    print(f"{'='*60}\n")

    update_times = deque(maxlen=100)
    last_time = None
    count = 0

    async with websockets.connect(url) as ws:
        start = time.time()
        while time.time() - start < 30:
            msg = await ws.recv()
            now = time.time()
            data = json.loads(msg)
            kline = data["k"]

            if last_time:
                delta_ms = (now - last_time) * 1000
                update_times.append(delta_ms)

            last_time = now
            count += 1

            if count <= 10 or count % 20 == 0:
                print(
                    f"[{count:4d}] close: ${float(kline['c']):,.2f} | "
                    f"closed: {kline['x']} | delta: {update_times[-1] if update_times else 0:.1f}ms"
                )

    if update_times:
        avg = sum(update_times) / len(update_times)
        min_delta = min(update_times)
        max_delta = max(update_times)
        print(f"\n{'='*60}")
        print(f"STATS (kline_1m):")
        print(f"  Total updates: {count}")
        print(f"  Updates/sec:   {count / 30:.1f}")
        print(f"  Avg delta:     {avg:.1f}ms")
        print(f"  Min delta:     {min_delta:.1f}ms")
        print(f"  Max delta:     {max_delta:.1f}ms")
        print(f"{'='*60}\n")


async def test_all():
    """Run all stream tests sequentially."""
    print("\n" + "=" * 60)
    print("BINANCE WEBSOCKET STREAM COMPARISON")
    print(f"Symbol: {SYMBOL.upper()}")
    print(f"Test duration: 30 seconds each")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    await test_bookticker()
    await test_aggtrade()
    await test_kline()

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print("  bookTicker: Fastest - updates on every bid/ask change")
    print("  aggTrade:   Fast - batched trades ~100ms")
    print("  kline_1m:   Slow - 250ms interval updates")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Test Binance WebSocket update rates")
    parser.add_argument(
        "--stream",
        choices=["bookTicker", "aggTrade", "kline_1m", "all"],
        default="all",
        help="Stream to test (default: all)",
    )
    args = parser.parse_args()

    if args.stream == "bookTicker":
        asyncio.run(test_bookticker())
    elif args.stream == "aggTrade":
        asyncio.run(test_aggtrade())
    elif args.stream == "kline_1m":
        asyncio.run(test_kline())
    else:
        asyncio.run(test_all())
