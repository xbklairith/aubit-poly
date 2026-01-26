"""
Crypto Price Comparison Tool

Compares BTC/USD prices from 3 WebSocket sources in real-time:
1. Binance Direct (bookTicker)
2. Polymarket RTDS Binance
3. Polymarket RTDS Chainlink

Measures price differences and latency/lag between sources.
"""

import argparse
import asyncio
import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from websockets.asyncio.client import connect as ws_connect  # type: ignore[import-not-found]
from websockets.exceptions import ConnectionClosed


@dataclass
class PricePoint:
    """Represents a single price observation from a source."""

    source: str  # "binance", "poly_binance", "poly_chainlink"
    price: float
    source_ts: int  # Timestamp from source (ms)
    receive_ts: int  # Local receive time (ms)
    latency_ms: int  # receive_ts - source_ts

    @property
    def receive_datetime(self) -> datetime:
        return datetime.fromtimestamp(self.receive_ts / 1000)


@dataclass
class CollectionStats:
    """Statistics for a collection run."""

    source: str
    total_updates: int = 0
    total_latency_ms: int = 0
    total_price: float = 0.0
    min_latency_ms: int = 999999
    max_latency_ms: int = 0
    min_price: float = float("inf")
    max_price: float = 0.0

    def update(self, point: PricePoint) -> None:
        self.total_updates += 1
        self.total_latency_ms += point.latency_ms
        self.total_price += point.price
        self.min_latency_ms = min(self.min_latency_ms, point.latency_ms)
        self.max_latency_ms = max(self.max_latency_ms, point.latency_ms)
        self.min_price = min(self.min_price, point.price)
        self.max_price = max(self.max_price, point.price)

    @property
    def avg_latency_ms(self) -> float:
        return self.total_latency_ms / self.total_updates if self.total_updates > 0 else 0

    @property
    def avg_price(self) -> float:
        return self.total_price / self.total_updates if self.total_updates > 0 else 0


def get_current_time_ms() -> int:
    """Get current time in milliseconds."""
    return int(time.time() * 1000)


async def binance_ws(queue: asyncio.Queue[PricePoint], stop_event: asyncio.Event) -> None:
    """
    Connect to Binance bookTicker stream for BTCUSDT.

    bookTicker provides the best bid/ask prices with lowest latency.
    Message format:
    {
        "u": 400900217,
        "s": "BTCUSDT",
        "b": "87500.00",    # best bid
        "B": "1.5",         # bid qty
        "a": "87500.50",    # best ask
        "A": "2.0",         # ask qty
        "E": 1769018000000  # event time (ms)
    }
    """
    url = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker"

    while not stop_event.is_set():
        try:
            async with ws_connect(url) as ws:
                print("[Binance] Connected to bookTicker stream")

                while not stop_event.is_set():
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        receive_ts = get_current_time_ms()
                        data = json.loads(msg)

                        # Calculate mid price from best bid/ask
                        bid = float(data["b"])
                        ask = float(data["a"])
                        mid_price = (bid + ask) / 2

                        # Get source timestamp (event time)
                        source_ts = data.get("E", receive_ts)

                        point = PricePoint(
                            source="binance",
                            price=mid_price,
                            source_ts=source_ts,
                            receive_ts=receive_ts,
                            latency_ms=receive_ts - source_ts,
                        )
                        await queue.put(point)

                    except TimeoutError:
                        continue
                    except ConnectionClosed:
                        print("[Binance] Connection closed, reconnecting...")
                        break

        except Exception as e:
            if not stop_event.is_set():
                print(f"[Binance] Error: {e}, reconnecting in 1s...")
                await asyncio.sleep(1)


async def polymarket_binance_ws(
    queue: asyncio.Queue[PricePoint], stop_event: asyncio.Event
) -> None:
    """
    Connect to Polymarket RTDS for Binance crypto prices.

    Subscription format:
    {"action":"subscribe","subscriptions":[{"topic":"crypto_prices","type":"update","filters":"btcusdt"}]}

    Message format:
    {
        "topic": "crypto_prices",
        "type": "update",
        "timestamp": 1769018000000,
        "payload": {
            "symbol": "btcusdt",
            "timestamp": 1769018000000,
            "value": 87500.25
        }
    }
    """
    url = "wss://ws-live-data.polymarket.com"

    subscribe_msg = {
        "action": "subscribe",
        "subscriptions": [
            {"topic": "crypto_prices", "type": "update", "filters": '{"symbol":"btcusdt"}'}
        ],
    }

    while not stop_event.is_set():
        try:
            async with ws_connect(url) as ws:
                print("[Poly/Binance] Connected to RTDS")

                # Subscribe to crypto_prices topic
                await ws.send(json.dumps(subscribe_msg))
                print("[Poly/Binance] Subscribed to crypto_prices (btcusdt)")

                while not stop_event.is_set():
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        receive_ts = get_current_time_ms()

                        # Skip empty messages (subscription confirmations)
                        if not msg or not msg.strip():
                            continue

                        data = json.loads(msg)

                        # Skip non-update messages (e.g., initial data payloads)
                        if data.get("topic") != "crypto_prices":
                            continue
                        if data.get("type") != "update":
                            continue

                        payload = data.get("payload", {})
                        if payload.get("symbol", "").lower() != "btcusdt":
                            continue

                        price = float(payload.get("value", 0))
                        source_ts = payload.get("timestamp", receive_ts)

                        point = PricePoint(
                            source="poly_binance",
                            price=price,
                            source_ts=source_ts,
                            receive_ts=receive_ts,
                            latency_ms=receive_ts - source_ts,
                        )
                        await queue.put(point)

                    except TimeoutError:
                        continue
                    except ConnectionClosed:
                        print("[Poly/Binance] Connection closed, reconnecting...")
                        break

        except Exception as e:
            if not stop_event.is_set():
                print(f"[Poly/Binance] Error: {e}, reconnecting in 1s...")
                await asyncio.sleep(1)


async def polymarket_chainlink_ws(
    queue: asyncio.Queue[PricePoint], stop_event: asyncio.Event
) -> None:
    """
    Connect to Polymarket RTDS for Chainlink crypto prices.

    Subscription format:
    {"action":"subscribe","subscriptions":[{"topic":"crypto_prices_chainlink","type":"*","filters":"{\"symbol\":\"btc/usd\"}"}]}

    Message format:
    {
        "topic": "crypto_prices_chainlink",
        "type": "update",
        "timestamp": 1769018000000,
        "payload": {
            "symbol": "btc/usd",
            "timestamp": 1769018000000,
            "value": 87500.10,
            "full_accuracy_value": "87500100000000000000000"
        }
    }
    """
    url = "wss://ws-live-data.polymarket.com"

    subscribe_msg = {
        "action": "subscribe",
        "subscriptions": [
            {
                "topic": "crypto_prices_chainlink",
                "type": "*",
                "filters": '{"symbol":"btc/usd"}',
            }
        ],
    }

    while not stop_event.is_set():
        try:
            async with ws_connect(url) as ws:
                print("[Poly/Chainlink] Connected to RTDS")

                # Subscribe to crypto_prices_chainlink topic
                await ws.send(json.dumps(subscribe_msg))
                print("[Poly/Chainlink] Subscribed to crypto_prices_chainlink (btc/usd)")

                while not stop_event.is_set():
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        receive_ts = get_current_time_ms()

                        # Skip empty messages (subscription confirmations)
                        if not msg or not msg.strip():
                            continue

                        data = json.loads(msg)

                        # Skip non-update messages (e.g., initial data payloads)
                        if data.get("topic") != "crypto_prices_chainlink":
                            continue

                        payload = data.get("payload", {})
                        symbol = payload.get("symbol", "").lower()
                        if symbol != "btc/usd":
                            continue

                        price = float(payload.get("value", 0))
                        source_ts = payload.get("timestamp", receive_ts)

                        point = PricePoint(
                            source="poly_chainlink",
                            price=price,
                            source_ts=source_ts,
                            receive_ts=receive_ts,
                            latency_ms=receive_ts - source_ts,
                        )
                        await queue.put(point)

                    except TimeoutError:
                        continue
                    except ConnectionClosed:
                        print("[Poly/Chainlink] Connection closed, reconnecting...")
                        break

        except Exception as e:
            if not stop_event.is_set():
                print(f"[Poly/Chainlink] Error: {e}, reconnecting in 1s...")
                await asyncio.sleep(1)


def format_price(price: float) -> str:
    """Format price with commas and 2 decimal places."""
    return f"${price:,.2f}"


def print_update(points: dict[str, PricePoint | None]) -> None:
    """Print a real-time update line."""
    now = datetime.now().strftime("%H:%M:%S")

    binance = points.get("binance")
    poly_bin = points.get("poly_binance")
    poly_chain = points.get("poly_chainlink")

    # Calculate spread (max - min) if we have at least 2 prices
    prices = [p.price for p in [binance, poly_bin, poly_chain] if p is not None]
    spread = max(prices) - min(prices) if len(prices) >= 2 else 0

    binance_str = f"{format_price(binance.price)} ({binance.latency_ms}ms)" if binance else "N/A"
    poly_bin_str = (
        f"{format_price(poly_bin.price)} ({poly_bin.latency_ms}ms)" if poly_bin else "N/A"
    )
    poly_chain_str = (
        f"{format_price(poly_chain.price)} ({poly_chain.latency_ms}ms)" if poly_chain else "N/A"
    )

    print(
        f"{now}  Binance: {binance_str:30s}  Poly/Bin: {poly_bin_str:30s}  Poly/Chain: {poly_chain_str:30s}  Spread: ${spread:.2f}"
    )


def print_summary(stats: dict[str, CollectionStats], duration: int) -> None:
    """Print final summary statistics."""
    print("\n" + "=" * 80)
    print(f"=== Summary ({duration}s collection) ===")
    print("=" * 80)
    print(
        f"{'Source':<18} {'Avg Price':>14} {'Avg Latency':>12} {'Min Lat':>10} {'Max Lat':>10} {'Updates':>10}"
    )
    print("-" * 80)

    for name, s in stats.items():
        if s.total_updates > 0:
            print(
                f"{name:<18} {format_price(s.avg_price):>14} {s.avg_latency_ms:>10.1f}ms {s.min_latency_ms:>8}ms {s.max_latency_ms:>8}ms {s.total_updates:>10}"
            )
        else:
            print(f"{name:<18} {'N/A':>14} {'N/A':>12} {'N/A':>10} {'N/A':>10} {0:>10}")

    print("=" * 80)


def create_visualization(data: list[PricePoint], output_path: Path, duration: int) -> None:
    """Create matplotlib visualization of the collected data."""
    if not data:
        print("No data to visualize")
        return

    # Separate data by source
    sources: dict[str, list[PricePoint]] = {"binance": [], "poly_binance": [], "poly_chainlink": []}
    for point in data:
        sources[point.source].append(point)

    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)
    fig.suptitle(f"BTC/USD Price Comparison ({duration}s collection)", fontsize=14)

    colors = {"binance": "#F0B90B", "poly_binance": "#5B4BDB", "poly_chainlink": "#375BD2"}
    markers = {"binance": "o", "poly_binance": "s", "poly_chainlink": "^"}  # circle, square, triangle
    labels = {
        "binance": "Binance Direct",
        "poly_binance": "Poly/Binance",
        "poly_chainlink": "Poly/Chainlink",
    }

    # Get the start time for relative timestamps
    start_time = min(p.receive_ts for p in data) if data else 0

    # Plot prices
    for source, points in sources.items():
        if points:
            times = [(p.receive_ts - start_time) / 1000 for p in points]  # Seconds from start
            prices = [p.price for p in points]
            ax1.plot(
                times,
                prices,
                marker=markers[source],
                linestyle="none",
                markersize=3,
                color=colors[source],
                label=labels[source],
                alpha=0.7,
            )

    ax1.set_ylabel("Price (USD)")
    ax1.legend(loc="upper right")
    ax1.grid(True, alpha=0.3)
    ax1.set_title("Price Over Time")

    # Format y-axis with comma separator
    ax1.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"${x:,.0f}"))

    # Plot latencies
    for source, points in sources.items():
        if points:
            times = [(p.receive_ts - start_time) / 1000 for p in points]
            latencies = [p.latency_ms for p in points]
            ax2.plot(
                times,
                latencies,
                marker=markers[source],
                linestyle="none",
                markersize=3,
                color=colors[source],
                label=labels[source],
                alpha=0.7,
            )

    ax2.set_ylabel("Latency (ms)")
    ax2.set_xlabel("Time (seconds)")
    ax2.legend(loc="upper right")
    ax2.grid(True, alpha=0.3)
    ax2.set_title("Latency Over Time")
    ax2.set_yscale("log")  # Log scale for latency since Chainlink is much slower

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"\nVisualization saved to: {output_path}")


async def collector(duration_secs: int = 60, output_dir: Path | None = None) -> None:
    """
    Main collector that runs all WebSocket clients concurrently.

    Args:
        duration_secs: How long to collect data (seconds)
        output_dir: Directory to save output files (defaults to script directory)
    """
    if output_dir is None:
        output_dir = Path(__file__).parent

    queue: asyncio.Queue[PricePoint] = asyncio.Queue()
    stop_event = asyncio.Event()
    data: list[PricePoint] = []

    # Statistics per source
    stats = {
        "binance": CollectionStats(source="binance"),
        "poly_binance": CollectionStats(source="poly_binance"),
        "poly_chainlink": CollectionStats(source="poly_chainlink"),
    }

    # Latest point per source for display
    latest: dict[str, PricePoint | None] = {
        "binance": None,
        "poly_binance": None,
        "poly_chainlink": None,
    }

    print("\n" + "=" * 100)
    print("=== Price Comparison (BTC/USD) ===")
    print(f"Duration: {duration_secs} seconds")
    print("=" * 100 + "\n")

    # Start WebSocket tasks
    tasks = [
        asyncio.create_task(binance_ws(queue, stop_event)),
        asyncio.create_task(polymarket_binance_ws(queue, stop_event)),
        asyncio.create_task(polymarket_chainlink_ws(queue, stop_event)),
    ]

    start_time = time.time()
    last_print_time = 0.0

    try:
        while time.time() - start_time < duration_secs:
            try:
                # Get data from queue with timeout
                point = await asyncio.wait_for(queue.get(), timeout=0.1)

                # Store data
                data.append(point)
                stats[point.source].update(point)
                latest[point.source] = point

                # Print update every second
                current_time = time.time()
                if current_time - last_print_time >= 1.0:
                    print_update(latest)
                    last_print_time = current_time

            except TimeoutError:
                continue

    except KeyboardInterrupt:
        print("\nInterrupted by user")

    finally:
        # Signal all tasks to stop
        stop_event.set()

        # Wait for tasks to complete
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    # Print summary
    print_summary(stats, duration_secs)

    # Create visualization
    output_path = output_dir / "price_compare_output.png"
    create_visualization(data, output_path, duration_secs)

    print(f"\nTotal data points collected: {len(data)}")


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Compare BTC/USD prices from multiple WebSocket sources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run python pylo/tools/price_compare.py --duration 60
  uv run python pylo/tools/price_compare.py -d 300

Sources:
  1. Binance Direct (bookTicker) - Lowest latency, best bid/ask
  2. Polymarket RTDS Binance - Relayed Binance prices
  3. Polymarket RTDS Chainlink - On-chain oracle prices
        """,
    )
    parser.add_argument(
        "-d",
        "--duration",
        type=int,
        default=60,
        help="Duration to collect data in seconds (default: 60)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=None,
        help="Output directory for visualization (default: script directory)",
    )

    args = parser.parse_args()

    output_dir = Path(args.output) if args.output else None

    asyncio.run(collector(duration_secs=args.duration, output_dir=output_dir))


if __name__ == "__main__":
    main()
