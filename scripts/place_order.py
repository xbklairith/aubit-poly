#!/usr/bin/env python3
"""
Place a test order on Polymarket.
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType

HOST = "https://clob.polymarket.com"
CHAIN_ID = 137


def main():
    # Get private key
    private_key = os.getenv("WALLET_PRIVATE_KEY", "")
    if not private_key:
        print("❌ Missing WALLET_PRIVATE_KEY in .env")
        sys.exit(1)

    if not private_key.startswith("0x"):
        private_key = "0x" + private_key

    # Initialize client with proxy wallet
    proxy_wallet = os.getenv("POLYMARKET_WALLET_ADDRESS", "")
    print("Initializing client...")

    # Try signature_type=2 (POLY_GNOSIS_SAFE) with proxy as funder
    client = ClobClient(
        HOST,
        key=private_key,
        chain_id=CHAIN_ID,
        signature_type=2,  # POLY_GNOSIS_SAFE
        funder=proxy_wallet if proxy_wallet else None,
    )
    print(f"Signer: {client.signer.address()}")
    print(f"Funder (proxy): {proxy_wallet}")

    # Derive and set API credentials
    print("Setting up credentials...")
    creds = client.create_or_derive_api_creds()
    client.set_api_creds(creds)

    # Check balance first (use signature_type=2 for proxy wallet)
    from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
    balance_params = BalanceAllowanceParams(
        asset_type=AssetType.COLLATERAL,
        signature_type=2  # POLY_GNOSIS_SAFE
    )
    balance_data = client.get_balance_allowance(balance_params)
    balance_usdc = int(balance_data.get("balance", 0)) / 1_000_000
    print(f"Available USDC: ${balance_usdc:.2f}")

    if balance_usdc < 1.0:
        print(f"\n❌ Insufficient balance. Need at least $1.00 USDC")
        print(f"   Proxy wallet: {proxy_wallet}")
        print(f"   Network: Polygon (MATIC)")
        sys.exit(1)

    # Find active market by checking our trades or using known active token
    print("\nFinding active market...")

    # Get user's trades to find an active market
    trades = client.get_trades()
    token_id = None

    if trades:
        # Use the token from user's trade - guaranteed to have orderbook
        t = trades[0]
        token_id = t.get("asset_id")
        print(f"Using token from recent trade")
        print(f"Token ID: {token_id[:30]}...")
    else:
        # Fallback: try to find any market with orderbook
        import requests
        resp = requests.get(f"{HOST}/markets?limit=200")
        if resp.status_code == 200:
            data = resp.json()
            markets = data.get("data", []) if isinstance(data, dict) else data

            for market in markets[:100]:
                tokens = market.get("tokens", [])
                if not tokens:
                    continue
                tid = tokens[0].get("token_id")
                try:
                    ob = client.get_order_book(tid)
                    if ob.asks:
                        token_id = tid
                        print(f"Found: {market.get('question', '')[:50]}")
                        break
                except:
                    continue

    if not token_id:
        print("❌ No active market found")
        sys.exit(1)

    # Get current price
    orderbook = client.get_order_book(token_id)
    if orderbook.asks:
        best_ask = float(orderbook.asks[0].price)
        print(f"Best ask: ${best_ask:.2f}")
    else:
        best_ask = 0.50  # Default
        print(f"No asks, using default: ${best_ask:.2f}")

    # Calculate size for $1 (round up to meet minimum)
    import math
    size = math.ceil(1.0 / best_ask * 100) / 100  # Round up to 2 decimals
    cost = size * best_ask
    print(f"Size: {size:.2f} shares @ ${best_ask:.2f} = ${cost:.2f}")

    # Get tick size
    tick_size = client.get_tick_size(token_id)
    print(f"Tick size: {tick_size}")

    # Create order
    print("\n🚀 Placing order...")
    order_args = OrderArgs(
        price=best_ask,
        size=size,
        side="BUY",
        token_id=token_id,
    )

    try:
        signed_order = client.create_order(order_args)
        result = client.post_order(signed_order, OrderType.GTC)
        print(f"✅ Order placed!")
        print(f"   Order ID: {result.get('orderID', 'N/A')}")
        print(f"   Status: {result.get('status', 'N/A')}")
    except Exception as e:
        print(f"❌ Order failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
