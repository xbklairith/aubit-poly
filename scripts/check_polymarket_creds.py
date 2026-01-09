#!/usr/bin/env python3
"""
Check Polymarket API credentials.
Uses POLYMARKET_WALLET_ADDRESS + API credentials (no private key needed).
"""

import os
import sys
import hmac
import hashlib
import base64
import requests
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

HOST = "https://clob.polymarket.com"


def build_hmac_signature(secret: str, timestamp: str, method: str, path: str, body=None) -> str:
    """Build HMAC signature matching py_clob_client format."""
    base64_secret = base64.urlsafe_b64decode(secret)
    message = str(timestamp) + str(method) + str(path)
    if body:
        message += str(body).replace("'", '"')
    h = hmac.new(base64_secret, bytes(message, "utf-8"), hashlib.sha256)
    return base64.urlsafe_b64encode(h.digest()).decode("utf-8")


def main():
    print("=" * 50)
    print("Polymarket API Credentials Check")
    print("=" * 50)

    # Get credentials from env
    wallet_address = os.getenv("POLYMARKET_WALLET_ADDRESS", "")
    api_key = os.getenv("POLYMARKET_API_KEY", "")
    api_secret = os.getenv("POLYMARKET_API_SECRET", "")
    api_passphrase = os.getenv("POLYMARKET_API_PASSPHRASE", "")

    missing = []
    if not wallet_address:
        missing.append("POLYMARKET_WALLET_ADDRESS")
    if not api_key:
        missing.append("POLYMARKET_API_KEY")
    if not api_secret:
        missing.append("POLYMARKET_API_SECRET")
    if not api_passphrase:
        missing.append("POLYMARKET_API_PASSPHRASE")

    if missing:
        print("\n❌ Missing in .env:")
        for m in missing:
            print(f"   - {m}")
        sys.exit(1)

    print(f"\n1. Credentials from .env:")
    print(f"   Wallet: {wallet_address}")
    print(f"   API Key: {api_key[:8]}...{api_key[-4:]}")
    print(f"   API Secret: {api_secret[:8]}...{api_secret[-4:]}")
    print(f"   Passphrase: {api_passphrase[:4]}...{api_passphrase[-4:]}")

    def make_authenticated_request(method: str, path: str, query: str = ""):
        """Make authenticated request to Polymarket API."""
        timestamp = str(int(datetime.now().timestamp()))
        # HMAC signature uses path WITHOUT query params
        signature = build_hmac_signature(api_secret, timestamp, method, path)
        headers = {
            "POLY_ADDRESS": wallet_address,
            "POLY_SIGNATURE": signature,
            "POLY_TIMESTAMP": timestamp,
            "POLY_API_KEY": api_key,
            "POLY_PASSPHRASE": api_passphrase,
        }
        url = f"{HOST}{path}"
        if query:
            url += f"?{query}"
        return requests.get(url, headers=headers)

    # Get balance
    print("\n2. Checking balance...")
    try:
        resp = make_authenticated_request(
            "GET",
            "/balance-allowance",
            "asset_type=COLLATERAL&signature_type=0"
        )

        if resp.status_code == 200:
            data = resp.json()
            # Balance is in smallest unit (6 decimals for USDC)
            balance_raw = int(data.get("balance", 0))
            balance_usdc = balance_raw / 1_000_000
            print(f"   ✅ Available USDC: ${balance_usdc:.2f}")
        else:
            print(f"   ❌ Failed: {resp.status_code} - {resp.text}")
            sys.exit(1)

    except Exception as e:
        print(f"   ❌ Failed: {e}")
        sys.exit(1)

    # Get trades
    print("\n3. Checking trades...")
    try:
        resp = make_authenticated_request("GET", "/data/trades")

        if resp.status_code == 200:
            data = resp.json()
            trades = data if isinstance(data, list) else data.get("data", [])
            print(f"   ✅ Trades: {len(trades)}")
            for t in trades[:5]:  # Show first 5
                side = t.get("side", "?")
                size = t.get("size", 0)
                price = t.get("price", 0)
                print(f"      - {side} {size} @ ${price}")
        else:
            print(f"   ⚠️  Trades: {resp.status_code} - {resp.text[:100]}")

    except Exception as e:
        print(f"   ⚠️  Trades failed: {e}")

    # Get orders
    print("\n4. Checking orders...")
    try:
        path = "/data/orders"
        resp = make_authenticated_request("GET", path)

        if resp.status_code == 200:
            data = resp.json()
            orders = data.get("data", [])
            print(f"   ✅ Open orders: {len(orders)}")
        else:
            print(f"   ❌ Failed: {resp.status_code} - {resp.text}")
            sys.exit(1)

    except Exception as e:
        print(f"   ❌ Failed: {e}")
        sys.exit(1)

    # Server time (no auth needed)
    try:
        resp = requests.get(f"{HOST}/time")
        if resp.status_code == 200:
            print(f"   ✅ Server time: {resp.text}")
    except Exception as e:
        print(f"   ⚠️  Server time failed: {e}")

    print("\n" + "=" * 50)
    print("✅ All checks passed! Credentials are valid.")
    print("=" * 50)


if __name__ == "__main__":
    main()
