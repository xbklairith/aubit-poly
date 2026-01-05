# User Action Required: Set Up API Keys (Optional)

## Overview

**Good news: No API keys are needed for market data!**

All prediction market data (Polymarket, Kalshi) and crypto exchange data (Binance) is publicly available. You can run the arbitrage detector immediately without any setup.

API keys are **only required if you want to place trades**.

## Prerequisites

- [ ] Python 3.11+ installed
- [ ] `uv` package manager installed (`pip install uv` or `brew install uv`)

## Quick Start (No API Keys)

```bash
cd /Users/xb/table/aubit-poly
uv sync
uv run python main.py --mode single
```

That's it! The scanner will fetch live market data and detect arbitrage opportunities.

---

## Optional: Configure Alerts

To receive notifications when opportunities are found:

```bash
cp .env.example .env
```

Then configure Discord or Telegram (see below).

---

## Optional: Configure Trading APIs

Only needed if you want to place trades programmatically.

### Polymarket (Trading Only)

1. Visit [Polymarket Docs](https://docs.polymarket.com)
2. Create a Polymarket account and connect a wallet
3. Generate API credentials from developer settings

Add to `.env`:
```
POLYMARKET_API_KEY=your_api_key
POLYMARKET_API_SECRET=your_api_secret
POLYMARKET_API_PASSPHRASE=your_passphrase
POLYMARKET_WALLET_ADDRESS=0x...
```

### 4. Configure Kalshi (Optional)

1. Create account at [Kalshi](https://kalshi.com)
2. Go to Developer Settings
3. Generate API credentials

Add to `.env`:
```
KALSHI_API_KEY=your_kalshi_key
KALSHI_API_SECRET=your_kalshi_secret
```

### 5. Configure Binance (For Hedging Arbitrage)

For options data and hedging strategies:

1. Create account at [Binance](https://www.binance.com)
2. Go to API Management
3. Create new API key (read-only is sufficient for data)

Add to `.env`:
```
BINANCE_API_KEY=your_binance_key
BINANCE_API_SECRET=your_binance_secret
```

### 6. Configure Alerts (Optional)

#### Discord Webhook

1. Open your Discord server
2. Go to Server Settings > Integrations > Webhooks
3. Create a new webhook
4. Copy the webhook URL

```
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
```

#### Telegram Bot

1. Message [@BotFather](https://t.me/botfather) on Telegram
2. Create a new bot with `/newbot`
3. Copy the bot token
4. Start a chat with your bot and get your chat ID

```
TELEGRAM_BOT_TOKEN=123456789:ABC...
TELEGRAM_CHAT_ID=your_chat_id
```

## Verification

Run the demo mode to verify setup:

```bash
uv run python main.py --mode demo
```

Run a single scan with real data:

```bash
uv run python main.py --mode single
```

## Expected Output

```
╔═══════════════════════════════════════════════════════════════╗
║                      AUBIT-POLY                                ║
║           Prediction Market Arbitrage Detection               ║
╚═══════════════════════════════════════════════════════════════╝

Starting single scan...
Fetched 150 Polymarket markets
Fetched 80 Kalshi markets

============================================================
Found 2 arbitrage opportunities!
============================================================

🎯 ARBITRAGE OPPORTUNITY DETECTED
Type: internal
Profit: 2.50%
...
```

## Troubleshooting

### "Client not connected" Error
- Ensure `.env` file exists and has valid credentials
- Check that API keys are not expired

### Rate Limit Errors
- Reduce `SCAN_INTERVAL` in `.env`
- Use WebSocket connections for high-frequency monitoring

### No Opportunities Found
- This is normal! Arbitrage opportunities are rare and short-lived
- Try running in continuous mode to catch fleeting opportunities
- Check that you're monitoring active markets

### SSL/Network Errors
- Check your internet connection
- Some regions may need VPN for certain APIs
