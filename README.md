# Aubit-Poly

Prediction market arbitrage detection system between Polymarket, Kalshi, and real-world crypto/stock markets.

## Features

- **Internal Arbitrage Detection**: Find YES + NO < $1 opportunities within single markets
- **Cross-Platform Arbitrage**: Detect price discrepancies across Polymarket, Kalshi, and other prediction markets
- **Hedging Arbitrage**: Compare prediction market prices with real-world derivatives (crypto options, stock options)
- **Real-time Alerts**: Discord and Telegram notifications for detected opportunities

## Quick Start

**No API keys required for market data!** All prediction market and exchange data is publicly available.

```bash
# Install dependencies
uv sync

# Run single scan (works immediately - no setup needed!)
uv run python main.py --mode single

# Run continuous monitoring
uv run python main.py --mode continuous

# Run demo mode with simulated data
uv run python main.py --mode demo
```

## Configuration (Optional)

API keys are **only needed for placing trades**, not for watching market data.

For alerts, copy `.env.example` to `.env` and configure:

- `DISCORD_WEBHOOK_URL`: Discord alert notifications
- `TELEGRAM_BOT_TOKEN`: Telegram alert notifications

For trading (optional):
- `POLYMARKET_API_KEY`: Polymarket trading credentials
- `KALSHI_API_KEY`: Kalshi trading credentials
- `BINANCE_API_KEY`: Binance trading credentials

## Architecture

```
src/
├── config/          # Settings management
├── data_sources/    # API clients (Polymarket, Kalshi, Binance, Yahoo)
├── arbitrage/       # Detection algorithms
├── models/          # Data models
├── alerts/          # Notification system
└── utils/           # Probability calculations
```

## Arbitrage Types

### Internal Arbitrage
When YES + NO prices sum to less than $1:
```
YES: $0.45
NO:  $0.50
---------
Total: $0.95 → 5% guaranteed profit
```

### Cross-Platform Arbitrage
Same event priced differently on different platforms:
```
Polymarket YES: $0.40
Kalshi NO:      $0.55
---------
Total: $0.95 → 5% profit
```

### Hedging Arbitrage
Prediction market price differs from options-implied probability:
```
Polymarket "BTC > $100k": 40%
Binance options implied:  55%
---------
Discrepancy: 15% edge
```

## Development

```bash
# Install dev dependencies
uv sync --all-extras

# Run tests
uv run pytest

# Type checking
uv run mypy src/

# Linting
uv run ruff check src/
```

## License

MIT
