# Aubit-Poly

Prediction market arbitrage detection system between Polymarket, Kalshi, and real-world crypto/stock markets.

## Features

- **Internal Arbitrage Detection**: Find YES + NO < $1 opportunities within single markets
- **Cross-Platform Arbitrage**: Detect price discrepancies across Polymarket, Kalshi, and other prediction markets
- **Hedging Arbitrage**: Compare prediction market prices with real-world derivatives (crypto options, stock options)
- **Real-time Alerts**: Discord and Telegram notifications for detected opportunities

## Quick Start

```bash
# Install dependencies
uv sync

# Copy environment file
cp .env.example .env

# Run demo mode (no API keys needed)
uv run python main.py --mode demo

# Run single scan
uv run python main.py --mode single

# Run continuous monitoring
uv run python main.py --mode continuous
```

## Configuration

See `.env.example` for all configuration options. Key settings:

- `POLYMARKET_API_KEY`: Polymarket API credentials
- `KALSHI_API_KEY`: Kalshi API credentials
- `BINANCE_API_KEY`: Binance credentials for options data
- `DISCORD_WEBHOOK_URL`: Discord alert notifications
- `TELEGRAM_BOT_TOKEN`: Telegram alert notifications

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
