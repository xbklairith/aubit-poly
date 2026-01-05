# Tech Stack

## Language & Runtime

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Language | Python 3.11+ | Best ecosystem for financial APIs |
| Package Manager | uv | Fast, modern Python package management |
| Type Checking | mypy | Catch errors early |

## Core Dependencies

### Data & APIs
```
httpx           # Async HTTP client
websockets      # Real-time data feeds
pydantic        # Data validation
python-dotenv   # Environment management
```

### Prediction Markets
```
polymarket-apis  # Official Polymarket client
# Kalshi - custom implementation
```

### Crypto Exchanges
```
ccxt            # Unified crypto exchange API
python-binance  # Binance-specific features
```

### Stock/Options
```
yfinance        # Yahoo Finance data
pandas          # Data manipulation
```

### Alerts & Notifications
```
discord.py      # Discord webhooks
python-telegram-bot  # Telegram alerts
```

### Database & Caching
```
sqlalchemy      # ORM
sqlite          # Local development
redis           # Caching (optional)
```

### Development
```
pytest          # Testing
pytest-asyncio  # Async test support
ruff            # Linting + formatting
pre-commit      # Git hooks
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Main Application                        │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │   Alerts    │  │  Arbitrage  │  │    Data Sources     │ │
│  │  (Discord/  │◄─│   Engine    │◄─│  (Polymarket, etc.) │ │
│  │  Telegram)  │  │             │  │                     │ │
│  └─────────────┘  └─────────────┘  └─────────────────────┘ │
│         ▲               │                    ▲              │
│         │               ▼                    │              │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                   Models / Storage                   │   │
│  │              (SQLite / Redis Cache)                  │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## File Structure

```
aubit-poly/
├── src/
│   ├── config/          # Settings, environment
│   ├── data_sources/    # API integrations
│   │   ├── polymarket.py
│   │   ├── kalshi.py
│   │   ├── crypto/      # Binance, Coinbase
│   │   └── stocks/      # Yahoo Finance
│   ├── arbitrage/       # Detection algorithms
│   │   ├── internal.py
│   │   ├── cross_platform.py
│   │   └── hedging.py
│   ├── models/          # Data models
│   ├── alerts/          # Notification system
│   └── utils/           # Helpers
├── tests/
├── docx/                # Documentation
└── main.py              # Entry point
```

## API Rate Limits

| Service | Rate Limit | Strategy |
|---------|------------|----------|
| Polymarket | 1000/hour | Batch requests |
| Binance | 1200/min | Use WebSocket |
| Yahoo Finance | 2000/hour | Cache heavily |
| Kalshi | TBD | TBD |

## Performance Targets

- Startup time: <5 seconds
- Memory footprint: <500MB
- API response caching: 30 seconds default
- Opportunity detection: <5 second latency
