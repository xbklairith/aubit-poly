# CLAUDE.md - AI Assistant Guide for Aubit-Poly

## Project Overview

Aubit-Poly is a prediction market arbitrage detection system that identifies profitable opportunities across:
- **Prediction markets**: Polymarket, Kalshi
- **Crypto exchanges**: Binance (options/futures)
- **Stock markets**: Yahoo Finance

The system detects three types of arbitrage:
1. **Internal**: YES + NO < $1 on the same platform
2. **Cross-Platform**: Same event priced differently across platforms
3. **Hedging**: Prediction market vs real-world derivatives

## Quick Reference

```bash
# Install dependencies
uv sync

# Run single scan (no API keys needed!)
uv run python main.py --mode single

# Run continuous monitoring
uv run python main.py --mode continuous

# Run demo with simulated data
uv run python main.py --mode demo

# Run tests
uv run pytest

# Type checking
uv run mypy src/

# Linting
uv run ruff check src/
```

## Architecture

```
aubit-poly/
├── main.py                 # Entry point - CLI with single/continuous/demo modes
├── src/
│   ├── config/
│   │   └── settings.py     # Pydantic settings from .env
│   ├── data_sources/
│   │   ├── base.py         # BaseDataSource ABC
│   │   ├── polymarket.py   # Polymarket Gamma/CLOB APIs
│   │   ├── kalshi.py       # Kalshi API client
│   │   ├── crypto/
│   │   │   └── binance.py  # Binance futures/options
│   │   └── stocks/
│   │       └── yahoo.py    # Yahoo Finance data
│   ├── arbitrage/
│   │   ├── detector.py     # ArbitrageEngine - main orchestrator
│   │   ├── internal.py     # YES+NO < $1 detection
│   │   ├── cross_platform.py  # Multi-platform detection
│   │   └── hedging.py      # Prediction vs derivatives
│   ├── models/
│   │   ├── market.py       # Market, MarketOutcome, Platform enums
│   │   └── opportunity.py  # ArbitrageOpportunity types
│   ├── alerts/
│   │   └── notifier.py     # Discord, Telegram, Console alerts
│   └── utils/
│       └── probability.py  # Probability calculations
├── tests/                  # Pytest test suite
├── docx/                   # Documentation
└── pyproject.toml          # Project config with tool settings
```

## Key Patterns

### 1. Data Source Pattern
All data sources inherit from `BaseDataSource` (`src/data_sources/base.py`):

```python
class BaseDataSource(ABC):
    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    async def get_markets(self) -> list[Market]: ...
    async def get_market(self, market_id: str) -> Market | None: ...
```

Data sources support async context managers:
```python
async with PolymarketClient() as client:
    markets = await client.get_markets()
```

### 2. Arbitrage Engine Pattern
The `ArbitrageEngine` (`src/arbitrage/detector.py`) orchestrates all detection:
- Fetches markets from all sources in parallel
- Runs all detectors in parallel using `asyncio.gather()`
- Deduplicates and ranks opportunities
- Supports both single scan and continuous modes

### 3. Pydantic Models
All data models use Pydantic v2 for validation:
- `Market`: Prediction market with outcomes
- `MarketOutcome`: Single outcome (YES/NO) with price
- `ArbitrageOpportunity`: Base opportunity class
- `InternalArbOpportunity`, `CrossPlatformArbOpportunity`, `HedgingArbOpportunity`: Specialized types

### 4. Settings Management
Configuration via `pydantic-settings` in `src/config/settings.py`:
- Loads from `.env` file
- All settings have defaults for detection mode
- API keys only needed for trading (not detection)

## Code Conventions

### Naming
- Files: `snake_case.py`
- Classes: `PascalCase`
- Functions/methods: `snake_case`
- Constants: `UPPER_SNAKE_CASE`
- Private methods: `_leading_underscore`

### Async
- ALL I/O operations are async
- Use `httpx` for HTTP (not `requests`)
- Use `asyncio.gather()` for parallel operations
- All data source methods are async

### Decimal Precision
- Use `Decimal` for all financial values (prices, profits)
- Convert from API strings: `Decimal(str(value))`
- Never use float for money

### Logging
```python
import logging
logger = logging.getLogger(__name__)
```

### Type Hints
- Full type annotations required (mypy strict mode)
- Use `X | None` syntax (not `Optional[X]`)
- Use `list[X]` (not `List[X]`)

## Important Files

| File | Purpose |
|------|---------|
| `main.py` | CLI entry point with argparse |
| `src/config/settings.py` | All configuration, environment variables |
| `src/arbitrage/detector.py` | Core orchestration engine |
| `src/models/market.py` | Market data models, Platform enum |
| `src/models/opportunity.py` | Opportunity types with factory methods |
| `src/data_sources/polymarket.py` | Primary data source implementation |
| `src/alerts/notifier.py` | Alert system with multiple channels |

## Testing

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src

# Run specific test file
uv run pytest tests/test_arbitrage.py

# Run with verbose output
uv run pytest -v
```

Tests use `pytest-asyncio` with `asyncio_mode = "auto"` in `pyproject.toml`.

## Environment Configuration

Copy `.env.example` to `.env`. Key variables:

```bash
# Alerts (optional but recommended)
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...

# Trading credentials (optional - only for execution)
POLYMARKET_API_KEY=...
KALSHI_API_KEY=...
BINANCE_API_KEY=...

# Application settings
LOG_LEVEL=INFO
SCAN_INTERVAL=30
MIN_INTERNAL_ARB_PROFIT=0.005    # 0.5%
MIN_CROSS_PLATFORM_ARB_PROFIT=0.02  # 2%
MIN_HEDGING_ARB_PROFIT=0.03      # 3%
```

## Common Tasks

### Adding a New Data Source
1. Create client in `src/data_sources/` inheriting `BaseDataSource`
2. Implement `connect()`, `disconnect()`, `get_markets()`, `get_market()`
3. Add to `ArbitrageEngine._fetch_all_markets()` in `detector.py`
4. Add platform to `Platform` enum in `models/market.py`

### Adding a New Arbitrage Type
1. Create detector in `src/arbitrage/`
2. Add opportunity class in `src/models/opportunity.py`
3. Add type to `ArbitrageType` enum
4. Integrate in `ArbitrageEngine.scan_once()`

### Adding a New Alert Channel
1. Create notifier class inheriting `BaseNotifier` in `src/alerts/notifier.py`
2. Implement `send()` and `send_batch()`
3. Add configuration check to `Settings` class
4. Register in `AlertManager._setup_notifiers()`

## API Rate Limits

| Service | Limit | Strategy |
|---------|-------|----------|
| Polymarket | 1000/hour | Batch requests |
| Binance | 1200/min | WebSocket preferred |
| Yahoo Finance | 2000/hour | Cache heavily |
| Kalshi | TBD | Conservative polling |

## Domain Knowledge

### Arbitrage Fundamentals
- Binary market: YES + NO should equal $1.00
- Internal arb: When YES + NO < $1.00, buy both for guaranteed profit
- Cross-platform: Same event, buy YES on cheaper platform, NO on other
- Hedging: Prediction price vs options-implied probability

### Profit Calculation
```python
# Internal arbitrage
profit = Decimal("1") - (yes_price + no_price)

# Example: YES=$0.45, NO=$0.50
# Total cost: $0.95
# Guaranteed return: $1.00
# Profit: $0.05 (5.26% return)
```

### Risk Factors
- Liquidity depth (displayed prices may have low volume)
- Settlement timing (capital lockup)
- Platform fees (vary by platform)
- Resolution discrepancies (different oracles)

## Gotchas

1. **No API keys needed for detection** - Market data is public
2. **Decimal, not float** - Always use `Decimal` for prices
3. **Async everything** - Never use blocking I/O
4. **Rate limits** - Respect API limits, use caching
5. **High profit = suspicious** - >5% profit often indicates stale data
6. **Check liquidity** - Opportunity size limited by order book depth

## Dependencies

Core:
- `httpx` - Async HTTP client
- `pydantic` / `pydantic-settings` - Data validation
- `ccxt` - Crypto exchange unified API
- `yfinance` - Stock/options data

Dev:
- `pytest` / `pytest-asyncio` - Testing
- `mypy` - Type checking (strict mode)
- `ruff` - Linting and formatting

## Git Workflow

- Main development on feature branches
- Tests must pass before merge
- Use conventional commit messages
- Run `uv run ruff check src/` before committing
