# Codebase Guide

## Directory Structure

```
aubit-poly/
├── src/                    # Source code
│   ├── __init__.py
│   ├── config/             # Configuration management
│   │   ├── __init__.py
│   │   └── settings.py     # Pydantic settings
│   │
│   ├── data_sources/       # External API integrations
│   │   ├── __init__.py
│   │   ├── base.py         # Abstract base class
│   │   ├── polymarket.py   # Polymarket API client
│   │   ├── kalshi.py       # Kalshi API client
│   │   ├── crypto/         # Crypto exchange clients
│   │   │   ├── __init__.py
│   │   │   ├── binance.py
│   │   │   └── coinbase.py
│   │   └── stocks/         # Stock data clients
│   │       ├── __init__.py
│   │       └── yahoo.py
│   │
│   ├── arbitrage/          # Arbitrage detection logic
│   │   ├── __init__.py
│   │   ├── detector.py     # Main orchestrator
│   │   ├── internal.py     # Internal arb (YES+NO<$1)
│   │   ├── cross_platform.py  # Cross-platform arb
│   │   └── hedging.py      # Real-world hedging
│   │
│   ├── models/             # Data models
│   │   ├── __init__.py
│   │   ├── market.py       # Market representations
│   │   └── opportunity.py  # Arbitrage opportunities
│   │
│   ├── alerts/             # Notification system
│   │   ├── __init__.py
│   │   └── notifier.py     # Discord/Telegram alerts
│   │
│   └── utils/              # Utilities
│       ├── __init__.py
│       └── probability.py  # Probability calculations
│
├── tests/                  # Test suite
│   ├── __init__.py
│   ├── conftest.py         # Pytest fixtures
│   ├── test_data_sources/
│   ├── test_arbitrage/
│   └── test_models/
│
├── docx/                   # Documentation
│   ├── core/               # Core docs
│   ├── features/           # Feature specs
│   ├── logs/               # Command logs
│   └── UserInstructions/   # Manual setup tasks
│
├── main.py                 # Entry point
├── pyproject.toml          # Project config
├── .env.example            # Environment template
└── .gitignore
```

## Key Patterns

### 1. Data Source Pattern
All data sources inherit from `BaseDataSource`:
```python
class BaseDataSource(ABC):
    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def get_markets(self) -> list[Market]: ...

    @abstractmethod
    async def disconnect(self) -> None: ...
```

### 2. Arbitrage Detector Pattern
```python
class BaseArbitrageDetector(ABC):
    @abstractmethod
    async def scan(self) -> list[Opportunity]: ...

    @abstractmethod
    def calculate_profit(self, opp: Opportunity) -> Decimal: ...
```

### 3. Pydantic Models
All data models use Pydantic for validation:
```python
class Market(BaseModel):
    id: str
    name: str
    platform: Platform
    yes_price: Decimal
    no_price: Decimal
```

## Conventions

### Naming
- Files: `snake_case.py`
- Classes: `PascalCase`
- Functions: `snake_case`
- Constants: `UPPER_SNAKE_CASE`

### Async
- All I/O operations are async
- Use `httpx` for HTTP, not `requests`
- Use `asyncio.gather()` for parallel operations

### Error Handling
```python
# Custom exceptions in src/exceptions.py
class AubitError(Exception): ...
class APIError(AubitError): ...
class ArbitrageError(AubitError): ...
```

### Logging
```python
import logging
logger = logging.getLogger(__name__)
```

## Running the Project

```bash
# Install dependencies
uv sync

# Run detection
uv run python main.py

# Run tests
uv run pytest

# Type check
uv run mypy src/

# Lint
uv run ruff check src/
```
