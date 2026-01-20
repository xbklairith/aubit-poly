# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Aubit-Poly is a prediction market arbitrage detection and execution system. It identifies profit opportunities across:
- **Prediction markets**: Polymarket, Kalshi
- **Real-world markets**: Crypto exchanges (Binance, Coinbase), stocks (Yahoo Finance)

The system uses a **polyglot architecture**: Python for arbitrage detection/APIs and Rust for high-performance trading execution.

## Build & Development Commands

### Python (package manager: uv)
```bash
uv sync                          # Install dependencies
uv sync --all-extras             # Install with dev dependencies
uv run python main.py --mode single      # Single scan
uv run python main.py --mode continuous  # Continuous monitoring
uv run pytest                    # Run tests
uv run pytest tests/test_arbitrage/      # Run specific test suite
uv run mypy pylo/                # Type checking (strict mode)
uv run ruff check pylo/          # Lint
uv run ruff format pylo/         # Format
```

### Rust (workspace with 4 services)
```bash
cargo build --release            # Build all services
cargo test --workspace           # Run all tests
cargo fmt                        # Format
cargo clippy                     # Lint

# Individual services
cargo build --release -p market-scanner
cargo build --release -p orderbook-stream
cargo build --release -p trade-executor
cargo build --release -p poly-check
```

### Running Services
```bash
# Database (required first)
docker compose -f docker-compose-db.yml up -d

# All services via Docker Compose
docker compose up -d             # Start all services
docker compose logs -f           # View logs
docker compose ps                # Check status

# Individual services
docker compose up -d momentum-trader
docker compose logs -f momentum-trader

# Rebuild after code changes
docker compose build && docker compose up -d
```

## Architecture

```
├── pylo/                        # Python package
│   ├── data_sources/            # API clients (Polymarket, Kalshi, Binance, Yahoo)
│   ├── arbitrage/               # Detection algorithms (internal, cross-platform, hedging)
│   ├── bots/                    # Trading bots (spread detector, position tracker)
│   ├── db/                      # SQLAlchemy async models + queries
│   └── models/                  # Pydantic data models
│
├── src/                         # Rust workspace
│   ├── common/                  # Shared library (db, models, API clients)
│   ├── market-scanner/          # Polls Gamma API for markets
│   ├── orderbook-stream/        # WebSocket orderbook aggregator
│   ├── trade-executor/          # Spread arbitrage execution
│   └── poly-check/              # Credential verification
│
├── migrations/                  # PostgreSQL migrations
├── docx/                        # Structured documentation
│   ├── core/                    # Product, tech stack, codebase guide
│   └── features/                # Feature specs (EARS requirements, TDD tasks)
└── docker-compose.yml           # Service orchestration
```

## Key Patterns

### Python Data Sources
All data sources inherit from `BaseDataSource` with async `connect()`, `get_markets()`, `disconnect()` methods.

### Rust Repository Pattern
SQLx compile-time verified queries in `common/src/repository.rs`. Database pooling via tokio-postgres.

### Configuration
- Python: Pydantic settings with `.env` file
- Rust: `dotenvy` for env loading, `clap` for CLI args
- All services share `DATABASE_URL` environment variable

## Domain Knowledge

### Arbitrage Types
1. **Internal**: YES + NO < $1.00 on same market (threshold: 0.5%)
2. **Cross-platform**: Same event priced differently across platforms (threshold: 2%)
3. **Hedging**: Prediction price vs real-world derivatives (threshold: 3%)

### Data Freshness Requirements
- Prices: 5 seconds max staleness (WebSocket)
- Order book: 2 seconds max staleness (WebSocket)
- Market list: 5 minutes (polling)

### Common Pitfalls
- Check order book depth before acting (liquidity illusion)
- Different platforms have different settlement mechanisms (USDC vs USD)
- Resolution sources vary by platform - verify before cross-platform arbitrage

## Database

PostgreSQL with async access:
- Python: SQLAlchemy async + asyncpg
- Rust: SQLx with offline mode (`.sqlx/` directory for compile-time verification)

Run migrations from `migrations/` directory.

## Environment Variables

Required in `.env`:
```
DATABASE_URL=postgres://aubit:password@localhost:5432/aubit_poly
```

For live trading (optional):
```
WALLET_PRIVATE_KEY=
POLYMARKET_API_KEY=
POLYMARKET_API_SECRET=
POLYMARKET_API_PASSPHRASE=
```
