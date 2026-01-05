# Critical Knowledge

## Prediction Market Fundamentals

### Binary Markets
- **YES + NO = $1.00** (always, by definition)
- Prices represent implied probabilities
- YES at $0.60 = 60% implied probability of YES outcome

### Arbitrage Condition (Internal)
```
Arbitrage exists when: YES_ask + NO_ask < $1.00

Example:
  YES ask: $0.48
  NO ask:  $0.50
  Total:   $0.98
  Profit:  $0.02 per share (2% guaranteed return)
```

### Cross-Platform Arbitrage
```
Platform A: BTC > $100k YES = $0.40
Platform B: BTC > $100k NO  = $0.55

Combined: $0.95 < $1.00
Profit: $0.05 per share if you buy YES on A, NO on B
```

## Key APIs

### Polymarket
- **Endpoint**: `https://clob.polymarket.com`
- **Gamma API**: `https://gamma-api.polymarket.com`
- **Auth**: HMAC signature with API key/secret
- **Markets**: GET `/markets`
- **Prices**: GET `/prices?token_id={id}`

### Kalshi
- **Endpoint**: `https://api.kalshi.com/trade-api/v2`
- **Auth**: Bearer token
- **Markets**: GET `/markets`

### Binance
- **Futures**: `https://fapi.binance.com`
- **Options**: `https://eapi.binance.com`
- Use CCXT for unified interface

## Common Pitfalls

### 1. Liquidity Illusion
- Displayed prices may have tiny depth
- Always check order book depth before acting
- $1000 opportunity might only have $50 fillable

### 2. Settlement Risk
- Polymarket settles in USDC on Polygon
- Kalshi settles in USD
- Different settlement times = capital lockup

### 3. Fee Structure
```
Polymarket: 0% maker, 0% taker (as of 2024)
Kalshi: Variable by market
Binance: 0.02-0.04% futures
```

### 4. API Latency
- WebSocket preferred over REST for real-time
- Geographic proximity matters for HFT
- Cache aggressively, but invalidate correctly

### 5. Resolution Discrepancies
- "BTC price" can resolve differently:
  - Polymarket: Specific oracle
  - Kalshi: Official exchange rate
  - Check resolution sources before arbitrage

## Risk Factors

| Risk | Mitigation |
|------|------------|
| API downtime | Multiple data sources, fallbacks |
| Slippage | Check depth, use limit orders |
| Settlement delay | Factor in opportunity cost |
| Regulatory | Monitor platform status |
| Smart contract risk | Audit exposure limits |

## Profit Thresholds

```python
# Minimum thresholds for alerting
MIN_INTERNAL_ARB = 0.005    # 0.5% for internal
MIN_CROSS_PLATFORM = 0.02   # 2% for cross-platform (higher friction)
MIN_HEDGING = 0.03          # 3% for hedging (execution complexity)
```

## Probability Conversion

### Options to Probability
```python
# Binary option / prediction market
prob = option_price / max_payout

# For real options (Black-Scholes implied)
# Use delta as rough probability proxy
# ATM call delta ≈ 0.5 ≈ 50% probability
```

### Crypto Derivatives
```python
# Binance BTC futures premium
# Positive premium = bullish sentiment
premium = (futures_price - spot_price) / spot_price
```

## Data Freshness Requirements

| Data Type | Max Staleness | Refresh Rate |
|-----------|---------------|--------------|
| Prices | 5 seconds | WebSocket |
| Order book | 2 seconds | WebSocket |
| Market list | 5 minutes | Polling |
| Historical | 1 hour | Batch |
