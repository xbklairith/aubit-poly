# Aubit-Poly: Prediction Market Arbitrage System

## Overview

Aubit-Poly is an arbitrage detection and analysis platform that identifies profitable opportunities between:
- **Prediction markets** (Polymarket, Kalshi, PredictIt)
- **Real-world markets** (Crypto exchanges, Stock/Options markets)

## Core Value Proposition

Prediction markets often misprice events relative to:
1. Other prediction platforms (cross-platform arbitrage)
2. Actual market instruments (hedging arbitrage)
3. Related markets on the same platform (internal arbitrage)

This system detects these discrepancies and alerts traders to opportunities.

## Arbitrage Types

### 1. Internal Arbitrage
- **YES + NO < $1**: Buy both sides for guaranteed profit
- **Related Markets**: Correlated events with inconsistent pricing
- **Time Decay**: Near-expiry markets with predictable outcomes

### 2. Cross-Platform Arbitrage
- Polymarket vs Kalshi price discrepancies
- Same event, different prices across platforms
- Requires accounts on multiple platforms

### 3. Real-World Hedging Arbitrage
- **Crypto**: Polymarket BTC predictions vs actual BTC derivatives
- **Stocks**: Event outcomes vs stock option implied probabilities
- **Example**: "BTC above $100k" at 30% on Polymarket, but options market implies 50%

## Target Markets

### Prediction Markets
| Platform | API | Focus |
|----------|-----|-------|
| Polymarket | REST + WebSocket | Primary |
| Kalshi | REST | Secondary |
| Metaculus | GraphQL | Research |

### Real-World Markets
| Market | Source | Use Case |
|--------|--------|----------|
| BTC/ETH | Binance, Coinbase | Crypto predictions |
| Stocks | Yahoo Finance | Company events |
| Options | CBOE, Deribit | Implied probabilities |

## Success Metrics

- **Detection Latency**: <5 seconds for new opportunities
- **Accuracy**: >95% of detected opportunities are valid
- **Coverage**: Monitor 100+ active markets simultaneously
- **False Positive Rate**: <5%

## Constraints

- Start with detection/alerts only (no automated execution)
- Respect API rate limits
- No market manipulation or wash trading
- Compliance with platform ToS
