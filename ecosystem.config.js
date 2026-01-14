/**
 * PM2 Ecosystem Configuration for aubit-poly
 *
 * This configuration manages three microservices:
 * 1. market-scanner (Rust) - Polls Gamma API for prediction markets
 * 2. orderbook-stream (Rust) - Streams orderbook data from CLOB WebSocket
 * 3. trade-executor (Rust) - Detects spreads and executes trades
 *
 * Usage:
 *   pm2 start ecosystem.config.js
 *   pm2 logs                       # View all logs
 *   pm2 logs market-scanner        # View specific service logs
 *   pm2 monit                      # Real-time monitoring
 *   pm2 stop all                   # Stop all services
 *   pm2 restart all                # Restart all services
 *
 * Prerequisites:
 *   - PostgreSQL running (docker compose -f docker-compose-db.yml up -d)
 *   - Rust services built (cargo build --release)
 *   - Python dependencies installed (uv sync)
 */

// Load DATABASE_URL from .env file manually
const fs = require('fs');
const path = require('path');

let DATABASE_URL = 'postgres://aubit:aubit_dev_password@localhost:5432/aubit_poly';
try {
  const envPath = path.join(__dirname, '.env');
  const envContent = fs.readFileSync(envPath, 'utf8');
  const match = envContent.match(/^DATABASE_URL=(.+)$/m);
  if (match && !match[1].startsWith('#')) {
    DATABASE_URL = match[1].trim();
  }
} catch (e) {
  // Use default if .env not found
}

module.exports = {
  apps: [
    // Rust Market Scanner Service
    {
      name: 'market-scanner',
      script: './target/release/market-scanner',
      interpreter: 'none',
      autorestart: true,
      watch: false,
      max_restarts: 10,
      min_uptime: '10s',
      restart_delay: 5000,
      env: {
        RUST_LOG: 'info',
        DATABASE_URL: DATABASE_URL,
      },
      // Run every 60 seconds by default
      args: '--interval 60',
    },

    // Rust Orderbook Stream Service
    {
      name: 'orderbook-stream',
      script: './target/release/orderbook-stream',
      interpreter: 'none',
      autorestart: true,
      watch: false,
      max_restarts: 10,
      min_uptime: '10s',
      restart_delay: 5000,
      env: {
        RUST_LOG: 'info',
        DATABASE_URL: DATABASE_URL,
      },
      // Hybrid mode: crypto (12h) + event markets (60 days)
      // Supports both short-term crypto and long-dated events (Super Bowl, elections)
      // Reconnect every 300s (5min) - subscription takes 60s+ so short intervals cause stale data
      args: '--hybrid --crypto-hours 12 --event-days 60 --crypto-limit 1500 --event-limit 1500 --reconnect-interval 300',
    },

    // Rust Trade Executor Service
    {
      name: 'trade-executor',
      script: './target/release/trade-executor',
      interpreter: 'none',
      autorestart: true,
      watch: false,
      max_restarts: 10,
      min_uptime: '10s',
      restart_delay: 5000,
      env: {
        RUST_LOG: 'info',
        DATABASE_URL: DATABASE_URL,
      },
      // Dry run mode with 1 second intervals
      // Match Python: assets include UNKNOWN, max expiry 60 days, min profit 2%
      // 30s orderbook freshness - staleness filter discards buffered messages >5s old
      args: '--dry-run --interval-ms 1000 --verbose-timing --assets BTC,ETH,SOL,XRP,UNKNOWN --max-time-to-expiry 5184000 --max-orderbook-age 30 --min-profit 0.02',
    },

    // Rust Expiry Scalper Service
    {
      name: 'expiry-scalper',
      script: './target/release/expiry-scalper',
      interpreter: 'none',
      autorestart: true,
      watch: false,
      max_restarts: 10,
      min_uptime: '10s',
      restart_delay: 5000,
      env: {
        RUST_LOG: 'info',
        DATABASE_URL: DATABASE_URL,
      },
      // Bet on skewed crypto markets near expiry (3 min)
      // $5 position size, threshold: >0.75 buy YES
      args: '--interval-secs 10 --expiry-minutes 3 --position-size 5 --high-threshold 0.75',
    },

    // Python Trade Executor Service (for comparison)
    {
      name: 'trade-executor-py',
      script: 'uv',
      args: 'run python -m pylo.bots.db_spread_arb_bot',
      interpreter: 'none',
      autorestart: true,
      watch: false,
      max_restarts: 10,
      min_uptime: '10s',
      restart_delay: 5000,
      env: {
        LOG_LEVEL: 'INFO',
        DRY_RUN: 'true',
        DATABASE_URL: DATABASE_URL,
      },
    },
  ],
};
