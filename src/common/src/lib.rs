//! Common library for aubit-poly Rust services.
//!
//! Provides shared functionality:
//! - Configuration loading from .env
//! - Database connection pooling
//! - Gamma API client
//! - Shared data models
//! - Binance WebSocket client
//! - Trading executor utilities

pub mod binance_ws;
pub mod clob;
pub mod config;
pub mod db;
pub mod executor;
pub mod gamma;
pub mod models;
pub mod repository;

pub use binance_ws::{
    BinanceKline, BinanceWsClient, BinanceWsStream, KlineBuffer, MomentumDirection,
};
pub use clob::{BookMessage, ClobClient, ClobMessage, PriceChange, PriceChangeMessage, PriceLevel};
pub use config::Config;
pub use db::Database;
pub use executor::{
    cancel_order, cancel_order_standalone, ensure_authenticated, execute_trade, CachedAuth,
    DryRunPortfolio, SimulatedPosition, MAX_SHARES,
};
pub use gamma::{GammaClient, GammaMarket, MarketType, ParsedMarket};
pub use repository::{
    calculate_effective_fill_price, calculate_fill_price_with_slippage, deactivate_expired_markets,
    get_15m_updown_markets_with_fresh_orderbooks, get_15m_updown_markets_with_orderbooks,
    get_active_markets, get_active_markets_expiring_within, get_latest_orderbook_snapshot,
    get_market_by_condition_id, get_market_resolution, get_market_resolutions_batch,
    get_markets_with_fresh_orderbooks, get_priority_markets_hybrid, insert_orderbook_snapshot,
    update_no_best_prices, update_no_orderbook, update_yes_best_prices, update_yes_orderbook,
    upsert_market, upsert_market_resolution, FillEstimate, MarketResolution,
    MarketResolutionInsert, MarketWithOrderbook, MarketWithPrices, OrderbookLevel,
};
