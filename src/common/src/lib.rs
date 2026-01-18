//! Common library for aubit-poly Rust services.
//!
//! Provides shared functionality:
//! - Configuration loading from .env
//! - Database connection pooling
//! - Gamma API client
//! - Shared data models

pub mod clob;
pub mod config;
pub mod db;
pub mod gamma;
pub mod models;
pub mod repository;

pub use clob::{BookMessage, ClobClient, ClobMessage, PriceChange, PriceChangeMessage, PriceLevel};
pub use config::Config;
pub use db::Database;
pub use gamma::{GammaClient, GammaMarket, MarketType, ParsedMarket};
pub use repository::{
    deactivate_expired_markets, get_15m_updown_markets_with_fresh_orderbooks, get_active_markets,
    get_active_markets_expiring_within, get_latest_orderbook_snapshot, get_market_by_condition_id,
    get_market_resolution, get_market_resolutions_batch, get_markets_with_fresh_orderbooks,
    get_priority_markets_hybrid, insert_orderbook_snapshot, update_no_best_prices,
    update_no_orderbook, update_yes_best_prices, update_yes_orderbook, upsert_market,
    upsert_market_resolution, MarketResolution, MarketResolutionInsert, MarketWithPrices,
};
