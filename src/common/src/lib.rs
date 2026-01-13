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

pub use clob::{BookMessage, ClobClient, ClobMessage, PriceLevel};
pub use config::Config;
pub use db::Database;
pub use gamma::{GammaClient, GammaMarket, MarketType, ParsedMarket};
pub use repository::{
    deactivate_expired_markets, get_active_markets, get_active_markets_expiring_within,
    get_latest_orderbook_snapshot, get_market_by_condition_id, get_markets_with_fresh_orderbooks,
    get_priority_markets_hybrid, insert_orderbook_snapshot, upsert_market,
};
