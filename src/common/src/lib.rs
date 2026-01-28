//! Common library for aubit-poly Rust services.
//!
//! Provides shared functionality:
//! - Configuration loading from .env
//! - Database connection pooling
//! - Gamma API client (Polymarket)
//! - Kalshi API client
//! - Limitless API client
//! - Platform abstraction for cross-platform arbitrage
//! - Shared data models
//! - Binance WebSocket client
//! - Trading executor utilities

pub mod binance_ws;
pub mod clob;
pub mod config;
pub mod db;
pub mod executor;
pub mod gamma;
pub mod kalshi;
pub mod kalshi_ws;
pub mod limitless;
pub mod limitless_ws;
pub mod models;
pub mod platform;
pub mod polymarket_rtds;
pub mod repository;

pub use binance_ws::{
    BinanceBookTicker, BinanceEvent, BinanceKline, BinanceStreamType, BinanceWsClient,
    BinanceWsStream, KlineBuffer, MomentumDirection,
};
pub use clob::{BookMessage, ClobClient, ClobMessage, PriceChange, PriceChangeMessage, PriceLevel};
pub use config::Config;
pub use db::Database;
pub use executor::{
    cancel_order, cancel_order_standalone, ensure_authenticated, execute_sell_order, execute_trade,
    query_order_fill_standalone, CachedAuth, DryRunPortfolio, SimulatedPosition, MAX_SHARES,
};
pub use gamma::{GammaClient, GammaMarket, MarketType, ParsedMarket};
pub use repository::{
    calculate_effective_fill_price,
    calculate_fill_price_with_slippage,
    deactivate_expired_markets,
    get_15m_updown_markets_with_fresh_orderbooks,
    get_15m_updown_markets_with_orderbooks,
    get_active_markets,
    get_active_markets_expiring_within,
    get_cross_platform_matches,
    get_latest_orderbook_snapshot,
    get_limitless_markets_with_prices,
    get_market_by_condition_id,
    get_market_resolution,
    get_market_resolutions_batch,
    get_markets_by_platform,
    get_markets_with_fresh_orderbooks,
    get_platform_markets_with_prices,
    get_priority_markets_hybrid,
    get_recent_opportunities,
    insert_orderbook_snapshot,
    record_cross_platform_opportunity,
    update_kalshi_prices,
    update_limitless_prices,
    update_no_best_prices,
    update_no_orderbook,
    update_polymarket_prices,
    update_yes_best_prices,
    update_yes_orderbook,
    upsert_cross_platform_match,
    // Kalshi and cross-platform functions
    upsert_kalshi_market,
    // Limitless functions
    upsert_limitless_market,
    upsert_market,
    upsert_market_resolution,
    CrossPlatformMatchInsert,
    FillEstimate,
    KalshiMarketInsert,
    LimitlessMarketInsert,
    MarketResolution,
    MarketResolutionInsert,
    MarketWithOrderbook,
    MarketWithPlatform,
    MarketWithPrices,
    OrderbookLevel,
};

// Kalshi API client
pub use kalshi::{
    KalshiClient, KalshiError, KalshiMarket, KalshiMarketType, KalshiOrderbook, ParsedKalshiMarket,
    KALSHI_API_URL, KALSHI_CRYPTO_ASSETS,
};

// Platform abstraction for cross-platform arbitrage
pub use platform::{CrossPlatformOpportunity, MarketPair, OrderbookDepth, Platform, UnifiedMarket};

// Kalshi WebSocket streaming
pub use kalshi_ws::{
    run_kalshi_orderbook_stream, KalshiOrderbookUpdate, KalshiWsClient, KALSHI_WS_URL,
};

// Limitless API client
pub use limitless::{
    LimitlessClient, LimitlessError, LimitlessMarket, LimitlessMarketType, LimitlessOrderbook,
    ParsedLimitlessMarket, LIMITLESS_API_URL, LIMITLESS_CRYPTO_ASSETS, LIMITLESS_WS_URL,
};

// Limitless WebSocket streaming
pub use limitless_ws::{
    run_limitless_orderbook_stream, LimitlessOrderbookUpdate, LimitlessWsClient,
};

// Polymarket RTDS (Chainlink prices)
pub use polymarket_rtds::{
    asset_to_chainlink_symbol, chainlink_symbol_to_asset, ChainlinkPrice, ChainlinkPriceBuffer,
    PolymarketRtdsClient, RtdsStream, TimestampedPrice, POLYMARKET_RTDS_URL,
};
