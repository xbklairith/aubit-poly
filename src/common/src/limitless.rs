//! Limitless Exchange API client for fetching prediction markets.
//!
//! Limitless is a Polymarket fork on Base L2 with:
//! - No KYC required (global access)
//! - Hourly crypto markets (15m coming soon)
//! - $750M+ volume traded
//! - 0% trading fees
//! - WebSocket orderbook support
//!
//! API Base URL: https://api.limitless.exchange/api-v1
//! Only `single-clob` markets have orderbooks. AMM markets return 400.

use chrono::{DateTime, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// Limitless API base URL
pub const LIMITLESS_API_URL: &str = "https://api.limitless.exchange/api-v1";

/// Limitless WebSocket URL
pub const LIMITLESS_WS_URL: &str = "wss://ws.limitless.exchange/markets";

/// Supported crypto assets for cross-platform matching
pub const LIMITLESS_CRYPTO_ASSETS: &[&str] = &["BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX"];

#[derive(Debug, Error)]
pub enum LimitlessError {
    #[error("HTTP request failed: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("Failed to parse response: {0}")]
    ParseError(String),

    #[error("API error: {0}")]
    ApiError(String),

    #[error("Rate limit exceeded")]
    RateLimitExceeded,

    #[error("Market not found: {0}")]
    MarketNotFound(String),

    #[error("AMM market - no orderbook: {0}")]
    AmmMarket(String),
}

/// Raw market data from Limitless API.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct LimitlessMarket {
    /// Market slug (unique identifier, e.g., "btc-hourly-up-down")
    #[serde(default)]
    pub slug: String,
    /// Market title/question
    #[serde(default)]
    pub title: String,
    /// Market description
    pub description: Option<String>,
    /// Market type: "single-clob", "group-negrisk", "AMM"
    #[serde(rename = "market_type")]
    pub market_type: Option<String>,
    /// Market status
    pub status: Option<String>,
    /// End/resolution time (ISO format)
    #[serde(rename = "endTime")]
    pub end_time: Option<String>,
    /// Alternative end time field
    pub close_time: Option<String>,
    /// Position IDs [YES, NO]
    #[serde(rename = "positionIds", default)]
    pub position_ids: Vec<String>,
    /// Best YES bid price (0-1 decimal)
    #[serde(rename = "bestYesBid")]
    pub best_yes_bid: Option<f64>,
    /// Best YES ask price (0-1 decimal)
    #[serde(rename = "bestYesAsk")]
    pub best_yes_ask: Option<f64>,
    /// Best NO bid price (0-1 decimal)
    #[serde(rename = "bestNoBid")]
    pub best_no_bid: Option<f64>,
    /// Best NO ask price (0-1 decimal)
    #[serde(rename = "bestNoAsk")]
    pub best_no_ask: Option<f64>,
    /// Current price (mid or last)
    pub price: Option<f64>,
    /// 24h volume
    #[serde(rename = "volume24h")]
    pub volume_24h: Option<f64>,
    /// Total volume
    pub volume: Option<f64>,
    /// Liquidity
    pub liquidity: Option<f64>,
    /// Category
    pub category: Option<String>,
    /// Venue information (contains exchange address for EIP-712 signing)
    pub venue: Option<LimitlessVenue>,
}

/// Venue information from Limitless API
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct LimitlessVenue {
    /// Exchange contract address
    pub exchange: Option<String>,
    /// Chain ID (Base L2 = 8453)
    #[serde(rename = "chainId")]
    pub chain_id: Option<u64>,
}

impl LimitlessMarket {
    /// Get YES bid as Decimal
    pub fn yes_bid_decimal(&self) -> Option<Decimal> {
        self.best_yes_bid
            .map(|p| Decimal::try_from(p).unwrap_or_default())
    }

    /// Get YES ask as Decimal
    pub fn yes_ask_decimal(&self) -> Option<Decimal> {
        self.best_yes_ask
            .map(|p| Decimal::try_from(p).unwrap_or_default())
    }

    /// Get NO bid as Decimal
    pub fn no_bid_decimal(&self) -> Option<Decimal> {
        self.best_no_bid
            .map(|p| Decimal::try_from(p).unwrap_or_default())
    }

    /// Get NO ask as Decimal
    pub fn no_ask_decimal(&self) -> Option<Decimal> {
        self.best_no_ask
            .map(|p| Decimal::try_from(p).unwrap_or_default())
    }

    /// Get liquidity as Decimal
    pub fn liquidity_decimal(&self) -> Option<Decimal> {
        self.liquidity
            .or(self.volume)
            .map(|l| Decimal::try_from(l).unwrap_or_default())
    }

    /// Check if this is a CLOB market (has orderbook)
    pub fn is_clob(&self) -> bool {
        self.market_type.as_deref() == Some("single-clob")
    }

    /// Get YES position ID
    pub fn yes_position_id(&self) -> Option<&str> {
        self.position_ids.first().map(|s| s.as_str())
    }

    /// Get NO position ID
    pub fn no_position_id(&self) -> Option<&str> {
        self.position_ids.get(1).map(|s| s.as_str())
    }
}

/// Parsed market ready for database insertion and cross-platform matching.
#[derive(Debug, Clone)]
pub struct ParsedLimitlessMarket {
    /// Market slug (unique identifier)
    pub slug: String,
    /// Market title/name
    pub name: String,
    /// Extracted asset (BTC, ETH, etc.)
    pub asset: String,
    /// Extracted timeframe (15m, 1h, daily)
    pub timeframe: String,
    /// Close/expiration time
    pub close_time: DateTime<Utc>,
    /// YES position ID
    pub yes_position_id: String,
    /// NO position ID
    pub no_position_id: String,
    /// Best bid for YES outcome (dollars 0-1)
    pub yes_best_bid: Option<Decimal>,
    /// Best ask for YES outcome (dollars 0-1)
    pub yes_best_ask: Option<Decimal>,
    /// Best bid for NO outcome (dollars 0-1)
    pub no_best_bid: Option<Decimal>,
    /// Best ask for NO outcome (dollars 0-1)
    pub no_best_ask: Option<Decimal>,
    /// Liquidity in dollars
    pub liquidity: Option<Decimal>,
    /// Market type classification
    pub market_type: LimitlessMarketType,
    /// Direction for up/down markets
    pub direction: Option<String>,
    /// Exchange contract address (for EIP-712 signing)
    pub exchange_address: Option<String>,
}

/// Market type classification for Limitless markets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitlessMarketType {
    /// Crypto price up/down (hourly markets)
    UpDown,
    /// Crypto price above/below target
    AboveBelow,
    /// CLOB market (general)
    Clob,
    /// AMM market (no orderbook)
    Amm,
    /// Unknown/other
    Unknown,
}

/// Orderbook from Limitless API.
#[derive(Debug, Clone, Deserialize)]
pub struct LimitlessOrderbook {
    pub yes: LimitlessOrderbookSide,
    pub no: LimitlessOrderbookSide,
}

/// Single side of Limitless orderbook.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct LimitlessOrderbookSide {
    pub bids: Vec<LimitlessOrderbookLevel>,
    pub asks: Vec<LimitlessOrderbookLevel>,
}

/// Single level in Limitless orderbook.
#[derive(Debug, Clone, Deserialize)]
pub struct LimitlessOrderbookLevel {
    /// Price (0-1 decimal)
    pub price: f64,
    /// Size/quantity
    #[serde(alias = "quantity")]
    pub size: f64,
}

/// Simple rate limiter for Limitless API (10 req/sec).
struct RateLimiter {
    last_request: Instant,
    min_interval: Duration,
}

impl RateLimiter {
    fn new(requests_per_second: u32) -> Self {
        Self {
            last_request: Instant::now() - Duration::from_secs(1),
            min_interval: Duration::from_millis(1000 / requests_per_second as u64),
        }
    }

    async fn wait(&mut self) {
        let elapsed = self.last_request.elapsed();
        if elapsed < self.min_interval {
            tokio::time::sleep(self.min_interval - elapsed).await;
        }
        self.last_request = Instant::now();
    }
}

/// Limitless API client.
pub struct LimitlessClient {
    client: Client,
    base_url: String,
    rate_limiter: Arc<Mutex<RateLimiter>>,
}

impl LimitlessClient {
    /// Create a new Limitless API client with default URL.
    pub fn new() -> Self {
        Self::with_url(LIMITLESS_API_URL)
    }

    /// Create a new Limitless API client with custom URL.
    pub fn with_url(base_url: &str) -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("Failed to create HTTP client"),
            base_url: base_url.to_string(),
            // Limitless rate limit is 10 req/sec, we use 8 to be safe
            rate_limiter: Arc::new(Mutex::new(RateLimiter::new(8))),
        }
    }

    /// Wait for rate limiter before making a request.
    async fn rate_limit(&self) {
        self.rate_limiter.lock().await.wait().await;
    }

    /// Fetch all active markets from Limitless.
    pub async fn fetch_active_markets(&self) -> Result<Vec<LimitlessMarket>, LimitlessError> {
        self.rate_limit().await;

        let url = format!("{}/markets/active", self.base_url);

        debug!("Fetching active markets from Limitless");

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            if status.as_u16() == 429 {
                return Err(LimitlessError::RateLimitExceeded);
            }
            let text = response.text().await.unwrap_or_default();
            return Err(LimitlessError::ApiError(format!(
                "API returned status {}: {}",
                status, text
            )));
        }

        let markets: Vec<LimitlessMarket> = response
            .json()
            .await
            .map_err(|e| LimitlessError::ParseError(e.to_string()))?;

        info!("Fetched {} active markets from Limitless", markets.len());
        Ok(markets)
    }

    /// Fetch all active CLOB markets (markets with orderbooks).
    pub async fn fetch_clob_markets(&self) -> Result<Vec<LimitlessMarket>, LimitlessError> {
        let markets = self.fetch_active_markets().await?;
        let clob_markets: Vec<_> = markets.into_iter().filter(|m| m.is_clob()).collect();
        info!("Found {} CLOB markets", clob_markets.len());
        Ok(clob_markets)
    }

    /// Fetch a single market by slug.
    pub async fn fetch_market(&self, slug: &str) -> Result<Option<LimitlessMarket>, LimitlessError> {
        self.rate_limit().await;

        let url = format!("{}/markets/{}", self.base_url, slug);

        let response = self.client.get(&url).send().await?;

        if response.status().as_u16() == 404 {
            return Ok(None);
        }

        if !response.status().is_success() {
            let status = response.status();
            if status.as_u16() == 429 {
                return Err(LimitlessError::RateLimitExceeded);
            }
            return Err(LimitlessError::ApiError(format!(
                "API returned status: {}",
                status
            )));
        }

        let market: LimitlessMarket = response
            .json()
            .await
            .map_err(|e| LimitlessError::ParseError(e.to_string()))?;

        Ok(Some(market))
    }

    /// Fetch orderbook for a specific market slug.
    /// Note: AMM markets return 400 error.
    pub async fn fetch_orderbook(&self, slug: &str) -> Result<LimitlessOrderbook, LimitlessError> {
        self.rate_limit().await;

        let url = format!("{}/markets/{}/orderbook", self.base_url, slug);

        let response = self.client.get(&url).send().await?;

        if response.status().as_u16() == 400 {
            return Err(LimitlessError::AmmMarket(slug.to_string()));
        }

        if response.status().as_u16() == 404 {
            return Err(LimitlessError::MarketNotFound(slug.to_string()));
        }

        if !response.status().is_success() {
            let status = response.status();
            if status.as_u16() == 429 {
                return Err(LimitlessError::RateLimitExceeded);
            }
            return Err(LimitlessError::ApiError(format!(
                "API returned status: {}",
                status
            )));
        }

        let orderbook: LimitlessOrderbook = response
            .json()
            .await
            .map_err(|e| LimitlessError::ParseError(e.to_string()))?;

        Ok(orderbook)
    }

    /// Fetch all crypto CLOB markets and parse them.
    pub async fn fetch_parsed_crypto_markets(&self) -> Result<Vec<ParsedLimitlessMarket>, LimitlessError> {
        let markets = self.fetch_clob_markets().await?;

        let parsed: Vec<ParsedLimitlessMarket> = markets
            .iter()
            .filter_map(|m| self.parse_market(m))
            .collect();

        info!("Parsed {} crypto markets from Limitless", parsed.len());
        Ok(parsed)
    }

    /// Fetch prices for multiple markets (batch).
    pub async fn fetch_market_prices(
        &self,
        slugs: &[String],
    ) -> Result<HashMap<String, ParsedLimitlessMarket>, LimitlessError> {
        let mut results = HashMap::new();

        for slug in slugs {
            match self.fetch_market(slug).await {
                Ok(Some(market)) => {
                    if let Some(parsed) = self.parse_market(&market) {
                        results.insert(slug.clone(), parsed);
                    }
                }
                Ok(None) => {
                    debug!("Market not found: {}", slug);
                }
                Err(e) => {
                    warn!("Failed to fetch market {}: {}", slug, e);
                }
            }
        }

        Ok(results)
    }

    /// Parse a raw Limitless market into our format.
    pub fn parse_market(&self, market: &LimitlessMarket) -> Option<ParsedLimitlessMarket> {
        // Skip non-CLOB markets
        if !market.is_clob() {
            return None;
        }

        // Parse end time
        let close_time = market
            .end_time
            .as_ref()
            .or(market.close_time.as_ref())
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc))?;

        // Skip expired markets
        if close_time < Utc::now() {
            debug!("Skipping expired market: {}", market.slug);
            return None;
        }

        // Extract asset from slug or title
        let asset = extract_asset_from_limitless(&market.slug, &market.title);
        if asset == "UNKNOWN" {
            debug!("Unknown asset for market: {}", market.slug);
            return None;
        }

        // Extract timeframe
        let timeframe = extract_timeframe_from_limitless(&market.slug, &market.title);

        // Determine market type and direction
        let (market_type, direction) = classify_limitless_market(&market.slug, &market.title);

        Some(ParsedLimitlessMarket {
            slug: market.slug.clone(),
            name: market.title.clone(),
            asset,
            timeframe,
            close_time,
            yes_position_id: market.yes_position_id().unwrap_or_default().to_string(),
            no_position_id: market.no_position_id().unwrap_or_default().to_string(),
            yes_best_bid: market.yes_bid_decimal(),
            yes_best_ask: market.yes_ask_decimal(),
            no_best_bid: market.no_bid_decimal(),
            no_best_ask: market.no_ask_decimal(),
            liquidity: market.liquidity_decimal(),
            market_type,
            direction,
            exchange_address: market.venue.as_ref().and_then(|v| v.exchange.clone()),
        })
    }
}

impl Default for LimitlessClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract cryptocurrency asset from Limitless slug and title.
fn extract_asset_from_limitless(slug: &str, title: &str) -> String {
    let slug_upper = slug.to_uppercase();
    let title_upper = title.to_uppercase();

    // Check slug/title for asset names
    if slug_upper.contains("BTC") || title_upper.contains("BITCOIN") {
        return "BTC".to_string();
    }
    if slug_upper.contains("ETH") || title_upper.contains("ETHEREUM") {
        return "ETH".to_string();
    }
    if slug_upper.contains("SOL") || title_upper.contains("SOLANA") {
        return "SOL".to_string();
    }
    if slug_upper.contains("XRP") || title_upper.contains("RIPPLE") {
        return "XRP".to_string();
    }
    if slug_upper.contains("DOGE") || title_upper.contains("DOGECOIN") {
        return "DOGE".to_string();
    }
    if slug_upper.contains("ADA") || title_upper.contains("CARDANO") {
        return "ADA".to_string();
    }
    if slug_upper.contains("AVAX") || title_upper.contains("AVALANCHE") {
        return "AVAX".to_string();
    }
    if slug_upper.contains("MATIC") || title_upper.contains("POLYGON") {
        return "MATIC".to_string();
    }
    if slug_upper.contains("DOT") || title_upper.contains("POLKADOT") {
        return "DOT".to_string();
    }

    "UNKNOWN".to_string()
}

/// Extract timeframe from Limitless slug and title.
fn extract_timeframe_from_limitless(slug: &str, title: &str) -> String {
    let slug_lower = slug.to_lowercase();
    let title_lower = title.to_lowercase();

    // Check for specific timeframe indicators
    if slug_lower.contains("hourly")
        || title_lower.contains("hourly")
        || slug_lower.contains("-1h-")
        || title_lower.contains("1 hour")
    {
        return "1h".to_string();
    }
    if slug_lower.contains("15m")
        || slug_lower.contains("15-min")
        || title_lower.contains("15 min")
        || title_lower.contains("15-minute")
    {
        return "15m".to_string();
    }
    if slug_lower.contains("4h") || title_lower.contains("4 hour") {
        return "4h".to_string();
    }
    if slug_lower.contains("daily")
        || slug_lower.contains("24h")
        || title_lower.contains("daily")
        || title_lower.contains("24 hour")
    {
        return "daily".to_string();
    }
    if slug_lower.contains("weekly") || title_lower.contains("weekly") {
        return "weekly".to_string();
    }

    "unknown".to_string()
}

/// Classify Limitless market type from slug and title.
fn classify_limitless_market(slug: &str, title: &str) -> (LimitlessMarketType, Option<String>) {
    let slug_lower = slug.to_lowercase();
    let title_lower = title.to_lowercase();

    // Check for up/down pattern (hourly markets)
    if slug_lower.contains("up") || title_lower.contains(" up ") || title_lower.ends_with(" up") {
        if slug_lower.contains("down") || title_lower.contains("down") {
            // "up-down" markets - need to determine which side
            // Usually the position determines it
            return (LimitlessMarketType::UpDown, None);
        }
        return (LimitlessMarketType::UpDown, Some("up".to_string()));
    }
    if slug_lower.contains("down") || title_lower.contains(" down ") || title_lower.ends_with(" down") {
        return (LimitlessMarketType::UpDown, Some("down".to_string()));
    }

    // Check for above/below pattern
    if slug_lower.contains("above") || title_lower.contains("above") {
        return (LimitlessMarketType::AboveBelow, Some("above".to_string()));
    }
    if slug_lower.contains("below") || title_lower.contains("below") {
        return (LimitlessMarketType::AboveBelow, Some("below".to_string()));
    }

    // Default to CLOB
    (LimitlessMarketType::Clob, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_asset_from_limitless() {
        assert_eq!(
            extract_asset_from_limitless("btc-hourly-up-down", "BTC Hourly Up/Down"),
            "BTC"
        );
        assert_eq!(
            extract_asset_from_limitless("eth-1h-volatility", "Ethereum 1 Hour"),
            "ETH"
        );
        assert_eq!(
            extract_asset_from_limitless("sol-15m-above-1234", "Solana 15 min above $1234"),
            "SOL"
        );
        assert_eq!(
            extract_asset_from_limitless("random-market", "Some Random Market"),
            "UNKNOWN"
        );
    }

    #[test]
    fn test_extract_timeframe_from_limitless() {
        assert_eq!(
            extract_timeframe_from_limitless("btc-hourly-up-down", "BTC Hourly"),
            "1h"
        );
        assert_eq!(
            extract_timeframe_from_limitless("eth-15m-above", "ETH 15 minute price"),
            "15m"
        );
        assert_eq!(
            extract_timeframe_from_limitless("btc-daily", "BTC daily close"),
            "daily"
        );
    }

    #[test]
    fn test_classify_limitless_market() {
        let (t, d) = classify_limitless_market("btc-hourly-up", "BTC up");
        assert_eq!(t, LimitlessMarketType::UpDown);
        assert_eq!(d, Some("up".to_string()));

        let (t, d) = classify_limitless_market("eth-above-4000", "ETH above $4000");
        assert_eq!(t, LimitlessMarketType::AboveBelow);
        assert_eq!(d, Some("above".to_string()));

        let (t, d) = classify_limitless_market("sol-below-200", "SOL below $200");
        assert_eq!(t, LimitlessMarketType::AboveBelow);
        assert_eq!(d, Some("below".to_string()));
    }

    #[test]
    fn test_limitless_market_decimals() {
        let market = LimitlessMarket {
            slug: "test".to_string(),
            title: "Test".to_string(),
            best_yes_bid: Some(0.55),
            best_yes_ask: Some(0.60),
            best_no_bid: Some(0.40),
            best_no_ask: Some(0.45),
            liquidity: Some(1000.0),
            ..Default::default()
        };

        assert_eq!(market.yes_bid_decimal(), Some(Decimal::try_from(0.55).unwrap()));
        assert_eq!(market.yes_ask_decimal(), Some(Decimal::try_from(0.60).unwrap()));
        assert_eq!(market.no_bid_decimal(), Some(Decimal::try_from(0.40).unwrap()));
        assert_eq!(market.no_ask_decimal(), Some(Decimal::try_from(0.45).unwrap()));
    }
}
