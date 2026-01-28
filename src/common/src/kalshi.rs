//! Kalshi API client for fetching prediction markets.
//!
//! Uses the REST API to fetch crypto markets and orderbooks.
//! See: https://trading-api.readme.io/reference/getting-started
//!
//! Note: Kalshi does NOT have orderbook WebSocket like Polymarket CLOB.
//! Price updates are done via REST polling.

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

/// Kalshi API base URL
pub const KALSHI_API_URL: &str = "https://api.elections.kalshi.com/trade-api/v2";

/// Kalshi supported crypto assets for matching with Polymarket
pub const KALSHI_CRYPTO_ASSETS: &[&str] = &["BTC", "ETH", "SOL", "XRP", "DOGE", "ADA"];

#[derive(Debug, Error)]
pub enum KalshiError {
    #[error("HTTP request failed: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("Failed to parse response: {0}")]
    ParseError(String),

    #[error("API error: {0}")]
    ApiError(String),

    #[error("Rate limit exceeded")]
    RateLimitExceeded,
}

/// Raw market data from Kalshi API.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct KalshiMarket {
    /// Unique ticker (e.g., "KXBTC-25JAN13-T100000")
    #[serde(default)]
    pub ticker: String,
    /// Event ticker (parent event)
    #[serde(default)]
    pub event_ticker: String,
    /// Market title/question
    #[serde(default)]
    pub title: String,
    /// Market subtitle
    pub subtitle: Option<String>,
    /// Market status: "open", "closed", "settled", "active"
    #[serde(default)]
    pub status: String,
    /// Close time (ISO format)
    pub close_time: Option<String>,
    /// Expiration time (ISO format)
    pub expiration_time: Option<String>,
    /// Yes bid price in cents (0-100)
    pub yes_bid: Option<i32>,
    /// Yes ask price in cents (0-100)
    pub yes_ask: Option<i32>,
    /// No bid price in cents (0-100) - directly from API
    pub no_bid: Option<i32>,
    /// No ask price in cents (0-100) - directly from API
    pub no_ask: Option<i32>,
    /// Last traded price in cents
    pub last_price: Option<i32>,
    /// 24h volume in contracts
    pub volume: Option<i64>,
    /// 24h volume string (some responses use this)
    pub volume_24h: Option<i64>,
    /// Open interest
    pub open_interest: Option<i64>,
    /// Liquidity in cents
    pub liquidity: Option<i64>,
    /// Liquidity in dollars (string format from API)
    pub liquidity_dollars: Option<String>,
    /// Category/series
    pub category: Option<String>,
    /// Market rules (primary resolution source)
    pub rules_primary: Option<String>,
    /// Strike price for price-based markets
    pub strike_price: Option<f64>,
    /// Floor strike for range markets
    pub floor_strike: Option<f64>,
    /// Cap strike for range markets
    pub cap_strike: Option<f64>,
    /// Strike type: "less", "greater", etc.
    pub strike_type: Option<String>,
    /// Market type: "binary", etc.
    pub market_type: Option<String>,
}

impl KalshiMarket {
    /// Convert yes price from cents (0-100) to dollars (0-1)
    pub fn yes_bid_dollars(&self) -> Option<Decimal> {
        self.yes_bid.map(|c| Decimal::new(c as i64, 2))
    }

    pub fn yes_ask_dollars(&self) -> Option<Decimal> {
        self.yes_ask.map(|c| Decimal::new(c as i64, 2))
    }

    /// Get NO bid price - use direct field if available, else calculate from YES ask
    pub fn no_bid_dollars(&self) -> Option<Decimal> {
        self.no_bid.map(|c| Decimal::new(c as i64, 2)).or_else(|| {
            self.yes_ask
                .map(|c| Decimal::ONE - Decimal::new(c as i64, 2))
        })
    }

    /// Get NO ask price - use direct field if available, else calculate from YES bid
    pub fn no_ask_dollars(&self) -> Option<Decimal> {
        self.no_ask.map(|c| Decimal::new(c as i64, 2)).or_else(|| {
            self.yes_bid
                .map(|c| Decimal::ONE - Decimal::new(c as i64, 2))
        })
    }

    pub fn last_price_dollars(&self) -> Option<Decimal> {
        self.last_price.map(|c| Decimal::new(c as i64, 2))
    }

    pub fn get_liquidity_dollars(&self) -> Option<Decimal> {
        // Try parsing from string field first, then fall back to integer field
        self.liquidity_dollars
            .as_ref()
            .and_then(|s| s.parse::<Decimal>().ok())
            .or_else(|| self.liquidity.map(|l| Decimal::new(l, 2))) // liquidity is in cents
    }
}

/// Parsed market ready for database insertion and cross-platform matching.
#[derive(Debug, Clone)]
pub struct ParsedKalshiMarket {
    /// Kalshi ticker (unique identifier)
    pub ticker: String,
    /// Event ticker
    pub event_ticker: String,
    /// Market title/name
    pub name: String,
    /// Extracted asset (BTC, ETH, etc.)
    pub asset: String,
    /// Extracted timeframe (15m, 1h, daily)
    pub timeframe: String,
    /// Close/expiration time
    pub close_time: DateTime<Utc>,
    /// Best bid for YES outcome (dollars 0-1)
    pub yes_best_bid: Option<Decimal>,
    /// Best ask for YES outcome (dollars 0-1)
    pub yes_best_ask: Option<Decimal>,
    /// Best bid for NO outcome (dollars 0-1)
    pub no_best_bid: Option<Decimal>,
    /// Best ask for NO outcome (dollars 0-1)
    pub no_best_ask: Option<Decimal>,
    /// Last traded price (dollars)
    pub last_price: Option<Decimal>,
    /// Liquidity in dollars
    pub liquidity: Option<Decimal>,
    /// Market type classification
    pub market_type: KalshiMarketType,
    /// Strike price (for above/below markets)
    pub strike_price: Option<f64>,
    /// Direction for above/below markets
    pub direction: Option<String>,
    /// Rules/resolution source
    pub rules_primary: Option<String>,
}

/// Market type classification for Kalshi markets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KalshiMarketType {
    /// BTC above/below X at time T
    AboveBelow,
    /// BTC up/down (15m markets)
    UpDown,
    /// BTC in range [A, B] at time T
    Range,
    /// Unknown/other
    Unknown,
}

/// Orderbook from Kalshi API.
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiOrderbook {
    pub ticker: String,
    #[serde(default)]
    pub yes: Vec<KalshiOrderbookLevel>,
    #[serde(default)]
    pub no: Vec<KalshiOrderbookLevel>,
}

/// Single level in Kalshi orderbook.
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiOrderbookLevel {
    /// Price in cents (0-100)
    pub price: i32,
    /// Number of contracts
    pub quantity: i64,
}

/// Response wrapper for markets list.
#[derive(Debug, Deserialize)]
struct MarketsResponse {
    markets: Vec<KalshiMarket>,
    cursor: Option<String>,
}

/// Response wrapper for orderbook.
#[derive(Debug, Deserialize)]
struct OrderbookResponse {
    orderbook: KalshiOrderbook,
}

/// Simple rate limiter for Kalshi API (10 req/sec).
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

/// Kalshi API client.
pub struct KalshiClient {
    client: Client,
    base_url: String,
    rate_limiter: Arc<Mutex<RateLimiter>>,
}

impl KalshiClient {
    /// Create a new Kalshi API client with default URL.
    pub fn new() -> Self {
        Self::with_url(KALSHI_API_URL)
    }

    /// Create a new Kalshi API client with custom URL.
    pub fn with_url(base_url: &str) -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("Failed to create HTTP client"),
            base_url: base_url.to_string(),
            // Kalshi rate limit is 10 req/sec, we use 8 to be safe
            rate_limiter: Arc::new(Mutex::new(RateLimiter::new(8))),
        }
    }

    /// Wait for rate limiter before making a request.
    async fn rate_limit(&self) {
        self.rate_limiter.lock().await.wait().await;
    }

    /// Fetch markets from Kalshi API with optional filters.
    pub async fn fetch_markets(
        &self,
        status: Option<&str>,
        series_ticker: Option<&str>,
        limit: i32,
        cursor: Option<&str>,
    ) -> Result<(Vec<KalshiMarket>, Option<String>), KalshiError> {
        self.rate_limit().await;

        let url = format!("{}/markets", self.base_url);

        let mut params = vec![("limit", limit.to_string())];
        if let Some(s) = status {
            params.push(("status", s.to_string()));
        }
        if let Some(s) = series_ticker {
            params.push(("series_ticker", s.to_string()));
        }
        if let Some(c) = cursor {
            params.push(("cursor", c.to_string()));
        }

        debug!("Fetching markets: {:?}", params);

        let response = self.client.get(&url).query(&params).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            if status.as_u16() == 429 {
                return Err(KalshiError::RateLimitExceeded);
            }
            let text = response.text().await.unwrap_or_default();
            return Err(KalshiError::ApiError(format!(
                "API returned status {}: {}",
                status, text
            )));
        }

        let body: MarketsResponse = response
            .json()
            .await
            .map_err(|e| KalshiError::ParseError(e.to_string()))?;

        Ok((body.markets, body.cursor))
    }

    /// Fetch all open crypto markets from Kalshi.
    /// Filters by known crypto series tickers.
    pub async fn fetch_all_crypto_markets(&self) -> Result<Vec<KalshiMarket>, KalshiError> {
        let mut all_markets = Vec::new();

        // Crypto series tickers on Kalshi
        // KXBTC = Bitcoin hourly, KXBTC15M = Bitcoin 15-minute, etc.
        let series_tickers = vec![
            // 15-minute up/down markets (match Polymarket 15m)
            "KXBTC15M", // Bitcoin 15m
            "KXETH15M", // Ethereum 15m
            "KXSOL15M", // Solana 15m
            "KXXRP15M", // XRP 15m
            // Hourly/daily markets
            "KXBTC",  // Bitcoin
            "KXETH",  // Ethereum
            "KXSOL",  // Solana
            "KXXRP",  // XRP
            "KXDOGE", // Dogecoin
            "KXADA",  // Cardano
            "INXBTC", // Bitcoin (INX series)
            "INXETH", // Ethereum (INX series)
            "INXSOL", // Solana (INX series)
        ];

        for series in &series_tickers {
            match self.fetch_markets_by_series(series).await {
                Ok(markets) => {
                    info!("Fetched {} markets for series {}", markets.len(), series);
                    all_markets.extend(markets);
                }
                Err(e) => {
                    warn!("Failed to fetch series {}: {}", series, e);
                }
            }
        }

        info!("Total crypto markets fetched: {}", all_markets.len());
        Ok(all_markets)
    }

    /// Fetch all markets for a specific series ticker.
    async fn fetch_markets_by_series(
        &self,
        series: &str,
    ) -> Result<Vec<KalshiMarket>, KalshiError> {
        let mut all_markets = Vec::new();
        let mut cursor: Option<String> = None;
        let max_iterations = 20; // Safety limit

        for _ in 0..max_iterations {
            let (markets, next_cursor) = self
                .fetch_markets(Some("open"), Some(series), 200, cursor.as_deref())
                .await?;

            if markets.is_empty() {
                break;
            }

            all_markets.extend(markets);

            match next_cursor {
                Some(c) if !c.is_empty() => cursor = Some(c),
                _ => break,
            }
        }

        Ok(all_markets)
    }

    /// Fetch orderbook for a specific market ticker.
    pub async fn fetch_orderbook(
        &self,
        ticker: &str,
        depth: i32,
    ) -> Result<KalshiOrderbook, KalshiError> {
        self.rate_limit().await;

        let url = format!("{}/markets/{}/orderbook", self.base_url, ticker);

        let response = self
            .client
            .get(&url)
            .query(&[("depth", depth.to_string())])
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            if status.as_u16() == 429 {
                return Err(KalshiError::RateLimitExceeded);
            }
            return Err(KalshiError::ApiError(format!(
                "API returned status: {}",
                status
            )));
        }

        let body: OrderbookResponse = response
            .json()
            .await
            .map_err(|e| KalshiError::ParseError(e.to_string()))?;

        Ok(body.orderbook)
    }

    /// Fetch prices for multiple markets (batch).
    /// Note: Kalshi doesn't support batch price queries, so we fetch sequentially.
    pub async fn fetch_market_prices(
        &self,
        tickers: &[String],
    ) -> Result<HashMap<String, ParsedKalshiMarket>, KalshiError> {
        let mut results = HashMap::new();

        for ticker in tickers {
            match self.fetch_market_by_ticker(ticker).await {
                Ok(Some(market)) => {
                    if let Some(parsed) = self.parse_market(&market) {
                        results.insert(ticker.clone(), parsed);
                    }
                }
                Ok(None) => {
                    debug!("Market not found: {}", ticker);
                }
                Err(e) => {
                    warn!("Failed to fetch market {}: {}", ticker, e);
                }
            }
        }

        Ok(results)
    }

    /// Fetch a single market by ticker.
    async fn fetch_market_by_ticker(
        &self,
        ticker: &str,
    ) -> Result<Option<KalshiMarket>, KalshiError> {
        self.rate_limit().await;

        let url = format!("{}/markets/{}", self.base_url, ticker);

        let response = self.client.get(&url).send().await?;

        if response.status().as_u16() == 404 {
            return Ok(None);
        }

        if !response.status().is_success() {
            let status = response.status();
            if status.as_u16() == 429 {
                return Err(KalshiError::RateLimitExceeded);
            }
            return Err(KalshiError::ApiError(format!(
                "API returned status: {}",
                status
            )));
        }

        #[derive(Deserialize)]
        struct MarketResponse {
            market: KalshiMarket,
        }

        let body: MarketResponse = response
            .json()
            .await
            .map_err(|e| KalshiError::ParseError(e.to_string()))?;

        Ok(Some(body.market))
    }

    /// Fetch all open crypto markets and parse them.
    pub async fn fetch_parsed_crypto_markets(
        &self,
    ) -> Result<Vec<ParsedKalshiMarket>, KalshiError> {
        let markets = self.fetch_all_crypto_markets().await?;

        let parsed: Vec<ParsedKalshiMarket> = markets
            .iter()
            .filter_map(|m| self.parse_market(m))
            .collect();

        info!("Parsed {} crypto markets", parsed.len());
        Ok(parsed)
    }

    /// Parse a raw Kalshi market into our format.
    pub fn parse_market(&self, market: &KalshiMarket) -> Option<ParsedKalshiMarket> {
        // Only process open/active markets
        if market.status != "open" && market.status != "active" {
            return None;
        }

        // Parse close time
        let close_time = market
            .close_time
            .as_ref()
            .or(market.expiration_time.as_ref())
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc))?;

        // Skip expired markets
        if close_time < Utc::now() {
            debug!("Skipping expired market: {}", market.ticker);
            return None;
        }

        // Extract asset from ticker or title
        let asset = extract_asset_from_kalshi(&market.ticker, &market.title);
        if asset == "UNKNOWN" {
            debug!("Unknown asset for market: {}", market.ticker);
            return None;
        }

        // Extract timeframe
        let timeframe = extract_timeframe_from_kalshi(&market.ticker, &market.title);

        // Determine market type and direction
        // Use strike_type from API if available, otherwise fall back to text parsing
        let (market_type, direction) = if let Some(ref strike_type) = market.strike_type {
            let dir = match strike_type.as_str() {
                s if s.starts_with("less") => Some("below".to_string()),
                s if s.starts_with("greater") => Some("above".to_string()),
                other => Some(other.to_string()),
            };
            // Check title for "up/down" markets (15m markets)
            let title_lower = market.title.to_lowercase();
            if title_lower.contains("up") && !title_lower.contains("down") {
                (KalshiMarketType::UpDown, Some("up".to_string()))
            } else if title_lower.contains("down") && !title_lower.contains("up") {
                (KalshiMarketType::UpDown, Some("down".to_string()))
            } else {
                (KalshiMarketType::AboveBelow, dir)
            }
        } else {
            classify_kalshi_market(&market.title, &market.subtitle)
        };

        // Get strike price from floor_strike or cap_strike if strike_price is not set
        let strike_price = market
            .strike_price
            .or(market.floor_strike)
            .or(market.cap_strike);

        Some(ParsedKalshiMarket {
            ticker: market.ticker.clone(),
            event_ticker: market.event_ticker.clone(),
            name: market.title.clone(),
            asset,
            timeframe,
            close_time,
            yes_best_bid: market.yes_bid_dollars(),
            yes_best_ask: market.yes_ask_dollars(),
            no_best_bid: market.no_bid_dollars(),
            no_best_ask: market.no_ask_dollars(),
            last_price: market.last_price_dollars(),
            liquidity: market.get_liquidity_dollars(),
            market_type,
            strike_price,
            direction,
            rules_primary: market.rules_primary.clone(),
        })
    }
}

impl Default for KalshiClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract cryptocurrency asset from Kalshi ticker and title.
fn extract_asset_from_kalshi(ticker: &str, title: &str) -> String {
    let ticker_upper = ticker.to_uppercase();
    let title_upper = title.to_uppercase();

    // Check ticker prefixes (KXBTC, INXBTC, etc.)
    if ticker_upper.contains("BTC") || title_upper.contains("BITCOIN") {
        return "BTC".to_string();
    }
    if ticker_upper.contains("ETH") || title_upper.contains("ETHEREUM") {
        return "ETH".to_string();
    }
    if ticker_upper.contains("SOL") || title_upper.contains("SOLANA") {
        return "SOL".to_string();
    }
    if ticker_upper.contains("XRP") {
        return "XRP".to_string();
    }
    if ticker_upper.contains("DOGE") || title_upper.contains("DOGECOIN") {
        return "DOGE".to_string();
    }
    if ticker_upper.contains("ADA") || title_upper.contains("CARDANO") {
        return "ADA".to_string();
    }

    "UNKNOWN".to_string()
}

/// Extract timeframe from Kalshi ticker and title.
fn extract_timeframe_from_kalshi(ticker: &str, title: &str) -> String {
    let title_lower = title.to_lowercase();
    let ticker_lower = ticker.to_lowercase();

    // Check for specific timeframe indicators
    // Handle both "15 min" in title and "15M" in ticker (e.g., KXBTC15M)
    if title_lower.contains("15 min")
        || title_lower.contains("15-minute")
        || title_lower.contains("15min")
        || ticker_lower.contains("15m")
    {
        return "15m".to_string();
    }
    if title_lower.contains("1 hour")
        || title_lower.contains("hourly")
        || ticker_lower.contains("-1h")
    {
        return "1h".to_string();
    }
    if title_lower.contains("4 hour") || ticker_lower.contains("-4h") {
        return "4h".to_string();
    }
    if title_lower.contains("daily")
        || title_lower.contains("24 hour")
        || title_lower.contains("end of day")
    {
        return "daily".to_string();
    }
    if title_lower.contains("weekly") {
        return "weekly".to_string();
    }

    // Parse from ticker pattern like KXBTC-25JAN13-T100000
    // The date part indicates resolution time
    // If it's the same day, it's likely intraday
    if ticker_lower.contains("-t") {
        // Contains price target, likely short-term
        return "intraday".to_string();
    }

    "unknown".to_string()
}

/// Classify Kalshi market type from title and subtitle.
fn classify_kalshi_market(
    title: &str,
    subtitle: &Option<String>,
) -> (KalshiMarketType, Option<String>) {
    let title_lower = title.to_lowercase();
    let subtitle_lower = subtitle
        .as_ref()
        .map(|s| s.to_lowercase())
        .unwrap_or_default();

    // Check for above/below pattern
    if title_lower.contains("above") || subtitle_lower.contains("above") {
        return (KalshiMarketType::AboveBelow, Some("above".to_string()));
    }
    if title_lower.contains("below") || subtitle_lower.contains("below") {
        return (KalshiMarketType::AboveBelow, Some("below".to_string()));
    }

    // Check for range pattern
    if title_lower.contains("between") || title_lower.contains("range") {
        return (KalshiMarketType::Range, None);
    }

    (KalshiMarketType::Unknown, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_asset_from_kalshi() {
        assert_eq!(
            extract_asset_from_kalshi("KXBTC-25JAN13-T100000", "Bitcoin above $100,000?"),
            "BTC"
        );
        assert_eq!(
            extract_asset_from_kalshi("KXETH-25JAN13-T4000", "Ethereum above $4,000?"),
            "ETH"
        );
        assert_eq!(
            extract_asset_from_kalshi("INXSOL-25JAN13", "Solana price target"),
            "SOL"
        );
        assert_eq!(
            extract_asset_from_kalshi("RANDOM-TICKER", "Some random market"),
            "UNKNOWN"
        );
    }

    #[test]
    fn test_extract_timeframe_from_kalshi() {
        assert_eq!(
            extract_timeframe_from_kalshi("KXBTC", "Bitcoin 15 minute price"),
            "15m"
        );
        assert_eq!(
            extract_timeframe_from_kalshi("KXBTC-1H", "Bitcoin 1 hour prediction"),
            "1h"
        );
        assert_eq!(
            extract_timeframe_from_kalshi("KXBTC", "Bitcoin daily close above"),
            "daily"
        );
    }

    #[test]
    fn test_classify_kalshi_market() {
        let (t, d) = classify_kalshi_market("Bitcoin above $100k?", &None);
        assert_eq!(t, KalshiMarketType::AboveBelow);
        assert_eq!(d, Some("above".to_string()));

        let (t, d) = classify_kalshi_market("BTC below $90k?", &None);
        assert_eq!(t, KalshiMarketType::AboveBelow);
        assert_eq!(d, Some("below".to_string()));

        let (t, d) = classify_kalshi_market("BTC between $95k and $100k", &None);
        assert_eq!(t, KalshiMarketType::Range);
        assert_eq!(d, None);
    }

    #[test]
    fn test_kalshi_market_price_conversion() {
        let market = KalshiMarket {
            ticker: "TEST".to_string(),
            event_ticker: "TEST".to_string(),
            title: "Test".to_string(),
            subtitle: None,
            status: "open".to_string(),
            close_time: None,
            expiration_time: None,
            yes_bid: Some(55), // 55 cents
            yes_ask: Some(60), // 60 cents
            no_bid: Some(40),  // 40 cents
            no_ask: Some(45),  // 45 cents
            last_price: Some(57),
            volume: None,
            volume_24h: None,
            open_interest: None,
            liquidity: Some(1000),
            liquidity_dollars: None,
            category: None,
            rules_primary: None,
            strike_price: None,
            floor_strike: None,
            cap_strike: None,
            strike_type: None,
            market_type: None,
        };

        assert_eq!(market.yes_bid_dollars(), Some(Decimal::new(55, 2))); // 0.55
        assert_eq!(market.yes_ask_dollars(), Some(Decimal::new(60, 2))); // 0.60
        assert_eq!(market.no_bid_dollars(), Some(Decimal::new(40, 2))); // 0.40
        assert_eq!(market.no_ask_dollars(), Some(Decimal::new(45, 2))); // 0.45
        assert_eq!(market.get_liquidity_dollars(), Some(Decimal::new(1000, 2)));
        // $10.00 (liquidity is in cents)
    }
}
