//! Gamma API client for fetching Polymarket markets.
//!
//! Uses the /events endpoint to fetch crypto Up/Down markets by series_id.
//! See: https://docs.polymarket.com/quickstart/fetching-data

use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info};

use crate::Config;

#[derive(Debug, Error)]
pub enum GammaError {
    #[error("HTTP request failed: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("Failed to parse response: {0}")]
    ParseError(String),

    #[error("API error: {0}")]
    ApiError(String),
}

/// Known crypto series IDs for Up/Down markets (15m+ only, 5m excluded)
pub const CRYPTO_SERIES: &[(&str, &str)] = &[
    // BTC Up or Down
    ("10192", "BTC Up or Down 15m"),
    ("10114", "BTC Up or Down 1h"),
    // ETH Up or Down
    ("10191", "ETH Up or Down 15m"),
    ("10117", "ETH Up or Down 1h"),
    // SOL Up or Down
    ("10423", "SOL Up or Down 15m"),
    ("10122", "SOL Up or Down 1h"),
    // XRP Up or Down
    ("10422", "XRP Up or Down 15m"),
    ("10123", "XRP Up or Down 1h"),
];

/// Market type classification for filtering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketType {
    UpDown,
    Above,
    PriceRange,
    Unknown,
}

impl MarketType {
    /// Parse market type from market name/description and outcomes.
    pub fn from_market_name(name: &str) -> Self {
        let name_lower = name.to_lowercase();

        if name_lower.contains("up or down") || name_lower.contains("higher or lower") {
            MarketType::UpDown
        } else if name_lower.contains("above") || name_lower.contains("below") {
            MarketType::Above
        } else if name_lower.contains("between") || name_lower.contains("range") {
            MarketType::PriceRange
        } else {
            MarketType::Unknown
        }
    }

    /// Parse market type from outcomes (e.g., ["Up", "Down"])
    pub fn from_outcomes(outcomes: &[String]) -> Self {
        if outcomes.len() == 2 {
            let o0 = outcomes[0].to_lowercase();
            let o1 = outcomes[1].to_lowercase();
            if (o0 == "up" && o1 == "down") || (o0 == "down" && o1 == "up") {
                return MarketType::UpDown;
            }
            if o0.contains("above") || o0.contains("below") || o1.contains("above") || o1.contains("below") {
                return MarketType::Above;
            }
        }
        MarketType::Unknown
    }

    /// Check if this market type is supported for trading.
    pub fn is_supported(&self) -> bool {
        matches!(self, MarketType::UpDown | MarketType::Above | MarketType::PriceRange)
    }
}

/// Raw event data from Gamma API /events endpoint.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GammaEvent {
    pub id: String,
    pub slug: String,
    pub title: String,
    pub active: Option<bool>,
    pub closed: Option<bool>,
    pub restricted: Option<bool>,
    #[serde(rename = "endDate")]
    pub end_date: Option<String>,
    /// Nested markets within this event
    #[serde(default)]
    pub markets: Vec<GammaMarket>,
}

/// Raw market data from Gamma API (nested in events).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GammaMarket {
    #[serde(rename = "conditionId")]
    pub condition_id: String,
    pub question: String,
    pub description: Option<String>,
    #[serde(rename = "endDate")]
    pub end_date: Option<String>,
    /// JSON string of array: "[\"Up\", \"Down\"]" or "[\"Yes\", \"No\"]"
    pub outcomes: Option<String>,
    /// JSON string of array: "[\"0.55\", \"0.45\"]"
    #[serde(rename = "outcomePrices")]
    pub outcome_prices: Option<String>,
    /// JSON string of array with token IDs
    #[serde(rename = "clobTokenIds")]
    pub clob_token_ids: Option<String>,
    pub active: Option<bool>,
    pub closed: Option<bool>,
    pub slug: Option<String>,
    #[serde(rename = "marketMakerAddress")]
    pub market_maker_address: Option<String>,
    /// Best bid price
    #[serde(rename = "bestBid")]
    pub best_bid: Option<f64>,
    /// Best ask price
    #[serde(rename = "bestAsk")]
    pub best_ask: Option<f64>,
}

impl GammaMarket {
    /// Parse the clob_token_ids JSON string into a vector
    pub fn parse_token_ids(&self) -> Option<Vec<String>> {
        self.clob_token_ids.as_ref()
            .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
    }

    /// Parse outcomes JSON string
    pub fn parse_outcomes(&self) -> Option<Vec<String>> {
        self.outcomes.as_ref()
            .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
    }
}

/// Parsed market ready for database insertion.
#[derive(Debug, Clone)]
pub struct ParsedMarket {
    pub condition_id: String,
    pub market_type: MarketType,
    pub asset: String,
    pub timeframe: String,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub name: String,
    pub end_time: DateTime<Utc>,
    /// Best bid for YES outcome (from Gamma API)
    pub yes_best_bid: Option<rust_decimal::Decimal>,
    /// Best ask for YES outcome (from Gamma API)
    pub yes_best_ask: Option<rust_decimal::Decimal>,
    /// Best bid for NO outcome (calculated as 1 - yes_best_ask)
    pub no_best_bid: Option<rust_decimal::Decimal>,
    /// Best ask for NO outcome (calculated as 1 - yes_best_bid)
    pub no_best_ask: Option<rust_decimal::Decimal>,
}

/// Gamma API client.
pub struct GammaClient {
    client: Client,
    base_url: String,
}

impl GammaClient {
    /// Create a new Gamma API client.
    pub fn new(config: &Config) -> Self {
        Self {
            client: Client::new(),
            base_url: config.gamma_api_url.clone(),
        }
    }

    /// Fetch active events for a specific series from the Gamma API.
    pub async fn fetch_events_by_series(&self, series_id: &str) -> Result<Vec<GammaEvent>, GammaError> {
        let url = format!("{}/events", self.base_url);

        debug!("Fetching events for series_id={}", series_id);

        let response = self.client
            .get(&url)
            .query(&[
                ("series_id", series_id),
                ("active", "true"),
                ("closed", "false"),
                ("limit", "100"),
            ])
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(GammaError::ApiError(format!(
                "API returned status: {}",
                response.status()
            )));
        }

        let events: Vec<GammaEvent> = response.json().await?;
        debug!("Fetched {} events for series {}", events.len(), series_id);
        Ok(events)
    }

    /// Fetch all active crypto markets from known series.
    pub async fn fetch_markets(&self) -> Result<Vec<GammaMarket>, GammaError> {
        let mut all_markets = Vec::new();

        for (series_id, series_name) in CRYPTO_SERIES {
            match self.fetch_events_by_series(series_id).await {
                Ok(events) => {
                    for event in events {
                        // Extract nested markets from each event
                        for market in event.markets {
                            // Only include active, non-closed markets
                            if market.active.unwrap_or(false) && !market.closed.unwrap_or(true) {
                                all_markets.push(market);
                            }
                        }
                    }
                    info!("Series '{}': found {} total markets so far", series_name, all_markets.len());
                }
                Err(e) => {
                    debug!("Failed to fetch series {}: {}", series_name, e);
                    // Continue with other series
                }
            }
        }

        info!("Total markets fetched from all series: {}", all_markets.len());
        Ok(all_markets)
    }

    /// Fetch and filter markets for supported types.
    pub async fn fetch_supported_markets(&self) -> Result<Vec<ParsedMarket>, GammaError> {
        let markets = self.fetch_markets().await?;

        let parsed: Vec<ParsedMarket> = markets
            .into_iter()
            .filter_map(|m| self.parse_market(m))
            .filter(|m| m.market_type.is_supported())
            .collect();

        Ok(parsed)
    }

    /// Parse a raw market into our format.
    fn parse_market(&self, market: GammaMarket) -> Option<ParsedMarket> {
        // Parse end time - Gamma uses ISO format like "2025-12-31T12:00:00Z"
        let end_time = market.end_date.as_ref()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc))?;

        // Skip expired markets
        if end_time < Utc::now() {
            debug!("Skipping expired market: {}", market.question);
            return None;
        }

        // Parse token IDs - should be array of 2 strings
        let token_ids = market.parse_token_ids()?;
        if token_ids.len() != 2 {
            debug!("Skipping market with {} token IDs: {}", token_ids.len(), market.question);
            return None;
        }

        // Parse outcomes to determine YES/NO or UP/DOWN mapping
        let outcomes = market.parse_outcomes()?;
        if outcomes.len() != 2 {
            debug!("Skipping market with {} outcomes: {}", outcomes.len(), market.question);
            return None;
        }

        // Find UP/YES and DOWN/NO token indices
        // For Up/Down markets: "Up" is like "Yes" (positive outcome)
        let (yes_idx, no_idx) = {
            let yes_pos = outcomes.iter().position(|o| {
                let lower = o.to_lowercase();
                lower == "yes" || lower == "up" || lower.contains("higher") || lower.contains("above")
            });
            let no_pos = outcomes.iter().position(|o| {
                let lower = o.to_lowercase();
                lower == "no" || lower == "down" || lower.contains("lower") || lower.contains("below")
            });
            match (yes_pos, no_pos) {
                (Some(y), Some(n)) => (y, n),
                _ => (0, 1), // Default: first is Yes/Up, second is No/Down
            }
        };

        // Determine market type - try outcomes first, then question text
        let mut market_type = MarketType::from_outcomes(&outcomes);
        if market_type == MarketType::Unknown {
            market_type = MarketType::from_market_name(&market.question);
        }

        // Extract asset from question (e.g., "BTC", "ETH")
        let asset = extract_asset(&market.question);

        // Extract timeframe if present
        let timeframe = extract_timeframe(&market.question);

        // Extract prices from Gamma API
        // best_bid/best_ask are for the YES (Up) outcome
        let yes_best_bid = market.best_bid.and_then(|p| rust_decimal::Decimal::try_from(p).ok());
        let yes_best_ask = market.best_ask.and_then(|p| rust_decimal::Decimal::try_from(p).ok());

        // Calculate NO prices: NO_bid = 1 - YES_ask, NO_ask = 1 - YES_bid
        let one = rust_decimal::Decimal::ONE;
        let no_best_bid = yes_best_ask.map(|ask| one - ask);
        let no_best_ask = yes_best_bid.map(|bid| one - bid);

        debug!(
            "Parsed market: {} | type={:?} | asset={} | timeframe={} | ends={} | yes_bid={:?} | yes_ask={:?}",
            market.question, market_type, asset, timeframe, end_time, yes_best_bid, yes_best_ask
        );

        Some(ParsedMarket {
            condition_id: market.condition_id,
            market_type,
            asset,
            timeframe,
            yes_token_id: token_ids[yes_idx].clone(),
            no_token_id: token_ids[no_idx].clone(),
            name: market.question,
            end_time,
            yes_best_bid,
            yes_best_ask,
            no_best_bid,
            no_best_ask,
        })
    }
}

/// Extract cryptocurrency asset from market question.
fn extract_asset(question: &str) -> String {
    let question_upper = question.to_uppercase();

    let assets = ["BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "MATIC", "DOT", "LINK"];

    for asset in assets {
        if question_upper.contains(asset) {
            return asset.to_string();
        }
    }

    // Check for full names
    if question_upper.contains("BITCOIN") {
        return "BTC".to_string();
    }
    if question_upper.contains("ETHEREUM") {
        return "ETH".to_string();
    }
    if question_upper.contains("SOLANA") {
        return "SOL".to_string();
    }

    "UNKNOWN".to_string()
}

/// Extract timeframe from market question.
fn extract_timeframe(question: &str) -> String {
    let question_lower = question.to_lowercase();

    // Look for common timeframe patterns
    if question_lower.contains("1 hour") || question_lower.contains("1h") {
        return "1h".to_string();
    }
    if question_lower.contains("4 hour") || question_lower.contains("4h") {
        return "4h".to_string();
    }
    if question_lower.contains("daily") || question_lower.contains("24h") {
        return "daily".to_string();
    }
    if question_lower.contains("weekly") {
        return "weekly".to_string();
    }

    // Try to extract date-based timeframe
    "unknown".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_market_type_from_name() {
        assert_eq!(
            MarketType::from_market_name("Will BTC go up or down in the next hour?"),
            MarketType::UpDown
        );
        assert_eq!(
            MarketType::from_market_name("Will ETH be above $4000?"),
            MarketType::Above
        );
        assert_eq!(
            MarketType::from_market_name("Will SOL be between $100 and $150?"),
            MarketType::PriceRange
        );
        assert_eq!(
            MarketType::from_market_name("Random question"),
            MarketType::Unknown
        );
    }

    #[test]
    fn test_market_type_is_supported() {
        assert!(MarketType::UpDown.is_supported());
        assert!(MarketType::Above.is_supported());
        assert!(MarketType::PriceRange.is_supported());
        assert!(!MarketType::Unknown.is_supported());
    }

    #[test]
    fn test_extract_asset() {
        assert_eq!(extract_asset("Will BTC go up?"), "BTC");
        assert_eq!(extract_asset("Ethereum price prediction"), "ETH");
        assert_eq!(extract_asset("SOL/USD above 100?"), "SOL");
        assert_eq!(extract_asset("Some random market"), "UNKNOWN");
    }

    #[test]
    fn test_extract_timeframe() {
        assert_eq!(extract_timeframe("BTC in the next 1 hour"), "1h");
        assert_eq!(extract_timeframe("ETH 4 hour prediction"), "4h");
        assert_eq!(extract_timeframe("Daily BTC movement"), "daily");
        assert_eq!(extract_timeframe("Weekly ETH outlook"), "weekly");
    }

    #[test]
    fn test_gamma_market_parse_token_ids() {
        let market = GammaMarket {
            condition_id: "test".to_string(),
            question: "Test?".to_string(),
            description: None,
            end_date: None,
            outcomes: Some(r#"["Yes", "No"]"#.to_string()),
            outcome_prices: None,
            clob_token_ids: Some(r#"["token1", "token2"]"#.to_string()),
            active: Some(true),
            closed: None,
            slug: None,
            market_maker_address: None,
            best_bid: None,
            best_ask: None,
        };

        let tokens = market.parse_token_ids().unwrap();
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0], "token1");
        assert_eq!(tokens[1], "token2");
    }
}
