//! Gamma API client for fetching Polymarket markets.
//!
//! Uses the /events endpoint to fetch crypto Up/Down markets by series_id.
//! See: https://docs.polymarket.com/quickstart/fetching-data

use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info, warn};

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

/// Known crypto series IDs for Up/Down markets (all timeframes)
pub const CRYPTO_SERIES: &[(&str, &str)] = &[
    // BTC Up or Down
    ("10193", "BTC Up or Down 5m"),
    ("10192", "BTC Up or Down 15m"),
    ("10114", "BTC Up or Down 1h"),
    ("10194", "BTC Up or Down 4h"),
    ("10115", "BTC Up or Down Daily"),
    // ETH Up or Down
    ("10190", "ETH Up or Down 5m"),
    ("10191", "ETH Up or Down 15m"),
    ("10117", "ETH Up or Down 1h"),
    ("10195", "ETH Up or Down 4h"),
    ("10118", "ETH Up or Down Daily"),
    // SOL Up or Down
    ("10424", "SOL Up or Down 5m"),
    ("10423", "SOL Up or Down 15m"),
    ("10122", "SOL Up or Down 1h"),
    ("10425", "SOL Up or Down 4h"),
    ("10121", "SOL Up or Down Daily"),
    // XRP Up or Down
    ("10421", "XRP Up or Down 5m"),
    ("10422", "XRP Up or Down 15m"),
    ("10123", "XRP Up or Down 1h"),
    ("10426", "XRP Up or Down 4h"),
    ("10124", "XRP Up or Down Daily"),
    // DOGE Up or Down
    ("10500", "DOGE Up or Down 15m"),
    ("10501", "DOGE Up or Down 1h"),
    // ADA Up or Down
    ("10502", "ADA Up or Down 15m"),
    ("10503", "ADA Up or Down 1h"),
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
            if o0.contains("above")
                || o0.contains("below")
                || o1.contains("above")
                || o1.contains("below")
            {
                return MarketType::Above;
            }
        }
        MarketType::Unknown
    }

    /// Check if this market type is supported for trading.
    pub fn is_supported(&self) -> bool {
        matches!(
            self,
            MarketType::UpDown | MarketType::Above | MarketType::PriceRange
        )
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
        self.clob_token_ids
            .as_ref()
            .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
    }

    /// Parse outcomes JSON string
    pub fn parse_outcomes(&self) -> Option<Vec<String>> {
        self.outcomes
            .as_ref()
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
    pub async fn fetch_events_by_series(
        &self,
        series_id: &str,
    ) -> Result<Vec<GammaEvent>, GammaError> {
        let url = format!("{}/events", self.base_url);

        debug!("Fetching events for series_id={}", series_id);

        let response = self
            .client
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

    /// Fetch all open events (paginated, not limited to series).
    pub async fn fetch_all_open_events(&self) -> Result<Vec<GammaEvent>, GammaError> {
        let mut all_events = Vec::new();
        let mut offset = 0;
        let max_events = 5000;

        while offset < max_events {
            let url = format!("{}/events", self.base_url);

            let response = self
                .client
                .get(&url)
                .query(&[
                    ("closed", "false"),
                    ("limit", "500"),
                    ("offset", &offset.to_string()),
                ])
                .send()
                .await?;

            if !response.status().is_success() {
                break;
            }

            let events: Vec<GammaEvent> = response.json().await?;
            if events.is_empty() {
                break;
            }

            let count = events.len();
            all_events.extend(events);
            offset += 500;

            if count < 500 {
                break;
            }
        }

        info!("Fetched {} total open events", all_events.len());
        Ok(all_events)
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
                    info!(
                        "Series '{}': found {} total markets so far",
                        series_name,
                        all_markets.len()
                    );
                }
                Err(e) => {
                    warn!("Failed to fetch series {}: {}", series_name, e);
                    // Continue with other series
                }
            }
        }

        info!(
            "Total markets fetched from all series: {}",
            all_markets.len()
        );
        Ok(all_markets)
    }

    /// Fetch all binary markets (any two-outcome market).
    pub async fn fetch_all_binary_markets(&self) -> Result<Vec<GammaMarket>, GammaError> {
        let events = self.fetch_all_open_events().await?;
        let mut all_markets = Vec::new();

        for event in events {
            for market in event.markets {
                // Only include active, non-closed markets with exactly 2 outcomes
                if !market.active.unwrap_or(false) || market.closed.unwrap_or(true) {
                    continue;
                }

                // Check for exactly 2 outcomes
                if let Some(outcomes) = market.parse_outcomes() {
                    if outcomes.len() == 2 {
                        // Check for exactly 2 token IDs
                        if let Some(token_ids) = market.parse_token_ids() {
                            if token_ids.len() == 2 {
                                all_markets.push(market);
                            }
                        }
                    }
                }
            }
        }

        info!("Total binary markets fetched: {}", all_markets.len());
        Ok(all_markets)
    }

    /// Fetch and filter markets for supported types.
    /// Combines fetch_all_binary_markets (general events) with fetch_markets (crypto series)
    /// to ensure we get all crypto up/down markets that might be beyond the 5000 event limit.
    pub async fn fetch_supported_markets(&self) -> Result<Vec<ParsedMarket>, GammaError> {
        use std::collections::HashSet;

        // Fetch general binary markets (has 5000 event limit)
        let general_markets = self.fetch_all_binary_markets().await?;

        // Also fetch crypto series markets (queries by series_id, no limit issues)
        let crypto_markets = self.fetch_markets().await?;

        // Combine and deduplicate by condition_id
        let mut seen_conditions: HashSet<String> = HashSet::new();
        let mut all_markets = Vec::new();

        // Add crypto markets first (they're the priority)
        for market in crypto_markets {
            if !market.condition_id.is_empty()
                && seen_conditions.insert(market.condition_id.clone())
            {
                all_markets.push(market);
            }
        }

        // Add general markets (skip duplicates)
        for market in general_markets {
            if !market.condition_id.is_empty()
                && seen_conditions.insert(market.condition_id.clone())
            {
                all_markets.push(market);
            }
        }

        info!("Combined {} total markets (deduped)", all_markets.len());

        let parsed: Vec<ParsedMarket> = all_markets
            .into_iter()
            .filter_map(|m| self.parse_market(m))
            .collect();

        info!("Parsed {} supported markets", parsed.len());
        Ok(parsed)
    }

    /// Parse a raw market into our format.
    fn parse_market(&self, market: GammaMarket) -> Option<ParsedMarket> {
        // Parse end time - Gamma uses ISO format like "2025-12-31T12:00:00Z"
        let end_time = market
            .end_date
            .as_ref()
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
            debug!(
                "Skipping market with {} token IDs: {}",
                token_ids.len(),
                market.question
            );
            return None;
        }

        // Parse outcomes to determine YES/NO or UP/DOWN mapping
        let outcomes = market.parse_outcomes()?;
        if outcomes.len() != 2 {
            debug!(
                "Skipping market with {} outcomes: {}",
                outcomes.len(),
                market.question
            );
            return None;
        }

        // Find UP/YES and DOWN/NO token indices
        // For Up/Down markets: "Up" is like "Yes" (positive outcome)
        let (yes_idx, no_idx) = {
            let yes_pos = outcomes.iter().position(|o| {
                let lower = o.to_lowercase();
                lower == "yes"
                    || lower == "up"
                    || lower.contains("higher")
                    || lower.contains("above")
            });
            let no_pos = outcomes.iter().position(|o| {
                let lower = o.to_lowercase();
                lower == "no"
                    || lower == "down"
                    || lower.contains("lower")
                    || lower.contains("below")
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
        let yes_best_bid = market
            .best_bid
            .and_then(|p| rust_decimal::Decimal::try_from(p).ok());
        let yes_best_ask = market
            .best_ask
            .and_then(|p| rust_decimal::Decimal::try_from(p).ok());

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

    /// Fetch a market by token_id and return its resolution if closed.
    /// Returns (winning_side, outcome_prices) where winning_side is "YES" or "NO".
    ///
    /// Note: Uses the clob_token_ids query parameter since /markets/{id} only accepts
    /// numeric IDs, not condition_id hex values.
    pub async fn fetch_market_resolution(
        &self,
        token_id: &str,
    ) -> Result<Option<String>, GammaError> {
        let url = format!("{}/markets", self.base_url);

        debug!("Fetching market resolution for token_id={}", token_id);

        let response = self
            .client
            .get(&url)
            .query(&[("clob_token_ids", token_id)])
            .send()
            .await?;

        if !response.status().is_success() {
            if response.status().as_u16() == 404 {
                return Ok(None);
            }
            return Err(GammaError::ApiError(format!(
                "API returned status: {}",
                response.status()
            )));
        }

        let markets: Vec<GammaMarket> = response.json().await?;
        let market = match markets.into_iter().next() {
            Some(m) => m,
            None => {
                debug!("No market found for token_id={}", token_id);
                return Ok(None);
            }
        };

        // Check if market is closed/resolved
        if !market.closed.unwrap_or(false) {
            debug!("Market {} is not yet closed", token_id);
            return Ok(None);
        }

        // Try to get resolution from outcome_prices
        // Format: ["1", "0"] means first outcome (YES/Up) won
        // Format: ["0", "1"] means second outcome (NO/Down) won
        if let Some(prices_str) = &market.outcome_prices {
            if let Ok(prices) = serde_json::from_str::<Vec<String>>(prices_str) {
                if prices.len() == 2 {
                    // Parse outcomes to determine which is YES/NO
                    let outcomes = market.parse_outcomes().unwrap_or_default();
                    let (yes_idx, _no_idx) = if outcomes.len() == 2 {
                        let yes_pos = outcomes.iter().position(|o| {
                            let lower = o.to_lowercase();
                            lower == "yes" || lower == "up" || lower == "higher" || lower == "above"
                        });
                        let no_pos = outcomes.iter().position(|o| {
                            let lower = o.to_lowercase();
                            lower == "no" || lower == "down" || lower == "lower" || lower == "below"
                        });
                        (yes_pos.unwrap_or(0), no_pos.unwrap_or(1))
                    } else {
                        (0, 1)
                    };

                    // Check which outcome won (price = "1" means winner)
                    if let (Ok(p0), Ok(p1)) = (prices[0].parse::<f64>(), prices[1].parse::<f64>()) {
                        if p0 > 0.5 {
                            // First outcome won
                            let winning = if yes_idx == 0 { "YES" } else { "NO" };
                            debug!("Market {} resolved to {}", token_id, winning);
                            return Ok(Some(winning.to_string()));
                        } else if p1 > 0.5 {
                            // Second outcome won
                            let winning = if yes_idx == 1 { "YES" } else { "NO" };
                            debug!("Market {} resolved to {}", token_id, winning);
                            return Ok(Some(winning.to_string()));
                        }
                    }
                }
            }
        }

        debug!("Could not determine resolution for market {}", token_id);
        Ok(None)
    }
}

/// Extract cryptocurrency asset from market question.
fn extract_asset(question: &str) -> String {
    let question_upper = question.to_uppercase();

    let assets = [
        "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "MATIC", "DOT", "LINK",
    ];

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

    // Look for common timeframe patterns (order matters - check shorter durations first)

    // 5-minute markets
    if question_lower.contains("5 min")
        || question_lower.contains("5min")
        || question_lower.contains("-5m")
    {
        return "5m".to_string();
    }

    // 15-minute markets
    if question_lower.contains("15 min")
        || question_lower.contains("15min")
        || question_lower.contains("-15m")
    {
        return "15m".to_string();
    }

    // Check for time range pattern (15-minute markets): contains patterns like "pm-" followed by time
    // e.g., "1:30pm-1:45pm" or "2:00pm-2:15pm"
    if (question_lower.contains("pm-") || question_lower.contains("am-"))
        && question_lower.contains(":")
    {
        return "15m".to_string();
    }

    // 4-hour markets
    if question_lower.contains("4 hour") || question_lower.contains("4h") {
        return "4h".to_string();
    }

    // 1-hour markets
    if question_lower.contains("1 hour") || question_lower.contains("1h") {
        return "1h".to_string();
    }

    // Hourly pattern: contains "am et" or "pm et" (single time, not range)
    if (question_lower.contains("am et") || question_lower.contains("pm et"))
        && !question_lower.contains(":")
    {
        return "1h".to_string();
    }

    // Daily markets (month names or "daily")
    let months = [
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    ];
    if months.iter().any(|m| question_lower.contains(m))
        || question_lower.contains("daily")
        || question_lower.contains("24h")
    {
        return "daily".to_string();
    }

    // Weekly
    if question_lower.contains("weekly") {
        return "weekly".to_string();
    }

    // Default to unknown
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
