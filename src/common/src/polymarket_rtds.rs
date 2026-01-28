//! Polymarket RTDS (Real-Time Data Service) WebSocket client for Chainlink prices.
//!
//! The RTDS provides real-time Chainlink oracle prices that Polymarket 15-minute
//! Up/Down markets likely settle on. These prices differ from Binance by ~$100-120
//! and update ~1/sec (vs Binance's 150+/sec).
//!
//! Key differences from Binance:
//! - Update rate: ~1/sec (vs ~150/sec for Binance)
//! - Latency: ~800ms (vs ~10ms for Binance)
//! - No klines available - must synthesize open price from first price at market start

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

/// Polymarket RTDS WebSocket URL.
pub const POLYMARKET_RTDS_URL: &str = "wss://ws-live-data.polymarket.com";

/// Map asset name to Chainlink symbol format.
pub fn asset_to_chainlink_symbol(asset: &str) -> Option<&'static str> {
    match asset.to_uppercase().as_str() {
        "BTC" => Some("btc/usd"),
        "ETH" => Some("eth/usd"),
        "SOL" => Some("sol/usd"),
        "XRP" => Some("xrp/usd"),
        _ => {
            warn!("Unsupported Chainlink asset: {}, skipping", asset);
            None
        }
    }
}

/// Map Chainlink symbol back to asset name.
pub fn chainlink_symbol_to_asset(symbol: &str) -> Option<&'static str> {
    match symbol.to_lowercase().as_str() {
        "btc/usd" => Some("BTC"),
        "eth/usd" => Some("ETH"),
        "sol/usd" => Some("SOL"),
        "xrp/usd" => Some("XRP"),
        _ => None,
    }
}

/// Subscription message for RTDS WebSocket.
#[derive(Debug, Serialize)]
struct SubscriptionMessage {
    action: String,
    subscriptions: Vec<Subscription>,
}

#[derive(Debug, Serialize)]
struct Subscription {
    topic: String,
    #[serde(rename = "type")]
    msg_type: String,
    filters: String,
}

/// Filter for Chainlink price subscription.
#[derive(Debug, Serialize)]
struct ChainlinkFilter {
    symbol: String,
}

/// Raw RTDS message wrapper.
#[derive(Debug, Deserialize)]
struct RtdsMessage {
    topic: String,
    #[serde(rename = "type")]
    msg_type: String,
    #[allow(dead_code)]
    timestamp: i64,
    payload: serde_json::Value,
}

/// Chainlink price update from RTDS.
#[derive(Debug, Clone)]
pub struct ChainlinkPrice {
    pub symbol: String,
    pub value: Decimal,
    pub timestamp: DateTime<Utc>,
}

/// Raw Chainlink price payload.
#[derive(Debug, Deserialize)]
struct ChainlinkPricePayload {
    symbol: String,
    timestamp: i64,
    value: f64,
}

/// Polymarket RTDS WebSocket client.
pub struct PolymarketRtdsClient {
    symbols: Vec<String>,
    reconnect_delay: Duration,
    max_reconnect_delay: Duration,
}

impl PolymarketRtdsClient {
    /// Create a new RTDS client.
    ///
    /// # Arguments
    /// * `symbols` - Chainlink symbols to subscribe to (e.g., ["btc/usd", "eth/usd"])
    pub fn new(symbols: Vec<String>) -> Self {
        Self {
            symbols,
            reconnect_delay: Duration::from_secs(1),
            max_reconnect_delay: Duration::from_secs(30),
        }
    }

    /// Connect to RTDS WebSocket with retry.
    pub async fn connect_with_retry(&self, max_retries: u32) -> anyhow::Result<RtdsStream> {
        let mut delay = self.reconnect_delay;
        let mut attempts = 0;

        loop {
            match self.connect().await {
                Ok(stream) => return Ok(stream),
                Err(e) => {
                    attempts += 1;
                    if attempts >= max_retries {
                        return Err(anyhow::anyhow!(
                            "Failed to connect to RTDS after {} attempts: {}",
                            max_retries,
                            e
                        ));
                    }

                    warn!(
                        "RTDS connection attempt {} failed: {}. Retrying in {:?}...",
                        attempts, e, delay
                    );
                    tokio::time::sleep(delay).await;
                    delay = std::cmp::min(delay * 2, self.max_reconnect_delay);
                }
            }
        }
    }

    /// Connect to RTDS WebSocket.
    async fn connect(&self) -> anyhow::Result<RtdsStream> {
        info!("Connecting to Polymarket RTDS: {}", POLYMARKET_RTDS_URL);

        let (ws_stream, _) = connect_async(POLYMARKET_RTDS_URL).await?;
        info!("Connected to RTDS WebSocket");

        let (mut write, read) = ws_stream.split();

        // Subscribe to each symbol
        for symbol in &self.symbols {
            let filter = serde_json::to_string(&ChainlinkFilter {
                symbol: symbol.clone(),
            })?;

            let sub_msg = SubscriptionMessage {
                action: "subscribe".to_string(),
                subscriptions: vec![Subscription {
                    topic: "crypto_prices_chainlink".to_string(),
                    msg_type: "*".to_string(),
                    filters: filter,
                }],
            };

            let msg_json = serde_json::to_string(&sub_msg)?;
            debug!("Subscribing to RTDS: {}", msg_json);
            write.send(Message::Text(msg_json.into())).await?;
        }

        info!(
            "Subscribed to {} Chainlink symbols: {:?}",
            self.symbols.len(),
            self.symbols
        );

        Ok(RtdsStream {
            ws_stream: read,
            _write: write,
            symbols: self.symbols.clone(),
        })
    }

    /// Get the list of subscribed symbols.
    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }
}

/// Active RTDS WebSocket stream.
pub struct RtdsStream {
    ws_stream: futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
    #[allow(dead_code)]
    _write: futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
    symbols: Vec<String>,
}

impl RtdsStream {
    /// Get the next price update from the stream.
    /// Returns None if the connection is closed.
    pub async fn next_price(&mut self) -> Option<ChainlinkPrice> {
        // 15 second timeout - Chainlink updates ~1/sec, so 15s without data indicates problems
        let receive_timeout = Duration::from_secs(15);

        loop {
            let msg_result = timeout(receive_timeout, self.ws_stream.next()).await;

            match msg_result {
                Ok(Some(Ok(Message::Text(text)))) => {
                    if let Some(price) = self.parse_message(&text) {
                        return Some(price);
                    }
                    // Non-price message, continue
                }
                Ok(Some(Ok(Message::Ping(data)))) => {
                    debug!("Received RTDS ping");
                    // Pong is handled automatically by tungstenite
                    let _ = data;
                }
                Ok(Some(Ok(Message::Pong(_)))) => {
                    debug!("Received RTDS pong");
                }
                Ok(Some(Ok(Message::Close(_)))) => {
                    warn!("RTDS WebSocket closed by server");
                    return None;
                }
                Ok(Some(Ok(_))) => {
                    // Binary or other message, ignore
                }
                Ok(Some(Err(e))) => {
                    error!("RTDS WebSocket error: {}", e);
                    return None;
                }
                Ok(None) => {
                    warn!("RTDS WebSocket stream ended");
                    return None;
                }
                Err(_) => {
                    // Timeout after 15s - Chainlink should update every ~1s
                    warn!("RTDS receive timeout (15s) - stream may be stalled");
                    return None; // Force reconnect instead of silently continuing
                }
            }
        }
    }

    /// Parse an RTDS message into a ChainlinkPrice.
    fn parse_message(&self, text: &str) -> Option<ChainlinkPrice> {
        let msg: RtdsMessage = match serde_json::from_str(text) {
            Ok(m) => m,
            Err(e) => {
                debug!("Failed to parse RTDS message: {} - {}", e, text);
                return None;
            }
        };

        // Only process crypto_prices_chainlink updates
        if msg.topic != "crypto_prices_chainlink" || msg.msg_type != "update" {
            return None;
        }

        let payload: ChainlinkPricePayload = match serde_json::from_value(msg.payload) {
            Ok(p) => p,
            Err(e) => {
                debug!("Failed to parse Chainlink payload: {}", e);
                return None;
            }
        };

        // Convert f64 to Decimal
        let value = match Decimal::try_from(payload.value) {
            Ok(d) => d,
            Err(e) => {
                warn!("Failed to convert price to Decimal: {}", e);
                return None;
            }
        };

        // Use actual Chainlink timestamp from payload (milliseconds)
        let timestamp = Utc
            .timestamp_millis_opt(payload.timestamp)
            .single()
            .unwrap_or_else(Utc::now);

        Some(ChainlinkPrice {
            symbol: payload.symbol,
            value,
            timestamp,
        })
    }

    /// Get the list of subscribed symbols.
    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }
}

/// Buffer for storing Chainlink prices and synthesizing open prices.
///
/// Unlike Binance which provides klines, Chainlink only provides point-in-time
/// prices. We capture the first price received at or after market start time
/// as the "open price" for that market.
#[derive(Debug)]
pub struct ChainlinkPriceBuffer {
    /// Current latest price per symbol
    latest_prices: HashMap<String, Decimal>,
    /// Captured open prices keyed by (symbol, market_start_rounded_to_minute)
    /// The key uses minute precision to handle slight timing differences
    open_prices: HashMap<(String, i64), Decimal>,
    /// Historical prices for debugging/analysis (ring buffer per symbol)
    history: HashMap<String, VecDeque<TimestampedPrice>>,
    /// Maximum history entries per symbol
    max_history: usize,
}

/// A timestamped price entry.
#[derive(Debug, Clone)]
pub struct TimestampedPrice {
    pub value: Decimal,
    pub timestamp: DateTime<Utc>,
}

impl ChainlinkPriceBuffer {
    /// Create a new Chainlink price buffer.
    ///
    /// # Arguments
    /// * `max_history` - Maximum price entries to keep per symbol (for debugging)
    pub fn new(max_history: usize) -> Self {
        Self {
            latest_prices: HashMap::new(),
            open_prices: HashMap::new(),
            history: HashMap::new(),
            max_history,
        }
    }

    /// Update the buffer with a new price.
    pub fn update(&mut self, price: &ChainlinkPrice) {
        // Update latest price
        self.latest_prices.insert(price.symbol.clone(), price.value);

        // Add to history
        let history = self
            .history
            .entry(price.symbol.clone())
            .or_insert_with(|| VecDeque::with_capacity(self.max_history + 1));

        history.push_back(TimestampedPrice {
            value: price.value,
            timestamp: price.timestamp,
        });

        // Trim history
        while history.len() > self.max_history {
            history.pop_front();
        }
    }

    /// Get the latest price for a symbol.
    pub fn get_latest(&self, symbol: &str) -> Option<Decimal> {
        self.latest_prices.get(symbol).copied()
    }

    /// Get or capture the open price for a market start time.
    ///
    /// If we don't have a captured open price for this (symbol, start_time),
    /// we capture the current latest price as the open price.
    ///
    /// This implements "first price at or after market start" semantics,
    /// which is appropriate since Chainlink prices update ~1/sec and we
    /// discover markets shortly after they start.
    ///
    /// Returns None if we don't have any price for this symbol yet.
    pub fn get_or_capture_open(
        &mut self,
        symbol: &str,
        start_time: DateTime<Utc>,
    ) -> Option<Decimal> {
        // Round to minute for key (handles slight timing differences)
        let minute_key = start_time.timestamp() / 60;
        let key = (symbol.to_string(), minute_key);

        // Return existing captured open if we have it
        if let Some(&open) = self.open_prices.get(&key) {
            return Some(open);
        }

        // Capture current price as open
        if let Some(&current) = self.latest_prices.get(symbol) {
            self.open_prices.insert(key, current);
            return Some(current);
        }

        // No price available yet
        None
    }

    /// Check if we have a captured open price for a market.
    pub fn has_open(&self, symbol: &str, start_time: DateTime<Utc>) -> bool {
        let minute_key = start_time.timestamp() / 60;
        let key = (symbol.to_string(), minute_key);
        self.open_prices.contains_key(&key)
    }

    /// Get the captured open price without capturing a new one.
    pub fn get_open(&self, symbol: &str, start_time: DateTime<Utc>) -> Option<Decimal> {
        let minute_key = start_time.timestamp() / 60;
        let key = (symbol.to_string(), minute_key);
        self.open_prices.get(&key).copied()
    }

    /// Clear old open prices to prevent memory growth.
    /// Removes entries older than the given cutoff time.
    pub fn cleanup_old_opens(&mut self, cutoff: DateTime<Utc>) {
        let cutoff_minute = cutoff.timestamp() / 60;
        self.open_prices
            .retain(|(_, minute), _| *minute >= cutoff_minute);
    }

    /// Get the number of tracked symbols.
    pub fn symbol_count(&self) -> usize {
        self.latest_prices.len()
    }

    /// Check if we have prices for all the given symbols.
    /// Used to verify the buffer is bootstrapped before trading.
    pub fn has_prices_for_all(&self, symbols: &[String]) -> bool {
        symbols.iter().all(|s| self.latest_prices.contains_key(s))
    }

    /// Check if we have at least one price.
    pub fn has_any_prices(&self) -> bool {
        !self.latest_prices.is_empty()
    }

    /// Get history for a symbol.
    pub fn get_history(&self, symbol: &str) -> Option<&VecDeque<TimestampedPrice>> {
        self.history.get(symbol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_asset_to_chainlink_symbol() {
        assert_eq!(asset_to_chainlink_symbol("BTC"), Some("btc/usd"));
        assert_eq!(asset_to_chainlink_symbol("btc"), Some("btc/usd"));
        assert_eq!(asset_to_chainlink_symbol("ETH"), Some("eth/usd"));
        assert_eq!(asset_to_chainlink_symbol("SOL"), Some("sol/usd"));
        assert_eq!(asset_to_chainlink_symbol("XRP"), Some("xrp/usd"));
        assert_eq!(asset_to_chainlink_symbol("DOGE"), None);
    }

    #[test]
    fn test_chainlink_symbol_to_asset() {
        assert_eq!(chainlink_symbol_to_asset("btc/usd"), Some("BTC"));
        assert_eq!(chainlink_symbol_to_asset("BTC/USD"), Some("BTC"));
        assert_eq!(chainlink_symbol_to_asset("eth/usd"), Some("ETH"));
        assert_eq!(chainlink_symbol_to_asset("unknown"), None);
    }

    #[test]
    fn test_price_buffer_basics() {
        let mut buffer = ChainlinkPriceBuffer::new(100);

        let price = ChainlinkPrice {
            symbol: "btc/usd".to_string(),
            value: dec!(87500.50),
            timestamp: Utc::now(),
        };

        buffer.update(&price);

        assert_eq!(buffer.get_latest("btc/usd"), Some(dec!(87500.50)));
        assert_eq!(buffer.get_latest("eth/usd"), None);
    }

    #[test]
    fn test_open_price_capture() {
        let mut buffer = ChainlinkPriceBuffer::new(100);

        // Add a price
        let price = ChainlinkPrice {
            symbol: "btc/usd".to_string(),
            value: dec!(87500),
            timestamp: Utc::now(),
        };
        buffer.update(&price);

        // Capture open price
        let start_time = Utc::now();
        let open = buffer.get_or_capture_open("btc/usd", start_time);
        assert_eq!(open, Some(dec!(87500)));

        // Update price
        let price2 = ChainlinkPrice {
            symbol: "btc/usd".to_string(),
            value: dec!(88000),
            timestamp: Utc::now(),
        };
        buffer.update(&price2);

        // Open price should still be the captured value
        let open2 = buffer.get_or_capture_open("btc/usd", start_time);
        assert_eq!(open2, Some(dec!(87500)));

        // Latest should be updated
        assert_eq!(buffer.get_latest("btc/usd"), Some(dec!(88000)));
    }

    #[test]
    fn test_no_price_returns_none() {
        let mut buffer = ChainlinkPriceBuffer::new(100);
        let start_time = Utc::now();

        // No price captured yet
        assert!(buffer.get_or_capture_open("btc/usd", start_time).is_none());
    }
}
