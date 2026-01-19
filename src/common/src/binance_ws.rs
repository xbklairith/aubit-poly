//! Binance WebSocket client for real-time kline (candlestick) data.
//!
//! Provides streaming price data for momentum detection strategies.

use std::collections::VecDeque;
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

/// Default Binance WebSocket URL for combined streams.
pub const BINANCE_WS_URL: &str = "wss://stream.binance.com:9443/stream";

/// A single kline (candlestick) from Binance.
#[derive(Debug, Clone)]
pub struct BinanceKline {
    pub symbol: String,
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub is_closed: bool,
}

/// Raw kline event from Binance WebSocket.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct KlineEvent {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "k")]
    kline: KlineData,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct KlineData {
    #[serde(rename = "t")]
    open_time: i64,
    #[serde(rename = "T")]
    close_time: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "i")]
    interval: String,
    #[serde(rename = "o")]
    open: String,
    #[serde(rename = "c")]
    close: String,
    #[serde(rename = "h")]
    high: String,
    #[serde(rename = "l")]
    low: String,
    #[serde(rename = "v")]
    volume: String,
    #[serde(rename = "x")]
    is_closed: bool,
}

/// Combined stream wrapper message.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct CombinedStreamMessage {
    stream: String,
    data: serde_json::Value,
}

/// Binance WebSocket client for streaming kline data.
pub struct BinanceWsClient {
    ws_url: String,
    symbols: Vec<String>,
    reconnect_delay: Duration,
    max_reconnect_delay: Duration,
}

impl BinanceWsClient {
    /// Create a new Binance WebSocket client.
    ///
    /// # Arguments
    /// * `symbols` - Trading pairs to subscribe to (e.g., ["BTCUSDT", "ETHUSDT"])
    pub fn new(symbols: Vec<String>) -> Self {
        Self {
            ws_url: BINANCE_WS_URL.to_string(),
            symbols,
            reconnect_delay: Duration::from_secs(1),
            max_reconnect_delay: Duration::from_secs(30),
        }
    }

    /// Build the combined stream URL for all symbols.
    fn build_stream_url(&self) -> String {
        let streams: Vec<String> = self
            .symbols
            .iter()
            .map(|s| format!("{}@kline_1m", s.to_lowercase()))
            .collect();

        format!("{}?streams={}", self.ws_url, streams.join("/"))
    }

    /// Connect to Binance WebSocket with retry logic.
    pub async fn connect_with_retry(
        &self,
        max_retries: u32,
    ) -> anyhow::Result<BinanceWsStream> {
        let url = self.build_stream_url();
        let mut delay = self.reconnect_delay;

        for attempt in 1..=max_retries {
            info!(
                "[BINANCE] Connecting to WebSocket (attempt {}/{}): {}",
                attempt, max_retries, url
            );

            match timeout(Duration::from_secs(10), connect_async(&url)).await {
                Ok(Ok((ws_stream, _))) => {
                    info!("[BINANCE] Connected successfully");
                    return Ok(BinanceWsStream::new(ws_stream, self.symbols.clone()));
                }
                Ok(Err(e)) => {
                    warn!("[BINANCE] Connection failed: {}", e);
                }
                Err(_) => {
                    warn!("[BINANCE] Connection timeout");
                }
            }

            if attempt < max_retries {
                info!("[BINANCE] Retrying in {:?}", delay);
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(self.max_reconnect_delay);
            }
        }

        Err(anyhow::anyhow!(
            "Failed to connect after {} attempts",
            max_retries
        ))
    }
}

/// Active WebSocket stream for receiving kline data.
pub struct BinanceWsStream {
    ws_stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    symbols: Vec<String>,
    last_ping: std::time::Instant,
}

impl BinanceWsStream {
    fn new(
        ws_stream: tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        symbols: Vec<String>,
    ) -> Self {
        Self {
            ws_stream,
            symbols,
            last_ping: std::time::Instant::now(),
        }
    }

    /// Receive the next kline event from the stream.
    /// Returns None if the connection is closed.
    pub async fn next_kline(&mut self) -> Option<BinanceKline> {
        loop {
            // Send ping every 30 seconds to keep connection alive
            if self.last_ping.elapsed() > Duration::from_secs(30) {
                if let Err(e) = self.ws_stream.send(Message::Ping(vec![].into())).await {
                    warn!("[BINANCE] Failed to send ping: {}", e);
                    return None;
                }
                self.last_ping = std::time::Instant::now();
            }

            match timeout(Duration::from_secs(60), self.ws_stream.next()).await {
                Ok(Some(Ok(msg))) => {
                    match msg {
                        Message::Text(text) => {
                            if let Some(kline) = self.parse_message(&text) {
                                return Some(kline);
                            }
                            // Continue if parse failed (might be other message type)
                        }
                        Message::Ping(data) => {
                            debug!("[BINANCE] Received ping, sending pong");
                            if let Err(e) = self.ws_stream.send(Message::Pong(data)).await {
                                warn!("[BINANCE] Failed to send pong: {}", e);
                            }
                        }
                        Message::Pong(_) => {
                            debug!("[BINANCE] Received pong");
                        }
                        Message::Close(_) => {
                            info!("[BINANCE] WebSocket closed by server");
                            return None;
                        }
                        _ => {}
                    }
                }
                Ok(Some(Err(e))) => {
                    error!("[BINANCE] WebSocket error: {}", e);
                    return None;
                }
                Ok(None) => {
                    info!("[BINANCE] WebSocket stream ended");
                    return None;
                }
                Err(_) => {
                    warn!("[BINANCE] WebSocket receive timeout");
                    // Don't return None, just continue (might be slow market)
                }
            }
        }
    }

    /// Parse a WebSocket message into a BinanceKline.
    fn parse_message(&self, text: &str) -> Option<BinanceKline> {
        // Try parsing as combined stream message first
        if let Ok(combined) = serde_json::from_str::<CombinedStreamMessage>(text) {
            if let Ok(event) = serde_json::from_value::<KlineEvent>(combined.data) {
                return self.kline_from_event(&event);
            }
        }

        // Try parsing as direct kline event
        if let Ok(event) = serde_json::from_str::<KlineEvent>(text) {
            return self.kline_from_event(&event);
        }

        debug!("[BINANCE] Failed to parse message: {}", text);
        None
    }

    fn kline_from_event(&self, event: &KlineEvent) -> Option<BinanceKline> {
        if event.event_type != "kline" {
            return None;
        }

        let k = &event.kline;

        Some(BinanceKline {
            symbol: k.symbol.clone(),
            open_time: Utc.timestamp_millis_opt(k.open_time).single()?,
            close_time: Utc.timestamp_millis_opt(k.close_time).single()?,
            open: k.open.parse().ok()?,
            high: k.high.parse().ok()?,
            low: k.low.parse().ok()?,
            close: k.close.parse().ok()?,
            volume: k.volume.parse().ok()?,
            is_closed: k.is_closed,
        })
    }

    /// Get the list of subscribed symbols.
    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }

    /// Close the WebSocket connection.
    pub async fn close(mut self) {
        let _ = self.ws_stream.close(None).await;
    }
}

/// Rolling buffer for storing recent klines per symbol.
#[derive(Debug)]
pub struct KlineBuffer {
    /// Map of symbol -> recent klines
    buffers: std::collections::HashMap<String, VecDeque<BinanceKline>>,
    /// Maximum number of klines to keep per symbol
    max_size: usize,
}

impl KlineBuffer {
    /// Create a new kline buffer.
    ///
    /// # Arguments
    /// * `max_size` - Maximum klines to keep per symbol (e.g., 10 for 10 minutes of 1m klines)
    pub fn new(max_size: usize) -> Self {
        Self {
            buffers: std::collections::HashMap::new(),
            max_size,
        }
    }

    /// Add a kline to the buffer.
    /// Only adds closed klines to avoid partial data.
    pub fn add(&mut self, kline: BinanceKline) {
        // Only store closed klines for accurate momentum calculation
        if !kline.is_closed {
            // Update the current (unclosed) kline for real-time price
            let buffer = self
                .buffers
                .entry(kline.symbol.clone())
                .or_insert_with(|| VecDeque::with_capacity(self.max_size + 1));

            // If the last kline is unclosed, replace it; otherwise add
            if let Some(last) = buffer.back_mut() {
                if !last.is_closed && last.open_time == kline.open_time {
                    *last = kline;
                    return;
                }
            }
            return;
        }

        let buffer = self
            .buffers
            .entry(kline.symbol.clone())
            .or_insert_with(|| VecDeque::with_capacity(self.max_size + 1));

        // Avoid duplicates
        if let Some(last) = buffer.back() {
            if last.open_time == kline.open_time && last.is_closed {
                return;
            }
        }

        buffer.push_back(kline);

        // Trim to max size
        while buffer.len() > self.max_size {
            buffer.pop_front();
        }
    }

    /// Calculate momentum (percentage change) over the lookback window.
    ///
    /// # Arguments
    /// * `symbol` - The trading pair (e.g., "BTCUSDT")
    /// * `lookback_minutes` - Number of minutes to look back
    ///
    /// # Returns
    /// * `Some((change_pct, direction))` if enough data
    /// * `None` if insufficient data
    pub fn calculate_momentum(
        &self,
        symbol: &str,
        lookback_minutes: usize,
    ) -> Option<(Decimal, MomentumDirection)> {
        let buffer = self.buffers.get(symbol)?;

        if buffer.len() < lookback_minutes {
            return None;
        }

        // Get oldest price in lookback window
        let oldest_idx = buffer.len().saturating_sub(lookback_minutes);
        let oldest = buffer.get(oldest_idx)?;
        let newest = buffer.back()?;

        if oldest.open == Decimal::ZERO {
            return None;
        }

        let change = (newest.close - oldest.open) / oldest.open;
        let direction = if change > Decimal::ZERO {
            MomentumDirection::Up
        } else {
            MomentumDirection::Down
        };

        Some((change, direction))
    }

    /// Get the current price for a symbol.
    pub fn current_price(&self, symbol: &str) -> Option<Decimal> {
        self.buffers
            .get(symbol)
            .and_then(|b| b.back())
            .map(|k| k.close)
    }

    /// Get the number of klines stored for a symbol.
    pub fn len(&self, symbol: &str) -> usize {
        self.buffers.get(symbol).map(|b| b.len()).unwrap_or(0)
    }

    /// Check if buffer is empty for a symbol.
    pub fn is_empty(&self, symbol: &str) -> bool {
        self.len(symbol) == 0
    }
}

/// Direction of price momentum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MomentumDirection {
    Up,
    Down,
}

impl MomentumDirection {
    /// Convert to Polymarket trade side.
    /// UP momentum -> YES (betting price goes up)
    /// DOWN momentum -> NO (betting price goes down)
    pub fn to_trade_side(&self) -> &'static str {
        match self {
            MomentumDirection::Up => "YES",
            MomentumDirection::Down => "NO",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_kline_buffer_momentum() {
        let mut buffer = KlineBuffer::new(10);

        // Add 5 closed klines with increasing prices
        for i in 0..5 {
            buffer.add(BinanceKline {
                symbol: "BTCUSDT".to_string(),
                open_time: Utc::now(),
                close_time: Utc::now(),
                open: Decimal::from(100 + i),
                high: Decimal::from(101 + i),
                low: Decimal::from(99 + i),
                close: Decimal::from(100 + i + 1),
                volume: dec!(1000),
                is_closed: true,
            });
        }

        // Calculate 5-minute momentum
        let result = buffer.calculate_momentum("BTCUSDT", 5);
        assert!(result.is_some());

        let (change, direction) = result.unwrap();
        assert!(change > Decimal::ZERO);
        assert_eq!(direction, MomentumDirection::Up);
    }
}
