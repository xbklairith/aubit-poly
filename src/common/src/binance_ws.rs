//! Binance WebSocket client for real-time market data.
//!
//! Supports two stream types:
//! - **bookTicker**: Real-time best bid/ask updates (~10ms latency, 150+ updates/sec)
//! - **kline_1m**: 1-minute candlestick data (~2s updates, for momentum calculation)

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

/// Stream type to subscribe to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinanceStreamType {
    /// Real-time best bid/ask updates (~10ms, 150+ updates/sec)
    BookTicker,
    /// 1-minute candlestick data (~2s updates)
    Kline1m,
    /// Both bookTicker and kline (for services needing both)
    Both,
}

/// Real-time book ticker data from Binance.
/// Updates on every best bid/ask change (~10ms latency).
#[derive(Debug, Clone)]
pub struct BinanceBookTicker {
    pub symbol: String,
    pub best_bid: Decimal,
    pub best_bid_qty: Decimal,
    pub best_ask: Decimal,
    pub best_ask_qty: Decimal,
    pub timestamp: DateTime<Utc>,
}

impl BinanceBookTicker {
    /// Get mid price (average of bid and ask).
    pub fn mid_price(&self) -> Decimal {
        (self.best_bid + self.best_ask) / Decimal::from(2)
    }

    /// Get spread in absolute terms.
    pub fn spread(&self) -> Decimal {
        self.best_ask - self.best_bid
    }

    /// Get spread as percentage of mid price.
    pub fn spread_pct(&self) -> Decimal {
        let mid = self.mid_price();
        if mid == Decimal::ZERO {
            Decimal::ZERO
        } else {
            self.spread() / mid * Decimal::from(100)
        }
    }
}

/// Raw bookTicker event from Binance WebSocket.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct BookTickerEvent {
    #[serde(rename = "u")]
    update_id: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    best_bid: String,
    #[serde(rename = "B")]
    best_bid_qty: String,
    #[serde(rename = "a")]
    best_ask: String,
    #[serde(rename = "A")]
    best_ask_qty: String,
}

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

/// Unified market data event (either ticker or kline).
#[derive(Debug, Clone)]
pub enum BinanceEvent {
    Ticker(BinanceBookTicker),
    Kline(BinanceKline),
}

/// Binance WebSocket client for streaming market data.
pub struct BinanceWsClient {
    ws_url: String,
    symbols: Vec<String>,
    stream_type: BinanceStreamType,
    reconnect_delay: Duration,
    max_reconnect_delay: Duration,
}

impl BinanceWsClient {
    /// Create a new Binance WebSocket client with bookTicker stream (fastest).
    ///
    /// # Arguments
    /// * `symbols` - Trading pairs to subscribe to (e.g., ["BTCUSDT", "ETHUSDT"])
    pub fn new(symbols: Vec<String>) -> Self {
        Self {
            ws_url: BINANCE_WS_URL.to_string(),
            symbols,
            stream_type: BinanceStreamType::BookTicker, // Default to fastest
            reconnect_delay: Duration::from_secs(1),
            max_reconnect_delay: Duration::from_secs(30),
        }
    }

    /// Create a client with specific stream type.
    pub fn with_stream_type(symbols: Vec<String>, stream_type: BinanceStreamType) -> Self {
        Self {
            ws_url: BINANCE_WS_URL.to_string(),
            symbols,
            stream_type,
            reconnect_delay: Duration::from_secs(1),
            max_reconnect_delay: Duration::from_secs(30),
        }
    }

    /// Create a client for kline data only (backward compatible).
    pub fn klines_only(symbols: Vec<String>) -> Self {
        Self::with_stream_type(symbols, BinanceStreamType::Kline1m)
    }

    /// Build the combined stream URL for all symbols.
    fn build_stream_url(&self) -> String {
        let streams: Vec<String> = match self.stream_type {
            BinanceStreamType::BookTicker => self
                .symbols
                .iter()
                .map(|s| format!("{}@bookTicker", s.to_lowercase()))
                .collect(),
            BinanceStreamType::Kline1m => self
                .symbols
                .iter()
                .map(|s| format!("{}@kline_1m", s.to_lowercase()))
                .collect(),
            BinanceStreamType::Both => {
                let mut streams = Vec::with_capacity(self.symbols.len() * 2);
                for s in &self.symbols {
                    let lower = s.to_lowercase();
                    streams.push(format!("{}@bookTicker", lower));
                    streams.push(format!("{}@kline_1m", lower));
                }
                streams
            }
        };

        format!("{}?streams={}", self.ws_url, streams.join("/"))
    }

    /// Connect to Binance WebSocket with retry logic.
    pub async fn connect_with_retry(&self, max_retries: u32) -> anyhow::Result<BinanceWsStream> {
        let url = self.build_stream_url();
        let mut delay = self.reconnect_delay;

        for attempt in 1..=max_retries {
            info!(
                "[BINANCE] Connecting to {:?} stream (attempt {}/{})",
                self.stream_type, attempt, max_retries
            );
            debug!("[BINANCE] URL: {}", url);

            match timeout(Duration::from_secs(10), connect_async(&url)).await {
                Ok(Ok((ws_stream, _))) => {
                    info!("[BINANCE] Connected successfully to {:?}", self.stream_type);
                    return Ok(BinanceWsStream::new(
                        ws_stream,
                        self.symbols.clone(),
                        self.stream_type,
                    ));
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

/// Active WebSocket stream for receiving market data.
pub struct BinanceWsStream {
    ws_stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    symbols: Vec<String>,
    stream_type: BinanceStreamType,
    last_ping: std::time::Instant,
    /// Latest ticker per symbol (for real-time price access)
    latest_tickers: std::collections::HashMap<String, BinanceBookTicker>,
}

impl BinanceWsStream {
    fn new(
        ws_stream: tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        symbols: Vec<String>,
        stream_type: BinanceStreamType,
    ) -> Self {
        Self {
            ws_stream,
            symbols,
            stream_type,
            last_ping: std::time::Instant::now(),
            latest_tickers: std::collections::HashMap::new(),
        }
    }

    /// Receive the next event (ticker or kline) from the stream.
    pub async fn next_event(&mut self) -> Option<BinanceEvent> {
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
                            if let Some(event) = self.parse_message(&text) {
                                // Update latest ticker cache
                                if let BinanceEvent::Ticker(ref ticker) = event {
                                    self.latest_tickers
                                        .insert(ticker.symbol.clone(), ticker.clone());
                                }
                                return Some(event);
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

    /// Receive the next book ticker (blocks until one arrives).
    /// Skips kline events if stream is Both.
    pub async fn next_ticker(&mut self) -> Option<BinanceBookTicker> {
        loop {
            match self.next_event().await? {
                BinanceEvent::Ticker(ticker) => return Some(ticker),
                BinanceEvent::Kline(_) => continue, // Skip klines
            }
        }
    }

    /// Receive the next kline (blocks until one arrives).
    /// Skips ticker events if stream is Both.
    pub async fn next_kline(&mut self) -> Option<BinanceKline> {
        loop {
            match self.next_event().await? {
                BinanceEvent::Kline(kline) => return Some(kline),
                BinanceEvent::Ticker(_) => continue, // Skip tickers
            }
        }
    }

    /// Get the latest cached ticker for a symbol.
    /// Returns immediately without waiting for new data.
    pub fn get_latest_ticker(&self, symbol: &str) -> Option<&BinanceBookTicker> {
        self.latest_tickers.get(symbol)
    }

    /// Get the current price for a symbol from cached ticker.
    pub fn current_price(&self, symbol: &str) -> Option<Decimal> {
        self.latest_tickers.get(symbol).map(|t| t.mid_price())
    }

    /// Get the current ask price for a symbol from cached ticker.
    pub fn current_ask(&self, symbol: &str) -> Option<Decimal> {
        self.latest_tickers.get(symbol).map(|t| t.best_ask)
    }

    /// Get the current bid price for a symbol from cached ticker.
    pub fn current_bid(&self, symbol: &str) -> Option<Decimal> {
        self.latest_tickers.get(symbol).map(|t| t.best_bid)
    }

    /// Parse a WebSocket message into an event.
    fn parse_message(&self, text: &str) -> Option<BinanceEvent> {
        // Try parsing as combined stream message first
        if let Ok(combined) = serde_json::from_str::<CombinedStreamMessage>(text) {
            // Check if it's a bookTicker stream
            if combined.stream.ends_with("@bookTicker") {
                if let Ok(event) = serde_json::from_value::<BookTickerEvent>(combined.data.clone())
                {
                    return self.ticker_from_event(&event);
                }
            }
            // Check if it's a kline stream
            if combined.stream.contains("@kline") {
                if let Ok(event) = serde_json::from_value::<KlineEvent>(combined.data) {
                    return self.kline_from_event(&event);
                }
            }
        }

        // Try parsing as direct bookTicker event
        if let Ok(event) = serde_json::from_str::<BookTickerEvent>(text) {
            return self.ticker_from_event(&event);
        }

        // Try parsing as direct kline event
        if let Ok(event) = serde_json::from_str::<KlineEvent>(text) {
            return self.kline_from_event(&event);
        }

        debug!("[BINANCE] Failed to parse message: {}", text);
        None
    }

    fn ticker_from_event(&self, event: &BookTickerEvent) -> Option<BinanceEvent> {
        Some(BinanceEvent::Ticker(BinanceBookTicker {
            symbol: event.symbol.clone(),
            best_bid: event.best_bid.parse().ok()?,
            best_bid_qty: event.best_bid_qty.parse().ok()?,
            best_ask: event.best_ask.parse().ok()?,
            best_ask_qty: event.best_ask_qty.parse().ok()?,
            timestamp: Utc::now(),
        }))
    }

    fn kline_from_event(&self, event: &KlineEvent) -> Option<BinanceEvent> {
        if event.event_type != "kline" {
            return None;
        }

        let k = &event.kline;

        Some(BinanceEvent::Kline(BinanceKline {
            symbol: k.symbol.clone(),
            open_time: Utc.timestamp_millis_opt(k.open_time).single()?,
            close_time: Utc.timestamp_millis_opt(k.close_time).single()?,
            open: k.open.parse().ok()?,
            high: k.high.parse().ok()?,
            low: k.low.parse().ok()?,
            close: k.close.parse().ok()?,
            volume: k.volume.parse().ok()?,
            is_closed: k.is_closed,
        }))
    }

    /// Get the list of subscribed symbols.
    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }

    /// Get the stream type.
    pub fn stream_type(&self) -> BinanceStreamType {
        self.stream_type
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
    /// Latest ticker prices (from bookTicker stream)
    latest_prices: std::collections::HashMap<String, Decimal>,
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
            latest_prices: std::collections::HashMap::new(),
        }
    }

    /// Add a kline to the buffer.
    /// Only adds closed klines to avoid partial data.
    pub fn add(&mut self, kline: BinanceKline) {
        // Update latest price from kline close
        self.latest_prices.insert(kline.symbol.clone(), kline.close);

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

    /// Update latest price from a book ticker event.
    pub fn update_price(&mut self, ticker: &BinanceBookTicker) {
        self.latest_prices
            .insert(ticker.symbol.clone(), ticker.mid_price());
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

        // Use latest real-time price if available, otherwise use newest kline close
        let current_price = self
            .latest_prices
            .get(symbol)
            .copied()
            .or_else(|| buffer.back().map(|k| k.close))?;

        if oldest.open == Decimal::ZERO {
            return None;
        }

        let change = (current_price - oldest.open) / oldest.open;
        let direction = if change > Decimal::ZERO {
            MomentumDirection::Up
        } else {
            MomentumDirection::Down
        };

        Some((change, direction))
    }

    /// Get the current price for a symbol (from bookTicker or kline).
    pub fn current_price(&self, symbol: &str) -> Option<Decimal> {
        // Prefer real-time ticker price
        self.latest_prices.get(symbol).copied().or_else(|| {
            self.buffers
                .get(symbol)
                .and_then(|b| b.back())
                .map(|k| k.close)
        })
    }

    /// Get the number of klines stored for a symbol.
    pub fn len(&self, symbol: &str) -> usize {
        self.buffers.get(symbol).map(|b| b.len()).unwrap_or(0)
    }

    /// Check if buffer is empty for a symbol.
    pub fn is_empty(&self, symbol: &str) -> bool {
        self.len(symbol) == 0
    }

    /// Get open price from kline containing the target timestamp.
    /// Used by misprice-trader to get the open price at market start time.
    ///
    /// Returns None if:
    /// - No buffer exists for the symbol
    /// - No kline contains the target timestamp (target is too old or too new)
    ///
    /// IMPORTANT: This function does NOT fall back to approximate data.
    /// Trading decisions require accurate data at the exact market start time.
    pub fn get_open_at_time(&self, symbol: &str, target: DateTime<Utc>) -> Option<Decimal> {
        let buffer = self.buffers.get(symbol)?;

        if buffer.is_empty() {
            return None;
        }

        // Find the kline that contains the target timestamp
        for kline in buffer.iter() {
            if kline.open_time <= target && target <= kline.close_time {
                return Some(kline.open);
            }
        }

        // No exact match found - do NOT fall back to approximate data
        // This is a trading system where incorrect open prices can cause losses
        //
        // Possible reasons:
        // 1. Target is before our oldest kline (we just started, don't have enough history)
        // 2. Target is after our newest kline (shouldn't happen in normal operation)
        // 3. Gap in kline data (network issues)
        None
    }

    /// Check if we have kline data covering the target timestamp.
    /// Useful for debugging why get_open_at_time returns None.
    pub fn has_kline_for_time(&self, symbol: &str, target: DateTime<Utc>) -> bool {
        self.get_open_at_time(symbol, target).is_some()
    }

    /// Get the time range covered by the buffer for a symbol.
    /// Returns (oldest_open_time, newest_close_time) or None if buffer is empty.
    pub fn get_buffer_time_range(
        &self,
        symbol: &str,
    ) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
        let buffer = self.buffers.get(symbol)?;
        let oldest = buffer.front()?;
        let newest = buffer.back()?;
        Some((oldest.open_time, newest.close_time))
    }

    /// Get latest close price (real-time price).
    pub fn get_latest_close(&self, symbol: &str) -> Option<Decimal> {
        // Prefer real-time ticker price
        self.latest_prices.get(symbol).copied().or_else(|| {
            self.buffers
                .get(symbol)
                .and_then(|b| b.back())
                .map(|k| k.close)
        })
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

    #[test]
    fn test_book_ticker_spread() {
        let ticker = BinanceBookTicker {
            symbol: "BTCUSDT".to_string(),
            best_bid: dec!(100),
            best_bid_qty: dec!(1),
            best_ask: dec!(101),
            best_ask_qty: dec!(1),
            timestamp: Utc::now(),
        };

        assert_eq!(ticker.mid_price(), dec!(100.5));
        assert_eq!(ticker.spread(), dec!(1));
    }
}
