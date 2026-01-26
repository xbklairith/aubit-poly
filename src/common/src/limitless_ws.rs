//! Limitless Exchange WebSocket client for real-time orderbook streaming.
//!
//! Connects to Limitless WebSocket API for orderbook updates.
//! Similar to Polymarket CLOB, provides both YES and NO orderbook data.
//!
//! WebSocket URL: wss://ws.limitless.exchange/markets

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use crate::limitless::LIMITLESS_WS_URL;

/// Limitless WebSocket message types (based on Polymarket fork)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LimitlessWsMessage {
    /// Subscribe to orderbook channels
    Subscribe(SubscribeRequest),
    /// Subscription confirmed
    Subscribed(SubscribedResponse),
    /// Orderbook snapshot
    #[serde(rename = "book")]
    BookSnapshot(BookSnapshot),
    /// Price/book change
    #[serde(rename = "price_change")]
    PriceChange(PriceChangeEvent),
    /// Error message
    Error(ErrorMessage),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeRequest {
    /// Assets or market slugs to subscribe to
    pub assets_ids: Vec<String>,
    /// Channel type
    #[serde(rename = "type")]
    pub channel_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribedResponse {
    pub msg: String,
}

/// Orderbook snapshot from Limitless
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookSnapshot {
    /// Market slug
    #[serde(alias = "market", alias = "asset_id")]
    pub slug: String,
    /// Timestamp
    pub timestamp: Option<String>,
    /// YES side orderbook
    pub yes: Option<OrderbookSide>,
    /// NO side orderbook
    pub no: Option<OrderbookSide>,
    /// Bids (alternative format)
    pub bids: Option<Vec<PriceLevel>>,
    /// Asks (alternative format)
    pub asks: Option<Vec<PriceLevel>>,
}

/// One side of the orderbook
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OrderbookSide {
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

/// Single price level in orderbook
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    /// Price (0-1 decimal)
    pub price: f64,
    /// Size/quantity
    #[serde(alias = "quantity")]
    pub size: f64,
}

/// Price change event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceChangeEvent {
    /// Market slug
    #[serde(alias = "market", alias = "asset_id")]
    pub slug: String,
    /// New YES price
    pub yes_price: Option<f64>,
    /// New NO price
    pub no_price: Option<f64>,
    /// Best YES bid
    pub yes_bid: Option<f64>,
    /// Best YES ask
    pub yes_ask: Option<f64>,
    /// Best NO bid
    pub no_bid: Option<f64>,
    /// Best NO ask
    pub no_ask: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorMessage {
    pub msg: String,
    pub code: Option<i32>,
}

/// Parsed orderbook update to send to the database
#[derive(Debug, Clone)]
pub struct LimitlessOrderbookUpdate {
    pub slug: String,
    pub yes_best_bid: Option<Decimal>,
    pub yes_best_ask: Option<Decimal>,
    pub no_best_bid: Option<Decimal>,
    pub no_best_ask: Option<Decimal>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Local orderbook state for a single market
#[derive(Debug, Clone, Default)]
struct LocalOrderbook {
    yes_bids: Vec<(f64, f64)>, // (price, size) sorted desc by price
    yes_asks: Vec<(f64, f64)>, // (price, size) sorted asc by price
    no_bids: Vec<(f64, f64)>,
    no_asks: Vec<(f64, f64)>,
}

impl LocalOrderbook {
    /// Apply a snapshot to reset the orderbook
    fn apply_snapshot(&mut self, snapshot: &BookSnapshot) {
        self.yes_bids.clear();
        self.yes_asks.clear();
        self.no_bids.clear();
        self.no_asks.clear();

        // Handle structured format (yes/no objects)
        if let Some(yes) = &snapshot.yes {
            for level in &yes.bids {
                if level.size > 0.0 {
                    self.yes_bids.push((level.price, level.size));
                }
            }
            for level in &yes.asks {
                if level.size > 0.0 {
                    self.yes_asks.push((level.price, level.size));
                }
            }
        }

        if let Some(no) = &snapshot.no {
            for level in &no.bids {
                if level.size > 0.0 {
                    self.no_bids.push((level.price, level.size));
                }
            }
            for level in &no.asks {
                if level.size > 0.0 {
                    self.no_asks.push((level.price, level.size));
                }
            }
        }

        // Handle flat format (bids/asks at top level)
        if let Some(bids) = &snapshot.bids {
            for level in bids {
                if level.size > 0.0 {
                    self.yes_bids.push((level.price, level.size));
                }
            }
        }

        if let Some(asks) = &snapshot.asks {
            for level in asks {
                if level.size > 0.0 {
                    self.yes_asks.push((level.price, level.size));
                }
            }
        }

        // Sort: bids desc by price, asks asc by price
        self.yes_bids.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        self.yes_asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        self.no_bids.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        self.no_asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    }

    /// Apply a price change event
    fn apply_price_change(&mut self, event: &PriceChangeEvent) {
        // Update best prices from event
        if let Some(bid) = event.yes_bid {
            if self.yes_bids.is_empty() || self.yes_bids[0].0 != bid {
                self.yes_bids.insert(0, (bid, 1.0)); // Size unknown, use placeholder
            }
        }
        if let Some(ask) = event.yes_ask {
            if self.yes_asks.is_empty() || self.yes_asks[0].0 != ask {
                self.yes_asks.insert(0, (ask, 1.0));
            }
        }
        if let Some(bid) = event.no_bid {
            if self.no_bids.is_empty() || self.no_bids[0].0 != bid {
                self.no_bids.insert(0, (bid, 1.0));
            }
        }
        if let Some(ask) = event.no_ask {
            if self.no_asks.is_empty() || self.no_asks[0].0 != ask {
                self.no_asks.insert(0, (ask, 1.0));
            }
        }
    }

    /// Get best prices from current orderbook state
    fn get_best_prices(&self) -> LimitlessOrderbookUpdate {
        let yes_best_bid = self
            .yes_bids
            .first()
            .map(|(p, _)| Decimal::try_from(*p).unwrap_or_default());
        let yes_best_ask = self
            .yes_asks
            .first()
            .map(|(p, _)| Decimal::try_from(*p).unwrap_or_default());
        let no_best_bid = self
            .no_bids
            .first()
            .map(|(p, _)| Decimal::try_from(*p).unwrap_or_default());
        let no_best_ask = self
            .no_asks
            .first()
            .map(|(p, _)| Decimal::try_from(*p).unwrap_or_default());

        LimitlessOrderbookUpdate {
            slug: String::new(), // Will be filled in by caller
            yes_best_bid,
            yes_best_ask,
            no_best_bid,
            no_best_ask,
            timestamp: chrono::Utc::now(),
        }
    }
}

/// Limitless WebSocket client
pub struct LimitlessWsClient {
    ws_url: String,
    orderbooks: HashMap<String, LocalOrderbook>,
}

impl LimitlessWsClient {
    /// Create new client
    pub fn new() -> Self {
        Self {
            ws_url: LIMITLESS_WS_URL.to_string(),
            orderbooks: HashMap::new(),
        }
    }

    /// Create new client with custom URL
    pub fn with_url(url: &str) -> Self {
        Self {
            ws_url: url.to_string(),
            orderbooks: HashMap::new(),
        }
    }

    /// Connect and stream orderbook updates
    pub async fn stream_orderbooks(
        &mut self,
        slugs: Vec<String>,
        tx: mpsc::Sender<LimitlessOrderbookUpdate>,
    ) -> Result<()> {
        info!("Connecting to Limitless WebSocket at {}", self.ws_url);

        // Connect with timeout
        let (ws_stream, response) = timeout(
            Duration::from_secs(10),
            connect_async(&self.ws_url),
        )
        .await
        .map_err(|_| anyhow!("WebSocket connection timeout"))?
        .map_err(|e| anyhow!("WebSocket connection failed: {}", e))?;

        info!(
            "Connected to Limitless WebSocket (status: {})",
            response.status()
        );

        let (mut write, mut read) = ws_stream.split();

        // Subscribe to orderbook channel for all slugs
        let subscribe_msg = serde_json::json!({
            "type": "subscribe",
            "assets_ids": slugs,
            "channel": "book"
        });

        let msg_json = serde_json::to_string(&subscribe_msg)?;
        info!("Subscribing to {} markets", slugs.len());
        debug!("Subscribe message: {}", msg_json);
        write.send(Message::Text(msg_json.into())).await?;

        // Initialize local orderbooks
        for slug in &slugs {
            self.orderbooks.insert(slug.clone(), LocalOrderbook::default());
        }

        // Process messages
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Err(e) = self.handle_message(&text, &tx).await {
                        warn!("Failed to handle message: {}", e);
                    }
                }
                Ok(Message::Ping(data)) => {
                    if let Err(e) = write.send(Message::Pong(data)).await {
                        error!("Failed to send pong: {}", e);
                        break;
                    }
                }
                Ok(Message::Close(_)) => {
                    info!("WebSocket closed by server");
                    break;
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Handle incoming WebSocket message
    async fn handle_message(
        &mut self,
        text: &str,
        tx: &mpsc::Sender<LimitlessOrderbookUpdate>,
    ) -> Result<()> {
        // Parse the raw JSON to determine message type
        let raw: serde_json::Value = serde_json::from_str(text)?;

        if let Some(msg_type) = raw.get("type").and_then(|t| t.as_str()) {
            match msg_type {
                "book" | "orderbook_snapshot" | "snapshot" => {
                    let snapshot: BookSnapshot = serde_json::from_value(raw)?;
                    self.handle_snapshot(snapshot, tx).await?;
                }
                "price_change" | "trade" => {
                    let event: PriceChangeEvent = serde_json::from_value(raw)?;
                    self.handle_price_change(event, tx).await?;
                }
                "subscribed" => {
                    debug!("Subscription confirmed: {}", text);
                }
                "error" => {
                    let error: ErrorMessage = serde_json::from_value(raw)?;
                    error!("Limitless error: {} (code: {:?})", error.msg, error.code);
                }
                _ => {
                    debug!("Unknown message type: {}", msg_type);
                }
            }
        } else {
            // Try to handle as book snapshot (some formats don't have type field)
            if raw.get("bids").is_some() || raw.get("yes").is_some() {
                if let Ok(snapshot) = serde_json::from_value::<BookSnapshot>(raw) {
                    self.handle_snapshot(snapshot, tx).await?;
                }
            } else {
                debug!("Message without type field: {}", text);
            }
        }

        Ok(())
    }

    /// Handle orderbook snapshot
    async fn handle_snapshot(
        &mut self,
        snapshot: BookSnapshot,
        tx: &mpsc::Sender<LimitlessOrderbookUpdate>,
    ) -> Result<()> {
        let slug = snapshot.slug.clone();
        debug!("Received snapshot for {}", slug);

        // Update local orderbook
        let book = self.orderbooks.entry(slug.clone()).or_default();
        book.apply_snapshot(&snapshot);

        // Send update
        let mut update = book.get_best_prices();
        update.slug = slug;

        tx.send(update).await?;

        Ok(())
    }

    /// Handle price change event
    async fn handle_price_change(
        &mut self,
        event: PriceChangeEvent,
        tx: &mpsc::Sender<LimitlessOrderbookUpdate>,
    ) -> Result<()> {
        let slug = event.slug.clone();

        // Update local orderbook
        if let Some(book) = self.orderbooks.get_mut(&slug) {
            book.apply_price_change(&event);

            // Send update
            let mut update = book.get_best_prices();
            update.slug = slug;

            tx.send(update).await?;
        }

        Ok(())
    }
}

impl Default for LimitlessWsClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Run Limitless orderbook streaming with reconnection logic
pub async fn run_limitless_orderbook_stream(
    slugs: Vec<String>,
    tx: mpsc::Sender<LimitlessOrderbookUpdate>,
    reconnect_interval: Duration,
) -> Result<()> {
    loop {
        let mut client = LimitlessWsClient::new();

        match client.stream_orderbooks(slugs.clone(), tx.clone()).await {
            Ok(_) => {
                info!("Limitless WebSocket stream ended gracefully");
            }
            Err(e) => {
                error!("Limitless WebSocket error: {}", e);
            }
        }

        info!("Reconnecting in {:?}...", reconnect_interval);
        tokio::time::sleep(reconnect_interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_orderbook_snapshot() {
        let mut book = LocalOrderbook::default();

        let snapshot = BookSnapshot {
            slug: "btc-hourly".to_string(),
            timestamp: None,
            yes: Some(OrderbookSide {
                bids: vec![
                    PriceLevel {
                        price: 0.55,
                        size: 100.0,
                    },
                    PriceLevel {
                        price: 0.54,
                        size: 200.0,
                    },
                ],
                asks: vec![PriceLevel {
                    price: 0.56,
                    size: 150.0,
                }],
            }),
            no: Some(OrderbookSide {
                bids: vec![PriceLevel {
                    price: 0.44,
                    size: 150.0,
                }],
                asks: vec![PriceLevel {
                    price: 0.46,
                    size: 100.0,
                }],
            }),
            bids: None,
            asks: None,
        };

        book.apply_snapshot(&snapshot);

        // Check YES orderbook
        assert_eq!(book.yes_bids.len(), 2);
        assert_eq!(book.yes_bids[0].0, 0.55); // Best bid
        assert_eq!(book.yes_asks.len(), 1);
        assert_eq!(book.yes_asks[0].0, 0.56); // Best ask

        // Check NO orderbook
        assert_eq!(book.no_bids.len(), 1);
        assert_eq!(book.no_bids[0].0, 0.44);
        assert_eq!(book.no_asks.len(), 1);
        assert_eq!(book.no_asks[0].0, 0.46);

        // Check best prices
        let prices = book.get_best_prices();
        assert!(prices.yes_best_bid.is_some());
        assert!(prices.yes_best_ask.is_some());
        assert!(prices.no_best_bid.is_some());
        assert!(prices.no_best_ask.is_some());
    }

    #[test]
    fn test_price_change_update() {
        let mut book = LocalOrderbook::default();

        // Apply initial snapshot
        let snapshot = BookSnapshot {
            slug: "eth-hourly".to_string(),
            timestamp: None,
            yes: Some(OrderbookSide {
                bids: vec![PriceLevel {
                    price: 0.50,
                    size: 100.0,
                }],
                asks: vec![PriceLevel {
                    price: 0.52,
                    size: 100.0,
                }],
            }),
            no: None,
            bids: None,
            asks: None,
        };
        book.apply_snapshot(&snapshot);

        // Apply price change
        let event = PriceChangeEvent {
            slug: "eth-hourly".to_string(),
            yes_price: None,
            no_price: None,
            yes_bid: Some(0.51),
            yes_ask: Some(0.53),
            no_bid: Some(0.47),
            no_ask: Some(0.49),
        };
        book.apply_price_change(&event);

        // New best prices should be updated
        assert_eq!(book.yes_bids[0].0, 0.51);
        assert_eq!(book.yes_asks[0].0, 0.53);
    }
}
