//! CLOB WebSocket client for Polymarket orderbook streaming.

use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error as WsError, Message},
    MaybeTlsStream, WebSocketStream,
};
use tracing::{debug, error, info, warn};

use crate::Config;

#[derive(Debug, Error)]
pub enum ClobError {
    #[error("WebSocket connection failed: {0}")]
    ConnectionError(#[from] WsError),

    #[error("Failed to parse message: {0}")]
    ParseError(String),

    #[error("Connection timeout")]
    Timeout,

    #[error("Channel closed")]
    ChannelClosed,
}

/// Price level in the orderbook.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: String,
    pub size: String,
}

impl PriceLevel {
    /// Parse price as Decimal.
    pub fn price_decimal(&self) -> Option<Decimal> {
        self.price.parse().ok()
    }

    /// Parse size as Decimal.
    pub fn size_decimal(&self) -> Option<Decimal> {
        self.size.parse().ok()
    }
}

/// Book message from CLOB WebSocket.
#[derive(Debug, Clone, Deserialize)]
pub struct BookMessage {
    pub event_type: String,
    pub asset_id: String,
    pub market: String,
    /// Bids (buy orders). May be named "buys" in some API versions.
    #[serde(alias = "buys")]
    pub bids: Vec<PriceLevel>,
    /// Asks (sell orders). May be named "sells" in some API versions.
    #[serde(alias = "sells")]
    pub asks: Vec<PriceLevel>,
    pub timestamp: String,
    pub hash: String,
}

impl BookMessage {
    /// Get best bid price (highest bid).
    /// Uses max() for robustness - doesn't rely on API sort order.
    pub fn best_bid(&self) -> Option<Decimal> {
        self.bids.iter().filter_map(|p| p.price_decimal()).max()
    }

    /// Get best ask price (lowest ask).
    /// Uses min() for robustness - doesn't rely on API sort order.
    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.iter().filter_map(|p| p.price_decimal()).min()
    }
}

/// Price change entry.
#[derive(Debug, Clone, Deserialize)]
pub struct PriceChange {
    pub asset_id: String,
    pub price: String,
    pub size: String,
    pub side: String,
    pub best_bid: Option<String>,
    pub best_ask: Option<String>,
    pub hash: Option<String>,
}

/// Price change message from CLOB WebSocket.
#[derive(Debug, Clone, Deserialize)]
pub struct PriceChangeMessage {
    pub event_type: String,
    pub market: String,
    pub price_changes: Vec<PriceChange>,
    pub timestamp: String,
}

/// Last trade price message.
#[derive(Debug, Clone, Deserialize)]
pub struct TradeMessage {
    pub asset_id: String,
    pub event_type: String,
    pub price: String,
    pub side: String,
    pub size: String,
    pub timestamp: String,
}

/// Enum of all possible CLOB WebSocket messages.
#[derive(Debug, Clone)]
pub enum ClobMessage {
    Book(BookMessage),
    /// Batch of book snapshots (initial subscription response)
    Books(Vec<BookMessage>),
    PriceChange(PriceChangeMessage),
    Trade(TradeMessage),
    Ping,
    /// Empty acknowledgement
    Ack,
    Unknown(String),
}

/// Parse a raw WebSocket message into ClobMessage.
pub fn parse_message(text: &str) -> ClobMessage {
    // Try to parse as JSON
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(text) {
        // Handle array responses (initial subscription snapshots)
        if let Some(arr) = value.as_array() {
            if arr.is_empty() {
                return ClobMessage::Ack;
            }
            // Try to parse as array of book messages
            let books: Vec<BookMessage> = arr
                .iter()
                .filter_map(|v| {
                    if v.get("event_type").and_then(|e| e.as_str()) == Some("book") {
                        serde_json::from_value::<BookMessage>(v.clone()).ok()
                    } else {
                        None
                    }
                })
                .collect();
            if !books.is_empty() {
                return ClobMessage::Books(books);
            }
        }

        // Handle single object responses
        if let Some(event_type) = value.get("event_type").and_then(|v| v.as_str()) {
            match event_type {
                "book" => {
                    if let Ok(msg) = serde_json::from_value::<BookMessage>(value) {
                        return ClobMessage::Book(msg);
                    }
                }
                "price_change" => {
                    if let Ok(msg) = serde_json::from_value::<PriceChangeMessage>(value) {
                        return ClobMessage::PriceChange(msg);
                    }
                }
                "last_trade_price" => {
                    if let Ok(msg) = serde_json::from_value::<TradeMessage>(value) {
                        return ClobMessage::Trade(msg);
                    }
                }
                _ => {}
            }
        }
    }

    // Check for ping
    if text == "ping" || text.contains("\"type\":\"ping\"") {
        return ClobMessage::Ping;
    }

    ClobMessage::Unknown(text.to_string())
}

/// Subscription request to market channel.
#[derive(Debug, Serialize)]
pub struct SubscribeRequest {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub assets_ids: Vec<String>,
}

impl SubscribeRequest {
    /// Create a subscription request for market updates.
    pub fn market(asset_ids: Vec<String>) -> Self {
        Self {
            msg_type: "market".to_string(),
            assets_ids: asset_ids,
        }
    }
}

/// CLOB WebSocket client with reconnection logic.
pub struct ClobClient {
    ws_url: String,
    reconnect_delay: Duration,
    max_reconnect_delay: Duration,
}

impl ClobClient {
    /// Create a new CLOB client.
    pub fn new(config: &Config) -> Self {
        // Use the market channel endpoint
        let ws_url = format!("{}/market", config.clob_ws_url);
        Self {
            ws_url,
            reconnect_delay: Duration::from_secs(1),
            max_reconnect_delay: Duration::from_secs(30),
        }
    }

    /// Connect to the WebSocket.
    pub async fn connect(&self) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, ClobError> {
        info!("Connecting to CLOB WebSocket: {}", self.ws_url);

        let connect_timeout = Duration::from_secs(30);
        let (ws_stream, _) = timeout(connect_timeout, connect_async(&self.ws_url))
            .await
            .map_err(|_| ClobError::Timeout)?
            .map_err(ClobError::ConnectionError)?;

        info!("Connected to CLOB WebSocket");
        Ok(ws_stream)
    }

    /// Connect with exponential backoff retry.
    pub async fn connect_with_retry(
        &self,
        max_attempts: u32,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, ClobError> {
        let mut delay = self.reconnect_delay;
        let mut attempts = 0;

        loop {
            attempts += 1;
            match self.connect().await {
                Ok(stream) => return Ok(stream),
                Err(e) => {
                    if attempts >= max_attempts {
                        error!("Failed to connect after {} attempts", attempts);
                        return Err(e);
                    }

                    warn!(
                        "Connection attempt {} failed: {}. Retrying in {:?}",
                        attempts, e, delay
                    );
                    sleep(delay).await;

                    // Exponential backoff with cap
                    delay = std::cmp::min(delay * 2, self.max_reconnect_delay);
                }
            }
        }
    }

    /// Subscribe to orderbook updates for the given assets.
    /// Subscriptions are batched to avoid hitting server limits.
    pub async fn subscribe(
        &self,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        asset_ids: Vec<String>,
    ) -> Result<(), ClobError> {
        const BATCH_SIZE: usize = 100;
        let total_batches = (asset_ids.len() + BATCH_SIZE - 1) / BATCH_SIZE;

        info!(
            "Subscribing to {} assets in {} batches",
            asset_ids.len(),
            total_batches
        );

        for (batch_num, chunk) in asset_ids.chunks(BATCH_SIZE).enumerate() {
            let request = SubscribeRequest::market(chunk.to_vec());
            let msg = serde_json::to_string(&request)
                .map_err(|e| ClobError::ParseError(e.to_string()))?;

            debug!(
                "Sending subscription batch {}/{} ({} assets)",
                batch_num + 1,
                total_batches,
                chunk.len()
            );
            ws.send(Message::Text(msg.into()))
                .await
                .map_err(ClobError::ConnectionError)?;

            // Small delay between batches to avoid overwhelming the server
            if batch_num + 1 < total_batches {
                sleep(Duration::from_millis(100)).await;
            }
        }

        info!("All subscription batches sent");
        Ok(())
    }

    /// Subscribe to orderbook updates while concurrently reading incoming messages.
    /// This prevents data loss during the subscription phase by draining the buffer
    /// between each batch send.
    /// Returns buffered messages received during subscription.
    pub async fn subscribe_with_read(
        &self,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        asset_ids: Vec<String>,
    ) -> Result<Vec<ClobMessage>, ClobError> {
        const BATCH_SIZE: usize = 100;
        let mut buffered_messages = Vec::new();
        let total_batches = (asset_ids.len() + BATCH_SIZE - 1) / BATCH_SIZE;

        info!(
            "Subscribing to {} assets in {} batches (with concurrent read)",
            asset_ids.len(),
            total_batches
        );

        for (batch_num, chunk) in asset_ids.chunks(BATCH_SIZE).enumerate() {
            let request = SubscribeRequest::market(chunk.to_vec());
            let msg = serde_json::to_string(&request)
                .map_err(|e| ClobError::ParseError(e.to_string()))?;

            debug!(
                "Sending subscription batch {}/{} ({} assets)",
                batch_num + 1,
                total_batches,
                chunk.len()
            );

            // Send the batch
            ws.send(Message::Text(msg.into()))
                .await
                .map_err(ClobError::ConnectionError)?;

            // Drain any pending messages (non-blocking)
            loop {
                match timeout(Duration::from_millis(10), ws.next()).await {
                    Ok(Some(Ok(Message::Text(text)))) => {
                        buffered_messages.push(parse_message(&text));
                    }
                    Ok(Some(Ok(Message::Ping(data)))) => {
                        ws.send(Message::Pong(data))
                            .await
                            .map_err(ClobError::ConnectionError)?;
                    }
                    _ => break, // Timeout or other - continue to next batch
                }
            }

            // Small delay between batches
            if batch_num + 1 < total_batches {
                sleep(Duration::from_millis(50)).await;
            }
        }

        // Final drain to catch any remaining messages
        loop {
            match timeout(Duration::from_millis(100), ws.next()).await {
                Ok(Some(Ok(Message::Text(text)))) => {
                    buffered_messages.push(parse_message(&text));
                }
                Ok(Some(Ok(Message::Ping(data)))) => {
                    ws.send(Message::Pong(data))
                        .await
                        .map_err(ClobError::ConnectionError)?;
                }
                _ => break,
            }
        }

        info!(
            "Subscription complete, buffered {} messages during subscribe",
            buffered_messages.len()
        );
        Ok(buffered_messages)
    }

    /// Read the next message from the WebSocket.
    pub async fn read_message(
        &self,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<Option<ClobMessage>, ClobError> {
        match ws.next().await {
            Some(Ok(Message::Text(text))) => Ok(Some(parse_message(&text))),
            Some(Ok(Message::Ping(data))) => {
                // Respond to ping with pong
                ws.send(Message::Pong(data))
                    .await
                    .map_err(ClobError::ConnectionError)?;
                Ok(Some(ClobMessage::Ping))
            }
            Some(Ok(Message::Close(_))) => {
                info!("WebSocket closed by server");
                Err(ClobError::ChannelClosed)
            }
            Some(Ok(_)) => Ok(None), // Binary, Pong, Frame - ignore
            Some(Err(e)) => Err(ClobError::ConnectionError(e)),
            None => Err(ClobError::ChannelClosed),
        }
    }

    /// Send a ping to keep the connection alive.
    /// Per Polymarket docs: send PING every 10 seconds to maintain connection.
    pub async fn send_ping(
        &self,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<(), ClobError> {
        ws.send(Message::Ping(vec![].into()))
            .await
            .map_err(ClobError::ConnectionError)?;
        debug!("Sent keepalive ping");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_book_message() {
        let json = r#"{
            "event_type": "book",
            "asset_id": "123",
            "market": "condition-456",
            "bids": [{"price": "0.55", "size": "100"}],
            "asks": [{"price": "0.56", "size": "200"}],
            "timestamp": "1704067200000",
            "hash": "abc123"
        }"#;

        match parse_message(json) {
            ClobMessage::Book(msg) => {
                assert_eq!(msg.asset_id, "123");
                assert_eq!(msg.market, "condition-456");
                assert_eq!(msg.bids.len(), 1);
                assert_eq!(msg.asks.len(), 1);
                assert_eq!(msg.best_bid(), Some(Decimal::new(55, 2)));
                assert_eq!(msg.best_ask(), Some(Decimal::new(56, 2)));
            }
            _ => panic!("Expected Book message"),
        }
    }

    #[test]
    fn test_parse_price_change_message() {
        let json = r#"{
            "event_type": "price_change",
            "market": "condition-456",
            "price_changes": [
                {
                    "asset_id": "123",
                    "price": "0.55",
                    "size": "50",
                    "side": "BUY",
                    "best_bid": "0.55",
                    "best_ask": "0.56"
                }
            ],
            "timestamp": "1704067200000"
        }"#;

        match parse_message(json) {
            ClobMessage::PriceChange(msg) => {
                assert_eq!(msg.market, "condition-456");
                assert_eq!(msg.price_changes.len(), 1);
                assert_eq!(msg.price_changes[0].side, "BUY");
            }
            _ => panic!("Expected PriceChange message"),
        }
    }

    #[test]
    fn test_parse_trade_message() {
        let json = r#"{
            "event_type": "last_trade_price",
            "asset_id": "123",
            "price": "0.55",
            "side": "BUY",
            "size": "25",
            "timestamp": "1704067200000"
        }"#;

        match parse_message(json) {
            ClobMessage::Trade(msg) => {
                assert_eq!(msg.asset_id, "123");
                assert_eq!(msg.price, "0.55");
                assert_eq!(msg.side, "BUY");
            }
            _ => panic!("Expected Trade message"),
        }
    }

    #[test]
    fn test_parse_ping() {
        match parse_message("ping") {
            ClobMessage::Ping => {}
            _ => panic!("Expected Ping message"),
        }
    }

    #[test]
    fn test_parse_unknown() {
        match parse_message("random garbage") {
            ClobMessage::Unknown(s) => assert_eq!(s, "random garbage"),
            _ => panic!("Expected Unknown message"),
        }
    }

    #[test]
    fn test_subscribe_request_serialization() {
        let req = SubscribeRequest::market(vec!["token1".to_string(), "token2".to_string()]);
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"type\":\"market\""));
        assert!(json.contains("\"assets_ids\""));
    }
}
