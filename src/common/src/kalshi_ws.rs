//! Kalshi WebSocket client for real-time orderbook streaming.
//!
//! Connects to Kalshi's WebSocket API for orderbook updates.
//! Unlike Polymarket's CLOB, Kalshi only returns YES bids/asks.
//! NO prices are derived: NO_ask = 1 - YES_bid, NO_bid = 1 - YES_ask

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use futures_util::{SinkExt, StreamExt};
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::sha2::Sha256;
use rsa::signature::{SignatureEncoding, Signer};
use rsa::RsaPrivateKey;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

/// Default Kalshi WebSocket URL
pub const KALSHI_WS_URL: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";

/// Kalshi WebSocket message types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KalshiWsMessage {
    /// Subscribe to channels
    #[serde(rename = "subscribe")]
    Subscribe(SubscribeRequest),
    /// Unsubscribe from channels
    #[serde(rename = "unsubscribe")]
    Unsubscribe(UnsubscribeRequest),
    /// Subscription response
    #[serde(rename = "subscribed")]
    Subscribed(SubscribedResponse),
    /// Orderbook snapshot
    #[serde(rename = "orderbook_snapshot")]
    OrderbookSnapshot(OrderbookSnapshot),
    /// Orderbook delta update
    #[serde(rename = "orderbook_delta")]
    OrderbookDelta(OrderbookDelta),
    /// Error message
    #[serde(rename = "error")]
    Error(ErrorMessage),
}

/// Command-based message format (alternative format Kalshi uses)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandMessage {
    pub id: u64,
    pub cmd: String,
    pub params: CommandParams,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandParams {
    pub channels: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub market_tickers: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeRequest {
    pub channels: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub market_tickers: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsubscribeRequest {
    pub channels: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub market_tickers: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribedResponse {
    pub msg: String,
    pub sid: i64,
}

/// Orderbook snapshot from Kalshi
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookSnapshot {
    pub market_ticker: String,
    pub yes: Vec<PriceLevel>,
    pub no: Vec<PriceLevel>,
    pub seq: u64,
}

/// Single price level in orderbook
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: i32, // cents (1-99)
    pub quantity: i64,
}

/// Orderbook delta update
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookDelta {
    pub market_ticker: String,
    pub price: i32,
    pub delta: i64,
    pub side: String, // "yes" or "no"
    pub seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorMessage {
    pub msg: String,
    pub code: Option<i32>,
}

/// Parsed orderbook update to send to the database
#[derive(Debug, Clone)]
pub struct KalshiOrderbookUpdate {
    pub market_ticker: String,
    pub yes_best_bid: Option<Decimal>,
    pub yes_best_ask: Option<Decimal>,
    pub no_best_bid: Option<Decimal>,
    pub no_best_ask: Option<Decimal>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Local orderbook state for a single market
#[derive(Debug, Clone, Default)]
struct LocalOrderbook {
    yes_bids: HashMap<i32, i64>, // price (cents) -> quantity
    yes_asks: HashMap<i32, i64>,
    no_bids: HashMap<i32, i64>,
    no_asks: HashMap<i32, i64>,
    seq: u64,
}

impl LocalOrderbook {
    /// Apply a snapshot to reset the orderbook
    fn apply_snapshot(&mut self, snapshot: &OrderbookSnapshot) {
        self.yes_bids.clear();
        self.yes_asks.clear();
        self.no_bids.clear();
        self.no_asks.clear();

        // Kalshi returns bid/ask levels for yes and no
        for level in &snapshot.yes {
            if level.quantity > 0 {
                // Positive = bid, negative would be ask (but Kalshi separates them)
                self.yes_bids.insert(level.price, level.quantity);
            }
        }
        for level in &snapshot.no {
            if level.quantity > 0 {
                self.no_bids.insert(level.price, level.quantity);
            }
        }

        self.seq = snapshot.seq;
    }

    /// Apply a delta update
    fn apply_delta(&mut self, delta: &OrderbookDelta) {
        // Skip stale updates
        if delta.seq <= self.seq {
            return;
        }

        let book = match delta.side.as_str() {
            "yes" => &mut self.yes_bids,
            "no" => &mut self.no_bids,
            _ => return,
        };

        if delta.delta == 0 {
            book.remove(&delta.price);
        } else {
            let current = book.get(&delta.price).copied().unwrap_or(0);
            let new_qty = current + delta.delta;
            if new_qty <= 0 {
                book.remove(&delta.price);
            } else {
                book.insert(delta.price, new_qty);
            }
        }

        self.seq = delta.seq;
    }

    /// Get best prices from current orderbook state
    fn get_best_prices(&self) -> KalshiOrderbookUpdate {
        // Best bid = highest price someone will buy at
        let yes_best_bid = self.yes_bids.keys().max().map(|&p| cents_to_decimal(p));
        let no_best_bid = self.no_bids.keys().max().map(|&p| cents_to_decimal(p));

        // In Kalshi's model:
        // If someone bids YES at 60c, they're effectively offering NO at 40c (100-60)
        // YES_ask = 100 - NO_bid, NO_ask = 100 - YES_bid
        let yes_best_ask = no_best_bid.map(|p| dec!(1) - p);
        let no_best_ask = yes_best_bid.map(|p| dec!(1) - p);

        KalshiOrderbookUpdate {
            market_ticker: String::new(), // Will be filled in by caller
            yes_best_bid,
            yes_best_ask,
            no_best_bid,
            no_best_ask,
            timestamp: chrono::Utc::now(),
        }
    }
}

/// Convert Kalshi cents (1-99) to decimal dollars (0.01-0.99)
fn cents_to_decimal(cents: i32) -> Decimal {
    Decimal::from(cents) / dec!(100)
}

/// Kalshi WebSocket authentication
pub struct KalshiAuth {
    api_key: String,
    private_key: RsaPrivateKey,
}

impl KalshiAuth {
    /// Create new auth from API key and PEM private key
    pub fn new(api_key: String, private_key_pem: &str) -> Result<Self> {
        let private_key = RsaPrivateKey::from_pkcs8_pem(private_key_pem)
            .map_err(|e| anyhow!("Failed to parse RSA private key: {}", e))?;

        Ok(Self {
            api_key,
            private_key,
        })
    }

    /// Generate authentication headers for WebSocket connection
    pub fn generate_headers(&self) -> Result<Vec<(String, String)>> {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();

        // Message to sign: timestamp + method + path
        let method = "GET";
        let path = "/trade-api/ws/v2";
        let message = format!("{}{}{}", timestamp, method, path);

        // Sign with RSA-PSS SHA256
        let signing_key = SigningKey::<Sha256>::new(self.private_key.clone());
        let signature = signing_key.sign(message.as_bytes());
        let signature_b64 = BASE64.encode(signature.to_bytes());

        Ok(vec![
            ("KALSHI-ACCESS-KEY".to_string(), self.api_key.clone()),
            ("KALSHI-ACCESS-SIGNATURE".to_string(), signature_b64),
            ("KALSHI-ACCESS-TIMESTAMP".to_string(), timestamp.to_string()),
        ])
    }
}

/// Kalshi WebSocket client
pub struct KalshiWsClient {
    auth: Option<KalshiAuth>,
    ws_url: String,
    orderbooks: HashMap<String, LocalOrderbook>,
}

impl KalshiWsClient {
    /// Create new client without authentication (public data only)
    pub fn new() -> Self {
        Self {
            auth: None,
            ws_url: KALSHI_WS_URL.to_string(),
            orderbooks: HashMap::new(),
        }
    }

    /// Create new client with authentication
    pub fn with_auth(api_key: String, private_key_pem: &str) -> Result<Self> {
        let auth = KalshiAuth::new(api_key, private_key_pem)?;
        Ok(Self {
            auth: Some(auth),
            ws_url: KALSHI_WS_URL.to_string(),
            orderbooks: HashMap::new(),
        })
    }

    /// Connect and stream orderbook updates
    pub async fn stream_orderbooks(
        &mut self,
        tickers: Vec<String>,
        tx: mpsc::Sender<KalshiOrderbookUpdate>,
    ) -> Result<()> {
        info!("Connecting to Kalshi WebSocket at {}", self.ws_url);

        // Build connection request with auth headers if available
        let request = if let Some(auth) = &self.auth {
            let headers = auth.generate_headers()?;
            let mut req = http::Request::builder()
                .uri(&self.ws_url)
                .header("Host", "api.elections.kalshi.com");

            for (key, value) in headers {
                req = req.header(key.as_str(), value.as_str());
            }

            req.body(())?
        } else {
            http::Request::builder()
                .uri(&self.ws_url)
                .header("Host", "api.elections.kalshi.com")
                .body(())?
        };

        // Connect with timeout
        let (ws_stream, response) = timeout(Duration::from_secs(10), connect_async(request))
            .await
            .map_err(|_| anyhow!("WebSocket connection timeout"))?
            .map_err(|e| anyhow!("WebSocket connection failed: {}", e))?;

        info!(
            "Connected to Kalshi WebSocket (status: {})",
            response.status()
        );

        let (mut write, mut read) = ws_stream.split();

        // Subscribe to orderbook channel for all tickers
        let subscribe_msg = CommandMessage {
            id: 1,
            cmd: "subscribe".to_string(),
            params: CommandParams {
                channels: vec!["orderbook_delta".to_string()],
                market_tickers: Some(tickers.clone()),
            },
        };

        let msg_json = serde_json::to_string(&subscribe_msg)?;
        info!("Subscribing to {} markets", tickers.len());
        debug!("Subscribe message: {}", msg_json);
        write.send(Message::Text(msg_json.into())).await?;

        // Initialize local orderbooks
        for ticker in &tickers {
            self.orderbooks
                .insert(ticker.clone(), LocalOrderbook::default());
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
        tx: &mpsc::Sender<KalshiOrderbookUpdate>,
    ) -> Result<()> {
        // Try parsing as different message types
        // Kalshi uses a flexible message format

        // First try parsing the raw JSON to see the type
        let raw: serde_json::Value = serde_json::from_str(text)?;

        if let Some(msg_type) = raw.get("type").and_then(|t| t.as_str()) {
            match msg_type {
                "orderbook_snapshot" => {
                    let snapshot: OrderbookSnapshot = serde_json::from_value(raw)?;
                    self.handle_snapshot(snapshot, tx).await?;
                }
                "orderbook_delta" => {
                    let delta: OrderbookDelta = serde_json::from_value(raw)?;
                    self.handle_delta(delta, tx).await?;
                }
                "subscribed" => {
                    debug!("Subscription confirmed: {}", text);
                }
                "error" => {
                    let error: ErrorMessage = serde_json::from_value(raw)?;
                    error!("Kalshi error: {} (code: {:?})", error.msg, error.code);
                }
                _ => {
                    debug!("Unknown message type: {}", msg_type);
                }
            }
        } else {
            debug!("Message without type field: {}", text);
        }

        Ok(())
    }

    /// Handle orderbook snapshot
    async fn handle_snapshot(
        &mut self,
        snapshot: OrderbookSnapshot,
        tx: &mpsc::Sender<KalshiOrderbookUpdate>,
    ) -> Result<()> {
        let ticker = snapshot.market_ticker.clone();
        debug!("Received snapshot for {} (seq: {})", ticker, snapshot.seq);

        // Update local orderbook
        let book = self.orderbooks.entry(ticker.clone()).or_default();
        book.apply_snapshot(&snapshot);

        // Send update
        let mut update = book.get_best_prices();
        update.market_ticker = ticker;

        tx.send(update).await?;

        Ok(())
    }

    /// Handle orderbook delta
    async fn handle_delta(
        &mut self,
        delta: OrderbookDelta,
        tx: &mpsc::Sender<KalshiOrderbookUpdate>,
    ) -> Result<()> {
        let ticker = delta.market_ticker.clone();

        // Update local orderbook
        if let Some(book) = self.orderbooks.get_mut(&ticker) {
            book.apply_delta(&delta);

            // Send update
            let mut update = book.get_best_prices();
            update.market_ticker = ticker;

            tx.send(update).await?;
        }

        Ok(())
    }
}

impl Default for KalshiWsClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Run Kalshi orderbook streaming with reconnection logic
pub async fn run_kalshi_orderbook_stream(
    tickers: Vec<String>,
    tx: mpsc::Sender<KalshiOrderbookUpdate>,
    api_key: Option<String>,
    private_key_pem: Option<String>,
    reconnect_interval: Duration,
) -> Result<()> {
    loop {
        let mut client = match (&api_key, &private_key_pem) {
            (Some(key), Some(pem)) => KalshiWsClient::with_auth(key.clone(), pem)?,
            _ => KalshiWsClient::new(),
        };

        match client.stream_orderbooks(tickers.clone(), tx.clone()).await {
            Ok(_) => {
                info!("Kalshi WebSocket stream ended gracefully");
            }
            Err(e) => {
                error!("Kalshi WebSocket error: {}", e);
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
    fn test_cents_to_decimal() {
        assert_eq!(cents_to_decimal(50), dec!(0.50));
        assert_eq!(cents_to_decimal(99), dec!(0.99));
        assert_eq!(cents_to_decimal(1), dec!(0.01));
    }

    #[test]
    fn test_local_orderbook_snapshot() {
        let mut book = LocalOrderbook::default();

        let snapshot = OrderbookSnapshot {
            market_ticker: "KXBTC-25JAN13-T100000".to_string(),
            yes: vec![
                PriceLevel {
                    price: 55,
                    quantity: 100,
                },
                PriceLevel {
                    price: 54,
                    quantity: 200,
                },
            ],
            no: vec![PriceLevel {
                price: 46,
                quantity: 150,
            }],
            seq: 1,
        };

        book.apply_snapshot(&snapshot);

        assert_eq!(book.yes_bids.get(&55), Some(&100));
        assert_eq!(book.yes_bids.get(&54), Some(&200));
        assert_eq!(book.no_bids.get(&46), Some(&150));

        let prices = book.get_best_prices();
        assert_eq!(prices.yes_best_bid, Some(dec!(0.55)));
        assert_eq!(prices.no_best_bid, Some(dec!(0.46)));
        // YES ask = 100 - NO bid = 100 - 46 = 54
        assert_eq!(prices.yes_best_ask, Some(dec!(0.54)));
        // NO ask = 100 - YES bid = 100 - 55 = 45
        assert_eq!(prices.no_best_ask, Some(dec!(0.45)));
    }

    #[test]
    fn test_local_orderbook_delta() {
        let mut book = LocalOrderbook::default();

        // Apply initial snapshot
        let snapshot = OrderbookSnapshot {
            market_ticker: "KXBTC".to_string(),
            yes: vec![PriceLevel {
                price: 55,
                quantity: 100,
            }],
            no: vec![],
            seq: 1,
        };
        book.apply_snapshot(&snapshot);

        // Apply delta that adds quantity
        let delta = OrderbookDelta {
            market_ticker: "KXBTC".to_string(),
            price: 55,
            delta: 50,
            side: "yes".to_string(),
            seq: 2,
        };
        book.apply_delta(&delta);

        assert_eq!(book.yes_bids.get(&55), Some(&150));

        // Apply delta that removes quantity
        let delta2 = OrderbookDelta {
            market_ticker: "KXBTC".to_string(),
            price: 55,
            delta: -150,
            side: "yes".to_string(),
            seq: 3,
        };
        book.apply_delta(&delta2);

        assert!(book.yes_bids.get(&55).is_none());
    }
}
