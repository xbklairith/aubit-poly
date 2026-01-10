//! CLOB operations trait for testable trading.
//!
//! This module provides a trait abstraction over Polymarket's CLOB API,
//! allowing the trade executor to use a mock implementation in tests.

use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;

/// Order placement result.
#[derive(Debug, Clone)]
pub struct OrderResult {
    /// Order ID if successfully placed
    pub order_id: Option<String>,
    /// Amount filled (for immediate fills)
    pub filled_amount: Decimal,
    /// Error message if order failed
    pub error: Option<String>,
}

impl OrderResult {
    /// Check if the order was successful (has order_id and no error)
    pub fn is_success(&self) -> bool {
        self.order_id.is_some() && self.error.is_none()
    }
}

/// Order info from API.
#[derive(Debug, Clone)]
pub struct OrderInfo {
    /// Order ID
    pub order_id: String,
    /// Amount matched/filled
    pub size_matched: Decimal,
    /// Order status
    pub status: String,
}

/// Limit order parameters.
#[derive(Debug, Clone)]
pub struct LimitOrderParams {
    /// Token ID (YES or NO token)
    pub token_id: String,
    /// Order size in shares
    pub size: Decimal,
    /// Order price
    pub price: Decimal,
    /// Order side (buy or sell)
    pub side: OrderSide,
}

/// Order side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
}

/// Trait for CLOB operations - mockable for testing.
///
/// This trait abstracts the Polymarket CLOB API operations needed
/// for spread arbitrage trading. Implementations can be:
/// - `PolymarketClob`: Real API wrapper for production
/// - `MockClobOperations`: Auto-generated mock for testing
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait ClobOperations: Send + Sync {
    /// Place a limit order.
    async fn place_limit_order(&self, params: LimitOrderParams) -> Result<OrderResult>;

    /// Place a market order (for rebalancing).
    async fn place_market_order(
        &self,
        token_id: &str,
        amount: Decimal,
        side: OrderSide,
    ) -> Result<OrderResult>;

    /// Cancel an order.
    async fn cancel_order(&self, order_id: &str) -> Result<()>;

    /// Get order info.
    async fn get_order(&self, order_id: &str) -> Result<OrderInfo>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_order_result_success() {
        let result = OrderResult {
            order_id: Some("order-123".to_string()),
            filled_amount: dec!(10),
            error: None,
        };
        assert!(result.is_success());
    }

    #[test]
    fn test_order_result_failure_no_id() {
        let result = OrderResult {
            order_id: None,
            filled_amount: dec!(0),
            error: Some("Insufficient funds".to_string()),
        };
        assert!(!result.is_success());
    }

    #[test]
    fn test_order_result_failure_with_error() {
        let result = OrderResult {
            order_id: Some("order-123".to_string()),
            filled_amount: dec!(0),
            error: Some("Rate limited".to_string()),
        };
        assert!(!result.is_success());
    }

    #[test]
    fn test_order_side_equality() {
        assert_eq!(OrderSide::Buy, OrderSide::Buy);
        assert_ne!(OrderSide::Buy, OrderSide::Sell);
    }
}
