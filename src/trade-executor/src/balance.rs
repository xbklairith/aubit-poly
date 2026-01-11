//! Balance checking for rebalance sell operations.
//!
//! Queries actual token holdings from Gamma Data API before attempting sells,
//! preventing "not enough balance / allowance" errors.

use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::{info, warn};

/// Position data from Gamma Data API.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PositionData {
    /// Token ID (the "asset" field from API)
    pub asset: String,
    /// Amount held (API returns float)
    pub size: f64,
    /// Average price paid
    #[serde(default)]
    pub avg_price: Option<f64>,
}

/// Trait for checking token balances.
/// Mockable for testing via mockall.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait BalanceChecker: Send + Sync {
    /// Get the balance of a specific token.
    async fn get_token_balance(&self, token_id: &str) -> Result<Decimal>;

    /// Get all positions for the user.
    async fn get_all_positions(&self) -> Result<Vec<PositionData>>;
}

/// Gamma Data API balance checker.
pub struct GammaBalanceChecker {
    client: reqwest::Client,
    base_url: String,
    user_address: String,
}

impl GammaBalanceChecker {
    /// Create a new Gamma balance checker.
    pub fn new(base_url: &str, user_address: &str) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("Failed to create HTTP client"),
            base_url: base_url.to_string(),
            user_address: user_address.to_lowercase(),
        }
    }
}

#[async_trait]
impl BalanceChecker for GammaBalanceChecker {
    async fn get_token_balance(&self, token_id: &str) -> Result<Decimal> {
        let positions = self.get_all_positions().await?;
        let balance = find_balance(&positions, token_id);

        info!(
            "[BALANCE] Token {} balance: {}",
            &token_id[..20.min(token_id.len())],
            balance
        );

        Ok(balance)
    }

    async fn get_all_positions(&self) -> Result<Vec<PositionData>> {
        let url = format!("{}/positions?user={}", self.base_url, self.user_address);

        info!("[BALANCE] Fetching positions from {}", url);

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(anyhow::anyhow!(
                "[BALANCE] Failed to fetch positions: HTTP {}",
                status
            ));
        }

        let positions: Vec<PositionData> = response.json().await?;
        info!("[BALANCE] Fetched {} positions", positions.len());

        Ok(positions)
    }
}

/// Calculate safe sell amount (minimum of imbalance and actual balance).
pub fn calculate_safe_sell_amount(imbalance: Decimal, actual_balance: Decimal) -> Decimal {
    imbalance.min(actual_balance)
}

/// Find balance for a specific token from positions list.
pub fn find_balance(positions: &[PositionData], token_id: &str) -> Decimal {
    positions
        .iter()
        .find(|p| p.asset == token_id)
        .map(|p| {
            Decimal::try_from(p.size).unwrap_or_else(|e| {
                warn!(
                    "[BALANCE] Failed to convert size {} for asset {}: {:?}",
                    p.size,
                    &p.asset[..20.min(p.asset.len())],
                    e
                );
                Decimal::ZERO
            })
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    // ============ RED PHASE TESTS ============

    #[test]
    fn test_parse_positions_response() {
        let json = r#"[{"asset":"12345","size":10.5,"avgPrice":0.62}]"#;
        let positions: Vec<PositionData> = serde_json::from_str(json).unwrap();

        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].asset, "12345");
        assert_eq!(positions[0].size, 10.5);
        assert_eq!(positions[0].avg_price, Some(0.62));
    }

    #[test]
    fn test_parse_positions_response_without_avg_price() {
        let json = r#"[{"asset":"12345","size":10.5}]"#;
        let positions: Vec<PositionData> = serde_json::from_str(json).unwrap();

        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].asset, "12345");
        assert_eq!(positions[0].size, 10.5);
        assert_eq!(positions[0].avg_price, None);
    }

    #[test]
    fn test_parse_empty_positions() {
        let json = r#"[]"#;
        let positions: Vec<PositionData> = serde_json::from_str(json).unwrap();
        assert!(positions.is_empty());
    }

    #[test]
    fn test_sell_amount_capped_to_actual_balance() {
        let imbalance = dec!(10.2);
        let actual_balance = dec!(5.0); // Only have 5 tokens

        let sell_amount = calculate_safe_sell_amount(imbalance, actual_balance);

        assert_eq!(sell_amount, dec!(5.0)); // Should sell only what we have
    }

    #[test]
    fn test_sell_amount_uses_imbalance_when_balance_sufficient() {
        let imbalance = dec!(10.2);
        let actual_balance = dec!(15.0); // Have more than needed

        let sell_amount = calculate_safe_sell_amount(imbalance, actual_balance);

        assert_eq!(sell_amount, dec!(10.2)); // Should sell full imbalance
    }

    #[test]
    fn test_sell_amount_zero_when_no_balance() {
        let imbalance = dec!(10.2);
        let actual_balance = dec!(0);

        let sell_amount = calculate_safe_sell_amount(imbalance, actual_balance);

        assert_eq!(sell_amount, dec!(0));
    }

    #[test]
    fn test_sell_amount_handles_equal_values() {
        let imbalance = dec!(10.2);
        let actual_balance = dec!(10.2);

        let sell_amount = calculate_safe_sell_amount(imbalance, actual_balance);

        assert_eq!(sell_amount, dec!(10.2));
    }

    #[tokio::test]
    async fn test_mock_balance_checker_returns_balance() {
        let mut mock = MockBalanceChecker::new();
        mock.expect_get_token_balance()
            .withf(|token_id| token_id == "token123")
            .times(1)
            .returning(|_| Ok(dec!(10.5)));

        let balance = mock.get_token_balance("token123").await.unwrap();
        assert_eq!(balance, dec!(10.5));
    }

    #[tokio::test]
    async fn test_mock_balance_checker_returns_zero_for_unknown_token() {
        let mut mock = MockBalanceChecker::new();
        mock.expect_get_token_balance()
            .withf(|token_id| token_id == "unknown_token")
            .times(1)
            .returning(|_| Ok(dec!(0)));

        let balance = mock.get_token_balance("unknown_token").await.unwrap();
        assert_eq!(balance, dec!(0));
    }

    #[tokio::test]
    async fn test_mock_get_all_positions() {
        let mut mock = MockBalanceChecker::new();
        mock.expect_get_all_positions().times(1).returning(|| {
            Ok(vec![
                PositionData {
                    asset: "token1".to_string(),
                    size: 10.5,
                    avg_price: Some(0.62),
                },
                PositionData {
                    asset: "token2".to_string(),
                    size: 5.0,
                    avg_price: None,
                },
            ])
        });

        let positions = mock.get_all_positions().await.unwrap();
        assert_eq!(positions.len(), 2);
        assert_eq!(positions[0].asset, "token1");
        assert_eq!(positions[1].asset, "token2");
    }

    #[test]
    fn test_find_balance_returns_correct_amount() {
        let positions = vec![
            PositionData {
                asset: "token1".to_string(),
                size: 10.5,
                avg_price: None,
            },
            PositionData {
                asset: "token2".to_string(),
                size: 5.25,
                avg_price: None,
            },
        ];

        assert_eq!(find_balance(&positions, "token1"), dec!(10.5));
        assert_eq!(find_balance(&positions, "token2"), dec!(5.25));
    }

    #[test]
    fn test_find_balance_returns_zero_for_unknown_token() {
        let positions = vec![PositionData {
            asset: "token1".to_string(),
            size: 10.5,
            avg_price: None,
        }];

        assert_eq!(find_balance(&positions, "unknown"), dec!(0));
    }

    #[test]
    fn test_find_balance_returns_zero_for_empty_positions() {
        let positions: Vec<PositionData> = vec![];
        assert_eq!(find_balance(&positions, "any_token"), dec!(0));
    }
}
