//! Configuration loading from environment variables.

use std::env;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Missing required environment variable: {0}")]
    MissingVar(String),

    #[error("Invalid URL format: {0}")]
    InvalidUrl(String),
}

/// Application configuration loaded from environment.
#[derive(Debug, Clone)]
pub struct Config {
    /// PostgreSQL connection URL
    pub database_url: String,

    /// Gamma API base URL
    pub gamma_api_url: String,

    /// CLOB WebSocket URL
    pub clob_ws_url: String,

    /// Market scanner poll interval in seconds
    pub scan_interval_secs: u64,
}

impl Config {
    /// Load configuration from environment variables.
    ///
    /// Required variables:
    /// - DATABASE_URL: PostgreSQL connection string
    ///
    /// Optional variables (with defaults):
    /// - GAMMA_API_URL: Gamma API base URL
    /// - CLOB_WS_URL: CLOB WebSocket URL
    /// - SCAN_INTERVAL_SECS: Poll interval (default: 60)
    pub fn from_env() -> Result<Self, ConfigError> {
        // Load .env file if present
        dotenvy::dotenv().ok();
        Self::from_env_only()
    }

    /// Load configuration from environment variables only (no .env file).
    /// Useful for testing.
    pub fn from_env_only() -> Result<Self, ConfigError> {
        let database_url = env::var("DATABASE_URL")
            .map_err(|_| ConfigError::MissingVar("DATABASE_URL".to_string()))?;

        let gamma_api_url = env::var("GAMMA_API_URL")
            .unwrap_or_else(|_| "https://gamma-api.polymarket.com".to_string());

        let clob_ws_url = env::var("CLOB_WS_URL")
            .unwrap_or_else(|_| "wss://ws-subscriptions-clob.polymarket.com/ws".to_string());

        let scan_interval_secs = env::var("SCAN_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(60);

        Ok(Self {
            database_url,
            gamma_api_url,
            clob_ws_url,
            scan_interval_secs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_config_missing_database_url() {
        // Clear DATABASE_URL if set
        env::remove_var("DATABASE_URL");

        // Use from_env_only to avoid .env file loading
        let result = Config::from_env_only();
        assert!(result.is_err());

        if let Err(ConfigError::MissingVar(var)) = result {
            assert_eq!(var, "DATABASE_URL");
        } else {
            panic!("Expected MissingVar error");
        }
    }

    #[test]
    #[serial]
    fn test_config_with_defaults() {
        env::set_var("DATABASE_URL", "postgres://localhost/test");

        // Use from_env_only to test just env vars
        let config = Config::from_env_only().unwrap();

        assert_eq!(config.database_url, "postgres://localhost/test");
        assert_eq!(config.gamma_api_url, "https://gamma-api.polymarket.com");
        assert_eq!(config.clob_ws_url, "wss://ws-subscriptions-clob.polymarket.com/ws");
        assert_eq!(config.scan_interval_secs, 60);

        // Cleanup
        env::remove_var("DATABASE_URL");
    }
}
