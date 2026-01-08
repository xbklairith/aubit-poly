//! Database connection and query helpers.

use sqlx::postgres::{PgPool, PgPoolOptions};
use thiserror::Error;

use crate::Config;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("Failed to connect to database: {0}")]
    ConnectionError(#[from] sqlx::Error),

    #[error("Query failed: {0}")]
    QueryError(String),
}

/// Database connection pool wrapper.
#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    /// Create a new database connection pool.
    pub async fn connect(config: &Config) -> Result<Self, DbError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&config.database_url)
            .await?;

        Ok(Self { pool })
    }

    /// Get a reference to the connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Check if the database connection is healthy.
    pub async fn health_check(&self) -> Result<(), DbError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_db_connection() {
        // Load config (requires DATABASE_URL in .env)
        dotenvy::dotenv().ok();
        let config = Config::from_env().expect("Config should load");

        // Test connection
        let db = Database::connect(&config).await;
        assert!(db.is_ok(), "Should connect to database");

        // Test health check
        let db = db.unwrap();
        let health = db.health_check().await;
        assert!(health.is_ok(), "Health check should pass");
    }
}
