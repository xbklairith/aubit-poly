//! Signal detection and cooldown management.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use rust_decimal::Decimal;

/// Signal detector with cooldown management.
pub struct SignalDetector {
    pub min_momentum: Decimal,
    #[allow(dead_code)]
    pub lookback_minutes: usize,
    pub max_entry_price: Decimal,
    cooldown_duration: Duration,
    /// Map of condition_id -> last trade time
    cooldowns: HashMap<String, Instant>,
}

impl SignalDetector {
    pub fn new(
        min_momentum: Decimal,
        lookback_minutes: usize,
        max_entry_price: Decimal,
        cooldown_secs: u64,
    ) -> Self {
        Self {
            min_momentum,
            lookback_minutes,
            max_entry_price,
            cooldown_duration: Duration::from_secs(cooldown_secs),
            cooldowns: HashMap::new(),
        }
    }

    /// Check if we can trade a market (not in cooldown).
    pub fn can_trade(&self, condition_id: &str) -> bool {
        match self.cooldowns.get(condition_id) {
            Some(last_trade) => last_trade.elapsed() >= self.cooldown_duration,
            None => true,
        }
    }

    /// Record a trade for cooldown tracking.
    pub fn record_trade(&mut self, condition_id: &str) {
        self.cooldowns
            .insert(condition_id.to_string(), Instant::now());
    }

    /// Clean up old cooldown entries.
    pub fn cleanup_cooldowns(&mut self) {
        self.cooldowns
            .retain(|_, instant| instant.elapsed() < self.cooldown_duration * 2);
    }
}
