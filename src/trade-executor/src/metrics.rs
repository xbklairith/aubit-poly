//! Performance metrics collection.

use rust_decimal::Decimal;

/// Summary of a market's spread opportunity (for reporting).
#[derive(Debug, Clone)]
pub struct MarketSummary {
    pub name: String,
    pub asset: String,
    pub yes_price: Decimal,
    pub no_price: Decimal,
    pub spread: Decimal,
    pub profit_pct: Decimal,
}

/// Per-cycle performance metrics.
#[derive(Debug, Default, Clone)]
pub struct CycleMetrics {
    pub total_cycle_ms: u64,
    pub market_query_ms: u64,
    pub detection_ms: u64,
    pub execution_ms: u64,
    pub settlement_ms: u64,
    pub markets_scanned: usize,
    pub opportunities_found: usize,
    pub trades_executed: usize,
    pub positions_settled: usize,
    pub top_markets: Vec<MarketSummary>,
}

impl CycleMetrics {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Aggregate metrics for benchmarking.
#[derive(Debug, Clone)]
pub struct AggregateMetrics {
    pub cycles: usize,
    pub total_time_ms: u64,
    pub avg_cycle_ms: f64,
    pub min_cycle_ms: u64,
    pub max_cycle_ms: u64,
    pub p50_cycle_ms: u64,
    pub p95_cycle_ms: u64,
    pub p99_cycle_ms: u64,
    // Breakdown averages
    pub avg_query_ms: f64,
    pub avg_detection_ms: f64,
    pub avg_execution_ms: f64,
}

impl AggregateMetrics {
    /// Calculate aggregate metrics from a list of cycle metrics.
    pub fn from_cycles(cycles: &[CycleMetrics]) -> Self {
        if cycles.is_empty() {
            return Self {
                cycles: 0,
                total_time_ms: 0,
                avg_cycle_ms: 0.0,
                min_cycle_ms: 0,
                max_cycle_ms: 0,
                p50_cycle_ms: 0,
                p95_cycle_ms: 0,
                p99_cycle_ms: 0,
                avg_query_ms: 0.0,
                avg_detection_ms: 0.0,
                avg_execution_ms: 0.0,
            };
        }

        let mut cycle_times: Vec<u64> = cycles.iter().map(|c| c.total_cycle_ms).collect();
        cycle_times.sort();

        let count = cycle_times.len();
        let total: u64 = cycle_times.iter().sum();

        let query_total: u64 = cycles.iter().map(|c| c.market_query_ms).sum();
        let detect_total: u64 = cycles.iter().map(|c| c.detection_ms).sum();
        let exec_total: u64 = cycles.iter().map(|c| c.execution_ms).sum();

        Self {
            cycles: count,
            total_time_ms: total,
            avg_cycle_ms: total as f64 / count as f64,
            min_cycle_ms: *cycle_times.first().unwrap_or(&0),
            max_cycle_ms: *cycle_times.last().unwrap_or(&0),
            p50_cycle_ms: cycle_times.get(count / 2).copied().unwrap_or(0),
            p95_cycle_ms: cycle_times.get(count * 95 / 100).copied().unwrap_or(0),
            p99_cycle_ms: cycle_times.get(count * 99 / 100).copied().unwrap_or(0),
            avg_query_ms: query_total as f64 / count as f64,
            avg_detection_ms: detect_total as f64 / count as f64,
            avg_execution_ms: exec_total as f64 / count as f64,
        }
    }

    /// Print a formatted benchmark report.
    pub fn print_report(&self, title: &str) {
        println!(
            r#"
═══════════════════════════════════════════════════════════════
  {}
═══════════════════════════════════════════════════════════════
  Cycles run:       {}
  Total time:       {}ms

  Cycle Time (ms):
    Average:        {:.2}
    Min:            {}
    Max:            {}
    P50:            {}
    P95:            {}
    P99:            {}

  Breakdown (avg ms):
    DB Query:       {:.2}
    Detection:      {:.2}
    Execution:      {:.2}
═══════════════════════════════════════════════════════════════
"#,
            title,
            self.cycles,
            self.total_time_ms,
            self.avg_cycle_ms,
            self.min_cycle_ms,
            self.max_cycle_ms,
            self.p50_cycle_ms,
            self.p95_cycle_ms,
            self.p99_cycle_ms,
            self.avg_query_ms,
            self.avg_detection_ms,
            self.avg_execution_ms,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    // ============ CycleMetrics TESTS ============

    #[test]
    fn test_cycle_metrics_new_has_defaults() {
        let metrics = CycleMetrics::new();

        assert_eq!(metrics.total_cycle_ms, 0);
        assert_eq!(metrics.market_query_ms, 0);
        assert_eq!(metrics.detection_ms, 0);
        assert_eq!(metrics.execution_ms, 0);
        assert_eq!(metrics.settlement_ms, 0);
        assert_eq!(metrics.markets_scanned, 0);
        assert_eq!(metrics.opportunities_found, 0);
        assert_eq!(metrics.trades_executed, 0);
        assert_eq!(metrics.positions_settled, 0);
        assert!(metrics.top_markets.is_empty());
    }

    #[test]
    fn test_cycle_metrics_default_trait() {
        let metrics = CycleMetrics::default();
        assert_eq!(metrics.total_cycle_ms, 0);
    }

    // ============ MarketSummary TESTS ============

    #[test]
    fn test_market_summary_creation() {
        let summary = MarketSummary {
            name: "Will BTC go up in the next hour?".to_string(),
            asset: "BTC".to_string(),
            yes_price: dec!(0.45),
            no_price: dec!(0.45),
            spread: dec!(0.90),
            profit_pct: dec!(0.10),
        };

        assert_eq!(summary.asset, "BTC");
        assert_eq!(summary.profit_pct, dec!(0.10));
    }

    #[test]
    fn test_market_summary_long_name() {
        // Test that long names are stored correctly (truncation happens elsewhere)
        let long_name = "A".repeat(100);
        let summary = MarketSummary {
            name: long_name.clone(),
            asset: "ETH".to_string(),
            yes_price: dec!(0.50),
            no_price: dec!(0.50),
            spread: dec!(1.00),
            profit_pct: dec!(0.00),
        };

        assert_eq!(summary.name.len(), 100);
    }

    // ============ AggregateMetrics TESTS ============

    #[test]
    fn test_aggregate_metrics_empty_cycles() {
        let aggregate = AggregateMetrics::from_cycles(&[]);

        assert_eq!(aggregate.cycles, 0);
        assert_eq!(aggregate.total_time_ms, 0);
        assert_eq!(aggregate.avg_cycle_ms, 0.0);
        assert_eq!(aggregate.min_cycle_ms, 0);
        assert_eq!(aggregate.max_cycle_ms, 0);
    }

    #[test]
    fn test_aggregate_metrics_single_cycle() {
        let cycle = CycleMetrics {
            total_cycle_ms: 100,
            market_query_ms: 50,
            detection_ms: 20,
            execution_ms: 30,
            ..Default::default()
        };

        let aggregate = AggregateMetrics::from_cycles(&[cycle]);

        assert_eq!(aggregate.cycles, 1);
        assert_eq!(aggregate.total_time_ms, 100);
        assert_eq!(aggregate.avg_cycle_ms, 100.0);
        assert_eq!(aggregate.min_cycle_ms, 100);
        assert_eq!(aggregate.max_cycle_ms, 100);
    }

    #[test]
    fn test_aggregate_metrics_multiple_cycles() {
        let cycles = vec![
            CycleMetrics {
                total_cycle_ms: 50,
                market_query_ms: 20,
                detection_ms: 10,
                execution_ms: 15,
                ..Default::default()
            },
            CycleMetrics {
                total_cycle_ms: 100,
                market_query_ms: 40,
                detection_ms: 20,
                execution_ms: 30,
                ..Default::default()
            },
            CycleMetrics {
                total_cycle_ms: 150,
                market_query_ms: 60,
                detection_ms: 30,
                execution_ms: 45,
                ..Default::default()
            },
        ];

        let aggregate = AggregateMetrics::from_cycles(&cycles);

        assert_eq!(aggregate.cycles, 3);
        assert_eq!(aggregate.total_time_ms, 300); // 50 + 100 + 150
        assert_eq!(aggregate.avg_cycle_ms, 100.0); // 300 / 3
        assert_eq!(aggregate.min_cycle_ms, 50);
        assert_eq!(aggregate.max_cycle_ms, 150);
        assert_eq!(aggregate.avg_query_ms, 40.0); // (20 + 40 + 60) / 3
        assert_eq!(aggregate.avg_detection_ms, 20.0); // (10 + 20 + 30) / 3
        assert_eq!(aggregate.avg_execution_ms, 30.0); // (15 + 30 + 45) / 3
    }

    #[test]
    fn test_aggregate_metrics_percentiles() {
        // Create 100 cycles with increasing times (1 to 100)
        let cycles: Vec<CycleMetrics> = (1..=100)
            .map(|i| CycleMetrics {
                total_cycle_ms: i as u64,
                ..Default::default()
            })
            .collect();

        let aggregate = AggregateMetrics::from_cycles(&cycles);

        assert_eq!(aggregate.cycles, 100);
        assert_eq!(aggregate.min_cycle_ms, 1);
        assert_eq!(aggregate.max_cycle_ms, 100);
        // Note: p50 is index 50 (0-based), which is value 51 (since values are 1-100)
        assert_eq!(aggregate.p50_cycle_ms, 51); // index 50 = value 51
        assert_eq!(aggregate.p95_cycle_ms, 96); // index 95 = value 96
        assert_eq!(aggregate.p99_cycle_ms, 100); // index 99 = value 100
    }
}
