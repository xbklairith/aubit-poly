//! Performance metrics collection.

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
