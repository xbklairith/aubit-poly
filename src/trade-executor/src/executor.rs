//! Trade executor state machine.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::{debug, info};
use uuid::Uuid;

use common::repository::{self, MarketWithPrices};
use common::Database;

use crate::config::ExecutorConfig;
use crate::detector::SpreadDetector;
use crate::metrics::CycleMetrics;
use crate::models::{BotState, PositionCache, SessionState, SpreadOpportunity, TradeDetails};

/// Trade executor - manages trading state and executes spread arbitrage.
pub struct TradeExecutor {
    config: ExecutorConfig,
    db: Arc<Database>,
    detector: SpreadDetector,
    state: BotState,
    session: SessionState,
}

impl TradeExecutor {
    /// Create a new trade executor.
    pub async fn new(config: ExecutorConfig, db: Arc<Database>) -> Result<Self> {
        let detector = SpreadDetector::new(config.min_profit, config.max_price_age_secs);

        let session = SessionState {
            id: Uuid::new_v4(),
            dry_run: config.dry_run,
            starting_balance: config.starting_balance,
            current_balance: config.starting_balance,
            total_trades: 0,
            winning_trades: 0,
            total_opportunities: 0,
            positions_opened: 0,
            positions_closed: 0,
            gross_profit: dec!(0),
            fees_paid: dec!(0),
            net_profit: dec!(0),
            started_at: Utc::now(),
            open_positions: HashMap::new(),
        };

        Ok(Self {
            config,
            db,
            detector,
            state: BotState::Idle,
            session,
        })
    }

    /// Run a single trading cycle.
    pub async fn run_cycle(&mut self, verbose: bool) -> Result<CycleMetrics> {
        let cycle_start = std::time::Instant::now();
        let mut metrics = CycleMetrics::new();

        self.state = BotState::Scanning;

        // Step 1: Fetch markets with fresh orderbooks
        let query_start = std::time::Instant::now();
        let markets = repository::get_markets_with_fresh_orderbooks(
            self.db.pool(),
            self.config.max_orderbook_age_secs,
            &self.config.assets,
            self.config.max_time_to_expiry_secs,
        )
        .await?;
        metrics.market_query_ms = query_start.elapsed().as_millis() as u64;
        metrics.markets_scanned = markets.len();

        debug!(
            "Fetched {} markets with fresh orderbooks in {}ms",
            markets.len(),
            metrics.market_query_ms
        );

        // Step 2: Detect spread opportunities
        let detect_start = std::time::Instant::now();
        let opportunities = self.detector.scan_markets(&markets);
        metrics.detection_ms = detect_start.elapsed().as_millis() as u64;
        metrics.opportunities_found = opportunities.len();

        self.session.total_opportunities += opportunities.len() as i32;

        // Print market analysis if verbose
        if verbose {
            self.print_market_analysis(&markets);
        }

        // Step 3: Execute on best opportunity
        if let Some(best) = opportunities.first() {
            self.state = BotState::Trading;

            let exec_start = std::time::Instant::now();
            if self.try_execute_trade(best).await? {
                metrics.trades_executed = 1;
            }
            metrics.execution_ms = exec_start.elapsed().as_millis() as u64;
        }

        // Step 4: Check for expired positions
        let settlement_start = std::time::Instant::now();
        let settled = self.check_and_settle_expired(&markets).await?;
        metrics.settlement_ms = settlement_start.elapsed().as_millis() as u64;
        metrics.positions_settled = settled;

        metrics.total_cycle_ms = cycle_start.elapsed().as_millis() as u64;
        self.state = BotState::Idle;

        Ok(metrics)
    }

    /// Attempt to execute a spread trade.
    async fn try_execute_trade(&mut self, opportunity: &SpreadOpportunity) -> Result<bool> {
        // Check if we already have a position in this market
        if self.session.open_positions.contains_key(&opportunity.market_id) {
            debug!("Already have position in {}", opportunity.market_name);
            return Ok(false);
        }

        // Calculate investment size
        let available = self.available_balance();
        let size = available.min(self.config.max_position_size);

        if size < dec!(10) {
            debug!("Insufficient balance for trade: ${}", available);
            return Ok(false);
        }

        // Check max exposure
        if self.total_exposure() + size > self.config.max_total_exposure {
            debug!("Would exceed max exposure");
            return Ok(false);
        }

        // Calculate trade details
        let details = self.detector.calculate_trade_details(
            opportunity,
            size,
            self.config.fee_rate,
        );

        // Execute trade (dry run or live)
        if self.config.dry_run {
            self.execute_dry_run(opportunity, &details).await?;
        } else {
            // Live trading would go here
            unimplemented!("Live trading not yet implemented");
        }

        Ok(true)
    }

    /// Execute a dry-run (simulated) trade.
    async fn execute_dry_run(
        &mut self,
        opportunity: &SpreadOpportunity,
        details: &TradeDetails,
    ) -> Result<()> {
        // Create position in database
        let position_id = repository::create_position(
            self.db.pool(),
            opportunity.market_id,
            details.yes_shares,
            details.no_shares,
            details.total_invested,
            true, // dry_run
        )
        .await?;

        // Record trades
        repository::record_trade(
            self.db.pool(),
            position_id,
            "yes",
            "buy",
            details.yes_price,
            details.yes_shares,
        )
        .await?;

        repository::record_trade(
            self.db.pool(),
            position_id,
            "no",
            "buy",
            details.no_price,
            details.no_shares,
        )
        .await?;

        // Update session state
        self.session.current_balance -= details.total_invested;
        self.session.total_trades += 2;
        self.session.positions_opened += 1;
        self.session.fees_paid += details.fee;

        // Cache position
        self.session.open_positions.insert(
            opportunity.market_id,
            PositionCache {
                id: position_id,
                market_id: opportunity.market_id,
                market_name: opportunity.market_name.clone(),
                yes_shares: details.yes_shares,
                no_shares: details.no_shares,
                total_invested: details.total_invested,
                end_time: opportunity.end_time,
            },
        );

        info!(
            "[DRY RUN] TRADE: {} | YES: {:.4} @ ${:.4} | NO: {:.4} @ ${:.4} | Invested: ${:.2} | Expected profit: ${:.4}",
            opportunity.market_name,
            details.yes_shares, details.yes_price,
            details.no_shares, details.no_price,
            details.total_invested, details.net_profit
        );

        Ok(())
    }

    /// Check for expired positions and settle them.
    async fn check_and_settle_expired(
        &mut self,
        _markets: &[MarketWithPrices],
    ) -> Result<usize> {
        let now = Utc::now();
        let mut settled = 0;

        // Find expired positions
        let expired: Vec<_> = self
            .session
            .open_positions
            .iter()
            .filter(|(_, p)| p.end_time < now)
            .map(|(id, p)| (*id, p.clone()))
            .collect();

        for (market_id, position) in expired {
            // Simulate settlement (assume YES wins for simplicity in dry-run)
            // In reality, we would query the market resolution
            let payout = position.yes_shares;
            let profit = payout - position.total_invested;

            // Close position in database
            repository::close_position(
                self.db.pool(),
                position.id,
                payout,
                profit,
            )
            .await?;

            // Update session
            self.session.current_balance += payout;
            self.session.positions_closed += 1;
            self.session.gross_profit += profit;
            self.session.net_profit += profit;
            if profit > dec!(0) {
                self.session.winning_trades += 1;
            }

            // Remove from cache
            self.session.open_positions.remove(&market_id);

            info!(
                "[DRY RUN] SETTLED: {} | Payout: ${:.2} | P/L: ${:+.4}",
                position.market_name, payout, profit
            );

            settled += 1;
        }

        Ok(settled)
    }

    /// Get available balance (not tied up in positions).
    fn available_balance(&self) -> Decimal {
        self.session.current_balance - self.total_exposure()
    }

    /// Get total exposure across all open positions.
    fn total_exposure(&self) -> Decimal {
        self.session
            .open_positions
            .values()
            .map(|p| p.total_invested)
            .sum()
    }

    /// Get current session state.
    pub fn session(&self) -> &SessionState {
        &self.session
    }

    /// Get current bot state.
    pub fn state(&self) -> BotState {
        self.state
    }

    /// Print market analysis table (like Python's _print_status).
    fn print_market_analysis(&self, markets: &[MarketWithPrices]) {
        let min_profit = self.config.min_profit;

        println!("\n📊 Market Analysis (from DB):");
        println!(
            "  {:<8} {:<40} {:>6} {:>6} {:>7} {:>7} {}",
            "Type", "Market", "YES", "NO", "Spread", "Profit", "Decision"
        );
        println!(
            "  {:-<8} {:-<40} {:-<6} {:-<6} {:-<7} {:-<7} {:-<10}",
            "", "", "", "", "", "", ""
        );

        let mut near_profitable_count = 0;

        // Sort by end_time
        let mut sorted_markets: Vec<_> = markets.iter().collect();
        sorted_markets.sort_by_key(|m| m.end_time);

        for market in sorted_markets {
            let yes_ask = match market.yes_best_ask {
                Some(p) => p,
                None => continue,
            };
            let no_ask = match market.no_best_ask {
                Some(p) => p,
                None => continue,
            };

            let spread = yes_ask + no_ask;
            let profit_pct = dec!(1.00) - spread;

            // Only show markets with spread < 1.1 (near profitable)
            if spread >= dec!(1.10) {
                continue;
            }
            near_profitable_count += 1;

            // Determine decision
            let decision = if yes_ask <= dec!(0) || no_ask <= dec!(0) {
                "❌ No price".to_string()
            } else if profit_pct >= min_profit {
                "✅ TRADE!".to_string()
            } else if profit_pct > dec!(0) {
                format!("⏳ +{:.1}% < {:.0}%", profit_pct * dec!(100), min_profit * dec!(100))
            } else {
                format!("❌ {:.1}%", profit_pct * dec!(100))
            };

            // Type abbreviation
            let type_abbrev = match market.market_type.as_str() {
                "up_down" => "UP/DOWN",
                "above" => "ABOVE",
                "price_range" => "RANGE",
                "sports" => "SPORTS",
                _ => "OTHER",
            };

            // Truncate name
            let name = if market.name.len() > 40 {
                format!("{}..", &market.name[..38])
            } else {
                market.name.clone()
            };

            println!(
                "  {:<8} {:<40} ${:.2} ${:.2} ${:.3} {:>+.1}% {}",
                type_abbrev,
                name,
                yes_ask,
                no_ask,
                spread,
                profit_pct * dec!(100),
                decision
            );
        }

        if near_profitable_count == 0 {
            println!("  (no markets with spread < 1.1)");
        }

        // Print status summary
        let return_pct = if self.session.starting_balance > dec!(0) {
            (self.session.net_profit / self.session.starting_balance) * dec!(100)
        } else {
            dec!(0)
        };

        println!(
            "\nBalance: ${:.2} | Positions: {} | Trades: {} | P/L: ${:+.2} ({:+.2}%)",
            self.session.current_balance,
            self.session.open_positions.len(),
            self.session.total_trades,
            self.session.net_profit,
            return_pct
        );
    }
}
