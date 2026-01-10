//! Trade executor state machine.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use alloy::signers::local::LocalSigner;
use alloy::signers::Signer;
use anyhow::{Context, Result};
use chrono::Utc;
use polymarket_client_sdk::clob::types::SignatureType;
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::POLYGON;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use common::repository::{self, MarketWithPrices};
use common::Database;

use crate::config::ExecutorConfig;
use crate::detector::SpreadDetector;
use crate::metrics::{CycleMetrics, MarketSummary};
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

        // Collect top 10 markets by profit (including negative)
        metrics.top_markets = self.get_top_markets(&markets, 10);

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
            self.execute_live_trade(opportunity, &details).await?;
        }

        Ok(true)
    }

    /// Execute a live trade on Polymarket.
    async fn execute_live_trade(
        &mut self,
        opportunity: &SpreadOpportunity,
        details: &TradeDetails,
    ) -> Result<()> {
        info!(
            "[LIVE] Placing orders for {} | YES: {:.4} @ ${:.4} | NO: {:.4} @ ${:.4}",
            opportunity.market_name,
            details.yes_shares, details.yes_price,
            details.no_shares, details.no_price
        );

        // Get credentials from environment
        let private_key = std::env::var("WALLET_PRIVATE_KEY")
            .context("Missing WALLET_PRIVATE_KEY for live trading")?;

        let private_key = if private_key.starts_with("0x") {
            private_key
        } else {
            format!("0x{}", private_key)
        };

        // Create signer
        let signer = LocalSigner::from_str(&private_key)
            .context("Invalid private key format")?
            .with_chain_id(Some(POLYGON));

        // Check for proxy wallet
        let proxy_wallet = std::env::var("POLYMARKET_WALLET_ADDRESS").ok();
        let signature_type = if proxy_wallet.is_some() {
            SignatureType::GnosisSafe
        } else {
            SignatureType::Eoa
        };

        // Authenticate with CLOB
        let mut auth_builder = ClobClient::new("https://clob.polymarket.com", ClobConfig::default())?
            .authentication_builder(&signer)
            .signature_type(signature_type);

        if let Some(ref proxy) = proxy_wallet {
            let funder_address: alloy::primitives::Address = proxy
                .parse()
                .context("Invalid proxy wallet address")?;
            auth_builder = auth_builder.funder(funder_address);
        }

        let clob_client = auth_builder
            .authenticate()
            .await
            .context("Failed to authenticate with Polymarket")?;

        // Round price and size to 2 decimal places (Polymarket requirement)
        let yes_size = details.yes_shares.round_dp(2);
        let yes_price = details.yes_price.round_dp(2);
        let no_size = details.no_shares.round_dp(2);
        let no_price = details.no_price.round_dp(2);

        // Check minimum order value ($1 minimum per Polymarket)
        let yes_value = yes_size * yes_price;
        let no_value = no_size * no_price;
        if yes_value < dec!(1) || no_value < dec!(1) {
            warn!(
                "[LIVE] Order value too low - YES: ${:.2}, NO: ${:.2} (min $1). Skipping.",
                yes_value, no_value
            );
            return Ok(());
        }

        // Place YES order
        info!(
            "[LIVE] Building YES order: token={}, size={}, price={}",
            opportunity.yes_token_id, yes_size, yes_price
        );
        let yes_order = clob_client
            .limit_order()
            .token_id(&opportunity.yes_token_id)
            .size(yes_size)
            .price(yes_price)
            .side(polymarket_client_sdk::clob::types::Side::Buy)
            .build()
            .await
            .with_context(|| format!(
                "Failed to build YES order: token={}, size={}, price={}",
                opportunity.yes_token_id, yes_size, yes_price
            ))?;

        let yes_signed = clob_client
            .sign(&signer, yes_order)
            .await
            .context("Failed to sign YES order")?;

        let yes_result = clob_client
            .post_order(yes_signed)
            .await
            .context("Failed to post YES order")?;

        info!("[LIVE] YES order result: {:?}", yes_result);

        // Check if YES order succeeded (must have order_id and no error)
        let yes_order_id = yes_result.first().and_then(|r| {
            if r.order_id.is_empty() {
                None
            } else if r.error_msg.as_ref().map(|e| !e.is_empty()).unwrap_or(false) {
                None
            } else {
                Some(r.order_id.clone())
            }
        });

        if yes_order_id.is_none() {
            let error_msg = yes_result.first()
                .and_then(|r| r.error_msg.clone())
                .unwrap_or_else(|| "Unknown error".to_string());
            error!("[LIVE] YES order failed: {}. Aborting trade.", error_msg);
            return Ok(());
        }

        // Check how many YES shares were actually filled
        // taking_amount = shares received, making_amount = USDC spent
        let yes_filled = yes_result.first()
            .map(|r| Decimal::try_from(r.taking_amount).unwrap_or(dec!(0)))
            .unwrap_or(dec!(0));

        // If partial fill, use filled amount for NO order to stay balanced
        let actual_no_size = if yes_filled > dec!(0) && yes_filled < yes_size {
            warn!(
                "[LIVE] YES order partial fill: requested {}, got {}. Adjusting NO to match.",
                yes_size, yes_filled
            );
            yes_filled.round_dp(2)
        } else if yes_filled == dec!(0) {
            // Order is Live (limit order waiting) - use original size
            // The order may fill later
            info!("[LIVE] YES order is Live (limit), proceeding with NO order");
            no_size
        } else {
            no_size
        };

        // Place NO order with matched size
        info!(
            "[LIVE] Building NO order: token={}, size={}, price={}",
            opportunity.no_token_id, actual_no_size, no_price
        );
        let no_order = clob_client
            .limit_order()
            .token_id(&opportunity.no_token_id)
            .size(actual_no_size)
            .price(no_price)
            .side(polymarket_client_sdk::clob::types::Side::Buy)
            .build()
            .await
            .with_context(|| format!(
                "Failed to build NO order: token={}, size={}, price={}",
                opportunity.no_token_id, no_size, no_price
            ))?;

        let no_signed = clob_client
            .sign(&signer, no_order)
            .await
            .context("Failed to sign NO order")?;

        let no_result = clob_client
            .post_order(no_signed)
            .await
            .context("Failed to post NO order")?;

        info!("[LIVE] NO order result: {:?}", no_result);

        // Check if NO order succeeded
        let no_order_id = no_result.first().and_then(|r| {
            if r.order_id.is_empty() {
                None
            } else if r.error_msg.as_ref().map(|e| !e.is_empty()).unwrap_or(false) {
                None
            } else {
                Some(r.order_id.clone())
            }
        });

        if no_order_id.is_none() {
            let error_msg = no_result.first()
                .and_then(|r| r.error_msg.clone())
                .unwrap_or_else(|| "Unknown error".to_string());
            error!("[LIVE] NO order failed: {}. Attempting to cancel YES order.", error_msg);

            // Try to cancel the YES order to avoid one-sided position
            if let Some(yes_id) = yes_order_id {
                match clob_client.cancel_order(&yes_id).await {
                    Ok(_) => info!("[LIVE] Cancelled YES order {}", yes_id),
                    Err(e) => error!("[LIVE] Failed to cancel YES order: {}", e),
                }
            }
            return Ok(());
        }

        // Create position in database (not dry_run)
        let position_id = repository::create_position(
            self.db.pool(),
            opportunity.market_id,
            details.yes_shares,
            details.no_shares,
            details.total_invested,
            false, // NOT dry_run
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
            "[LIVE] TRADE EXECUTED: {} | YES: {:.4} @ ${:.4} | NO: {:.4} @ ${:.4} | Invested: ${:.2}",
            opportunity.market_name,
            details.yes_shares, details.yes_price,
            details.no_shares, details.no_price,
            details.total_invested
        );

        Ok(())
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

    /// Get top N markets sorted by profit (highest first, including negative).
    fn get_top_markets(&self, markets: &[MarketWithPrices], limit: usize) -> Vec<MarketSummary> {
        let mut summaries: Vec<MarketSummary> = markets
            .iter()
            .filter_map(|m| {
                let yes_price = m.yes_best_ask?;
                let no_price = m.no_best_ask?;
                if yes_price <= dec!(0) || no_price <= dec!(0) {
                    return None;
                }
                let spread = yes_price + no_price;
                let profit_pct = dec!(1.00) - spread;
                Some(MarketSummary {
                    name: if m.name.chars().count() > 50 {
                        format!("{}..", m.name.chars().take(48).collect::<String>())
                    } else {
                        m.name.clone()
                    },
                    asset: m.asset.clone(),
                    yes_price,
                    no_price,
                    spread,
                    profit_pct,
                })
            })
            .collect();

        // Sort by profit (highest/best first)
        summaries.sort_by(|a, b| b.profit_pct.partial_cmp(&a.profit_pct).unwrap_or(std::cmp::Ordering::Equal));
        summaries.truncate(limit);
        summaries
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

            // Only show markets with actual profit opportunity (spread < $1.00)
            if spread >= dec!(1.00) {
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

            // Truncate name (use chars() for UTF-8 safety)
            let name = if market.name.chars().count() > 40 {
                format!("{}..", market.name.chars().take(38).collect::<String>())
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
            println!("  (no profitable markets found)");
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
