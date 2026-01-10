//! Trade executor state machine.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use alloy::signers::local::LocalSigner;
use alloy::signers::Signer;
use anyhow::{Context, Result};
use chrono::Utc;
use polymarket_client_sdk::clob::types::SignatureType;
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::POLYGON;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Timeout for order operations (build, sign, post)
const ORDER_TIMEOUT_SECS: u64 = 30;

/// Maximum retries for order cancellation
const CANCEL_MAX_RETRIES: u32 = 3;

/// Delay between cancellation retries
const CANCEL_RETRY_DELAY_MS: u64 = 500;

use common::models::OrderbookSnapshot;
use common::repository::{self, MarketWithPrices};
use common::Database;

/// Calculate available liquidity from orderbook depth at best ask price.
/// Returns the minimum USDC value available at best ask between YES and NO sides.
/// (For spread arb, we need liquidity on BOTH sides - limited by the smaller one)
fn calculate_orderbook_liquidity(snapshot: &OrderbookSnapshot) -> Decimal {
    // Parse YES asks depth
    let yes_liquidity = snapshot
        .yes_asks
        .as_ref()
        .and_then(|asks| asks.as_array())
        .and_then(|arr| arr.first())
        .map(|best_ask| {
            let size = best_ask
                .get("size")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<Decimal>().ok())
                .unwrap_or(Decimal::ZERO);
            let price = best_ask
                .get("price")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<Decimal>().ok())
                .unwrap_or(Decimal::ZERO);
            size * price
        })
        .unwrap_or(Decimal::ZERO);

    // Parse NO asks depth
    let no_liquidity = snapshot
        .no_asks
        .as_ref()
        .and_then(|asks| asks.as_array())
        .and_then(|arr| arr.first())
        .map(|best_ask| {
            let size = best_ask
                .get("size")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<Decimal>().ok())
                .unwrap_or(Decimal::ZERO);
            let price = best_ask
                .get("price")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<Decimal>().ok())
                .unwrap_or(Decimal::ZERO);
            size * price
        })
        .unwrap_or(Decimal::ZERO);

    // Use minimum - can only trade as much as the thinner side allows
    yes_liquidity.min(no_liquidity)
}

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

        // Fetch orderbook to check liquidity
        let liquidity = match repository::get_latest_orderbook_snapshot(
            self.db.pool(),
            opportunity.market_id,
        )
        .await
        {
            Ok(Some(snapshot)) => calculate_orderbook_liquidity(&snapshot),
            Ok(None) => {
                debug!("No orderbook snapshot for {}", opportunity.market_name);
                Decimal::ZERO
            }
            Err(e) => {
                warn!("Failed to fetch orderbook: {}", e);
                Decimal::ZERO
            }
        };

        // Calculate investment size based on liquidity
        // Baseline: base_position_size ($10)
        // Scale up to max_position_size ($20) if liquidity >= threshold ($50)
        let available = self.available_balance();
        let target_size = if liquidity >= self.config.liquidity_threshold {
            debug!(
                "Good liquidity ${:.2} >= ${:.2}, using max size ${}",
                liquidity, self.config.liquidity_threshold, self.config.max_position_size
            );
            self.config.max_position_size
        } else {
            debug!(
                "Low liquidity ${:.2} < ${:.2}, using base size ${}",
                liquidity, self.config.liquidity_threshold, self.config.base_position_size
            );
            self.config.base_position_size
        };

        let size = available.min(target_size);

        if size < self.config.base_position_size {
            debug!("Insufficient balance for trade: ${}", available);
            return Ok(false);
        }

        // Check max exposure
        if self.total_exposure() + size > self.config.max_total_exposure {
            debug!("Would exceed max exposure");
            return Ok(false);
        }

        info!(
            "Trade size: ${} (liquidity: ${:.2})",
            size, liquidity
        );

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
        let mut yes_size = details.yes_shares.round_dp(2);
        let yes_price = details.yes_price.round_dp(2);
        let mut no_size = details.no_shares.round_dp(2);
        let no_price = details.no_price.round_dp(2);

        // Check minimum order value ($1 minimum per Polymarket)
        let yes_value = yes_size * yes_price;
        let no_value = no_size * no_price;
        let yes_valid = yes_value >= dec!(1);
        let no_valid = no_value >= dec!(1);

        // Determine trade mode: both sides, single side, or skip
        let single_side_only: Option<&str> = if yes_valid && no_valid {
            None // Both sides valid, do normal spread arb
        } else if yes_valid && !no_valid {
            // Only YES valid - use half size for single-sided bet
            let half_investment = details.total_invested / dec!(2);
            yes_size = (half_investment / yes_price).round_dp(2);
            no_size = Decimal::ZERO;
            info!(
                "[LIVE] NO value too low (${:.2}), single-sided YES bet with half size: {} @ ${}",
                no_value, yes_size, yes_price
            );
            Some("yes")
        } else if !yes_valid && no_valid {
            // Only NO valid - use half size for single-sided bet
            let half_investment = details.total_invested / dec!(2);
            no_size = (half_investment / no_price).round_dp(2);
            yes_size = Decimal::ZERO;
            info!(
                "[LIVE] YES value too low (${:.2}), single-sided NO bet with half size: {} @ ${}",
                yes_value, no_size, no_price
            );
            Some("no")
        } else {
            warn!(
                "[LIVE] Both order values too low - YES: ${:.2}, NO: ${:.2} (min $1). Skipping.",
                yes_value, no_value
            );
            return Ok(());
        };

        // Log order details
        if single_side_only.is_none() {
            info!(
                "[LIVE] Building orders: YES {} @ ${}, NO {} @ ${}",
                yes_size, yes_price, no_size, no_price
            );
        }

        // Helper to extract order result
        let extract_order_info = |result: &[polymarket_client_sdk::clob::types::PostOrderResponse]| {
            result.first().map(|r| {
                let has_error = r.error_msg.as_ref().map(|e| !e.is_empty()).unwrap_or(false);
                let order_id = if r.order_id.is_empty() || has_error {
                    None
                } else {
                    Some(r.order_id.clone())
                };
                let filled = r.taking_amount;
                let error = r.error_msg.clone();
                (order_id, filled, error)
            })
        };

        // Execute orders based on trade mode
        let (yes_order_id, yes_filled, no_order_id, no_filled): (Option<String>, Decimal, Option<String>, Decimal) =
            match single_side_only {
                Some("yes") => {
                    // Single-sided YES bet
                    info!("[LIVE] Building single YES order: {} @ ${}", yes_size, yes_price);
                    let yes_order = timeout(
                        Duration::from_secs(ORDER_TIMEOUT_SECS),
                        clob_client
                            .limit_order()
                            .token_id(&opportunity.yes_token_id)
                            .size(yes_size)
                            .price(yes_price)
                            .side(polymarket_client_sdk::clob::types::Side::Buy)
                            .build()
                    )
                    .await
                    .context("YES order building timed out")?
                    .context("Failed to build YES order")?;

                    let yes_signed = timeout(
                        Duration::from_secs(ORDER_TIMEOUT_SECS),
                        clob_client.sign(&signer, yes_order)
                    )
                    .await
                    .context("YES order signing timed out")?
                    .context("Failed to sign YES order")?;

                    info!("[LIVE] Posting single YES order...");
                    let yes_result = timeout(
                        Duration::from_secs(ORDER_TIMEOUT_SECS),
                        clob_client.post_order(yes_signed)
                    )
                    .await
                    .context("YES order posting timed out")?
                    .context("Failed to post YES order")?;

                    info!("[LIVE] YES order result: {:?}", yes_result);
                    let (order_id, filled, error) = extract_order_info(&yes_result)
                        .unwrap_or((None, dec!(0), Some("No response".to_string())));

                    if order_id.is_none() {
                        error!("[LIVE] Single YES order failed: {:?}", error);
                        return Ok(());
                    }
                    (order_id, filled, None, dec!(0))
                }
                Some("no") => {
                    // Single-sided NO bet
                    info!("[LIVE] Building single NO order: {} @ ${}", no_size, no_price);
                    let no_order = timeout(
                        Duration::from_secs(ORDER_TIMEOUT_SECS),
                        clob_client
                            .limit_order()
                            .token_id(&opportunity.no_token_id)
                            .size(no_size)
                            .price(no_price)
                            .side(polymarket_client_sdk::clob::types::Side::Buy)
                            .build()
                    )
                    .await
                    .context("NO order building timed out")?
                    .context("Failed to build NO order")?;

                    let no_signed = timeout(
                        Duration::from_secs(ORDER_TIMEOUT_SECS),
                        clob_client.sign(&signer, no_order)
                    )
                    .await
                    .context("NO order signing timed out")?
                    .context("Failed to sign NO order")?;

                    info!("[LIVE] Posting single NO order...");
                    let no_result = timeout(
                        Duration::from_secs(ORDER_TIMEOUT_SECS),
                        clob_client.post_order(no_signed)
                    )
                    .await
                    .context("NO order posting timed out")?
                    .context("Failed to post NO order")?;

                    info!("[LIVE] NO order result: {:?}", no_result);
                    let (order_id, filled, error) = extract_order_info(&no_result)
                        .unwrap_or((None, dec!(0), Some("No response".to_string())));

                    if order_id.is_none() {
                        error!("[LIVE] Single NO order failed: {:?}", error);
                        return Ok(());
                    }
                    (None, dec!(0), order_id, filled)
                }
                _ => {
                    // Both sides - original spread arb logic
                    let (yes_order, no_order) = timeout(
                        Duration::from_secs(ORDER_TIMEOUT_SECS),
                        async {
                            tokio::try_join!(
                                async {
                                    clob_client
                                        .limit_order()
                                        .token_id(&opportunity.yes_token_id)
                                        .size(yes_size)
                                        .price(yes_price)
                                        .side(polymarket_client_sdk::clob::types::Side::Buy)
                                        .build()
                                        .await
                                        .context("Failed to build YES order")
                                },
                                async {
                                    clob_client
                                        .limit_order()
                                        .token_id(&opportunity.no_token_id)
                                        .size(no_size)
                                        .price(no_price)
                                        .side(polymarket_client_sdk::clob::types::Side::Buy)
                                        .build()
                                        .await
                                        .context("Failed to build NO order")
                                }
                            )
                        }
                    )
                    .await
                    .context("Order building timed out")??;

                    let (yes_signed, no_signed) = timeout(
                        Duration::from_secs(ORDER_TIMEOUT_SECS),
                        async {
                            tokio::try_join!(
                                clob_client.sign(&signer, yes_order),
                                clob_client.sign(&signer, no_order)
                            )
                        }
                    )
                    .await
                    .context("Order signing timed out")?
                    .context("Failed to sign orders")?;

                    info!("[LIVE] Posting YES and NO orders simultaneously...");
                    let (yes_result, no_result) = timeout(
                        Duration::from_secs(ORDER_TIMEOUT_SECS),
                        async {
                            tokio::try_join!(
                                clob_client.post_order(yes_signed),
                                clob_client.post_order(no_signed)
                            )
                        }
                    )
                    .await
                    .context("Order posting timed out")?
                    .context("Failed to post orders")?;

                    info!("[LIVE] YES order result: {:?}", yes_result);
                    info!("[LIVE] NO order result: {:?}", no_result);

                    let (yes_id, yes_f, yes_err) = extract_order_info(&yes_result)
                        .unwrap_or((None, dec!(0), Some("No response".to_string())));
                    let (no_id, no_f, no_err) = extract_order_info(&no_result)
                        .unwrap_or((None, dec!(0), Some("No response".to_string())));

                    let yes_failed = yes_id.is_none();
                    let no_failed = no_id.is_none();

                    if yes_failed && no_failed {
                        error!("[LIVE] Both orders failed - YES: {:?}, NO: {:?}", yes_err, no_err);
                        return Ok(());
                    }

                    // If only one failed, cancel the other with retries
                    if yes_failed || no_failed {
                        warn!("[LIVE] Partial failure - YES: {:?}, NO: {:?}. Cancelling successful order.", yes_err, no_err);

                        if let Some(ref order_id) = yes_id {
                            for attempt in 1..=CANCEL_MAX_RETRIES {
                                match timeout(Duration::from_secs(10), clob_client.cancel_order(order_id)).await {
                                    Ok(Ok(_)) => {
                                        info!("[LIVE] Cancelled YES order {} on attempt {}", order_id, attempt);
                                        break;
                                    }
                                    Ok(Err(e)) => warn!("[LIVE] Cancel YES failed (attempt {}): {:?}", attempt, e),
                                    Err(_) => warn!("[LIVE] Cancel YES timeout (attempt {})", attempt),
                                }
                                if attempt == CANCEL_MAX_RETRIES {
                                    error!("[LIVE] CRITICAL: Failed to cancel YES order {} after {} attempts. ORPHANED ORDER!", order_id, CANCEL_MAX_RETRIES);
                                } else {
                                    tokio::time::sleep(Duration::from_millis(CANCEL_RETRY_DELAY_MS)).await;
                                }
                            }
                        }

                        if let Some(ref order_id) = no_id {
                            for attempt in 1..=CANCEL_MAX_RETRIES {
                                match timeout(Duration::from_secs(10), clob_client.cancel_order(order_id)).await {
                                    Ok(Ok(_)) => {
                                        info!("[LIVE] Cancelled NO order {} on attempt {}", order_id, attempt);
                                        break;
                                    }
                                    Ok(Err(e)) => warn!("[LIVE] Cancel NO failed (attempt {}): {:?}", attempt, e),
                                    Err(_) => warn!("[LIVE] Cancel NO timeout (attempt {})", attempt),
                                }
                                if attempt == CANCEL_MAX_RETRIES {
                                    error!("[LIVE] CRITICAL: Failed to cancel NO order {} after {} attempts. ORPHANED ORDER!", order_id, CANCEL_MAX_RETRIES);
                                } else {
                                    tokio::time::sleep(Duration::from_millis(CANCEL_RETRY_DELAY_MS)).await;
                                }
                            }
                        }
                        return Ok(());
                    }

                    (yes_id, yes_f, no_id, no_f)
                }
            };

        // Log fill amounts
        info!(
            "[LIVE] Orders placed - YES: {} filled of {} (order: {:?}), NO: {} filled of {} (order: {:?})",
            yes_filled, yes_size, yes_order_id,
            no_filled, no_size, no_order_id
        );

        // Determine order status based on fill (handle non-placed orders)
        let yes_status = if yes_size == Decimal::ZERO {
            "not_placed"
        } else if yes_filled >= yes_size {
            "filled"
        } else if yes_filled > dec!(0) {
            "partial"
        } else {
            "pending"
        };
        let no_status = if no_size == Decimal::ZERO {
            "not_placed"
        } else if no_filled >= no_size {
            "filled"
        } else if no_filled > dec!(0) {
            "partial"
        } else {
            "pending"
        };

        // Record position with requested amounts
        let position_id = repository::create_position(
            self.db.pool(),
            opportunity.market_id,
            yes_size,  // Requested (rounded) size
            no_size,   // Requested (rounded) size
            yes_size * yes_price + no_size * no_price, // Actual order value
            false, // NOT dry_run
        )
        .await?;

        // Update position with actual fill amounts
        repository::update_position_fills(
            self.db.pool(),
            position_id,
            yes_filled,
            no_filled,
        )
        .await?;

        // Record trades with order IDs and fill amounts (only for sides that were placed)
        let mut trade_count = 0;
        if yes_size > Decimal::ZERO {
            repository::record_trade_with_order(
                self.db.pool(),
                position_id,
                "yes",
                "buy",
                yes_price,
                yes_size,
                yes_order_id.as_deref(),
                yes_filled,
                yes_status,
            )
            .await?;
            trade_count += 1;
        }

        if no_size > Decimal::ZERO {
            repository::record_trade_with_order(
                self.db.pool(),
                position_id,
                "no",
                "buy",
                no_price,
                no_size,
                no_order_id.as_deref(),
                no_filled,
                no_status,
            )
            .await?;
            trade_count += 1;
        }

        // Update session state with actual order value
        let actual_invested = yes_size * yes_price + no_size * no_price;
        let actual_fee = actual_invested * self.config.fee_rate;
        self.session.current_balance -= actual_invested;
        self.session.total_trades += trade_count;
        self.session.positions_opened += 1;
        self.session.fees_paid += actual_fee;

        // Cache position with actual order values
        self.session.open_positions.insert(
            opportunity.market_id,
            PositionCache {
                id: position_id,
                market_id: opportunity.market_id,
                market_name: opportunity.market_name.clone(),
                yes_shares: yes_size,
                no_shares: no_size,
                total_invested: actual_invested,
                end_time: opportunity.end_time,
            },
        );

        let trade_type = match single_side_only {
            Some("yes") => "SINGLE-YES",
            Some("no") => "SINGLE-NO",
            _ => "SPREAD-ARB",
        };
        info!(
            "[LIVE] {} EXECUTED: {} | YES: {} @ ${} | NO: {} @ ${} | Invested: ${:.2}",
            trade_type,
            opportunity.market_name,
            yes_size, yes_price,
            no_size, no_price,
            actual_invested
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
