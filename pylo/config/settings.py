"""Application settings using Pydantic."""

from decimal import Decimal
from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Polymarket (SecretStr prevents accidental logging)
    polymarket_api_key: SecretStr = SecretStr("")
    polymarket_api_secret: SecretStr = SecretStr("")
    polymarket_api_passphrase: SecretStr = SecretStr("")
    polymarket_wallet_address: str = ""

    # Blockchain/Web3 (for position redemption)
    wallet_private_key: SecretStr = SecretStr("")
    polygon_rpc_url: str = "https://polygon-bor-rpc.publicnode.com"

    # Kalshi
    kalshi_api_key: SecretStr = SecretStr("")
    kalshi_api_secret: SecretStr = SecretStr("")

    # Limitless Exchange (Polymarket fork on Base L2)
    limitless_api_url: str = "https://api.limitless.exchange"
    limitless_ws_url: str = "wss://ws.limitless.exchange/markets"
    limitless_api_key: SecretStr = SecretStr("")  # Optional for trading

    # Binance
    binance_api_key: SecretStr = SecretStr("")
    binance_api_secret: SecretStr = SecretStr("")

    # Coinbase
    coinbase_api_key: SecretStr = SecretStr("")
    coinbase_api_secret: SecretStr = SecretStr("")
    coinbase_api_passphrase: SecretStr = SecretStr("")

    # Alerts
    discord_webhook_url: SecretStr = SecretStr("")
    telegram_bot_token: SecretStr = SecretStr("")
    telegram_chat_id: str = ""

    # Database
    database_url: str = "sqlite+aiosqlite:///./data/aubit.db"

    # Application
    log_level: str = "INFO"
    scan_interval: int = Field(default=30, ge=5, le=300)
    max_concurrent_requests: int = Field(default=10, ge=1, le=50)

    # Arbitrage thresholds (as decimals, e.g., 0.01 = 1%)
    min_internal_arb_profit: Decimal = Decimal("0.005")
    min_cross_platform_arb_profit: Decimal = Decimal("0.035")  # 3.5% (fees ~2.5%)
    min_cross_platform_15m_arb_profit: Decimal = Decimal("0.01")  # 1.0% for 15m markets (quick execution, lower risk)
    min_hedging_arb_profit: Decimal = Decimal("0.03")

    # ═══════════════════════════════════════════════════════════════════════════
    # Cross-Platform Arbitrage Settings
    # ═══════════════════════════════════════════════════════════════════════════

    # Minimum confidence for automatic event matching (0-1)
    cross_platform_min_match_confidence: float = Field(default=0.9, ge=0.5, le=1.0)

    # Minimum liquidity depth on each side (USD)
    cross_platform_min_liquidity: Decimal = Decimal("500")
    cross_platform_15m_min_liquidity: Decimal = Decimal("100")  # Lower for 15m markets

    # Maximum slippage tolerance (as decimal, e.g., 0.005 = 0.5%)
    cross_platform_max_slippage: Decimal = Decimal("0.005")

    # Minimum time to resolution (seconds) - avoid last-minute chaos
    cross_platform_min_time_to_resolution: int = Field(default=3600, ge=60)

    # Minimum time to resolution for 15-minute directional markets (seconds)
    # These markets are designed for quick trades, so shorter threshold
    cross_platform_15m_min_time_to_resolution: int = Field(default=120, ge=30)

    # Minimum daily volume indicator (USD) - skip illiquid markets
    cross_platform_min_daily_volume: Decimal = Decimal("1000")
    cross_platform_15m_min_daily_volume: Decimal = Decimal("0")  # Disabled for 15m markets (new markets often have 0 volume initially)

    # Resolution validation - only trade markets with matching resolution rules
    cross_platform_validate_resolution: bool = True

    # Safe resolution sources - markets using these sources are whitelisted
    # crypto_price: Objective exchange prices (Binance, Coinbase)
    # sports: Official league/tournament data
    # government_data: Official Fed/BLS/Census data
    cross_platform_safe_resolution_types: str = "crypto_price,sports,government_data"

    # Platform trading fees (as decimals, e.g., 0.01 = 1%)
    # These are subtracted from profit calculations
    polymarket_fee_rate: Decimal = Decimal("0.00")  # 0% as of 2024
    kalshi_fee_rate: Decimal = Decimal("0.01")  # ~1% variable by market
    limitless_fee_rate: Decimal = Decimal("0.00")  # 0% fees on Limitless
    binance_fee_rate: Decimal = Decimal("0.0004")  # 0.04% for futures

    # Price staleness threshold (seconds) - opportunities with older prices are rejected
    max_price_age_seconds: int = Field(default=30, ge=5, le=300)

    # ═══════════════════════════════════════════════════════════════════════════
    # Spread Arbitrage Bot Settings
    # ═══════════════════════════════════════════════════════════════════════════

    # Enable/disable the spread bot
    spread_bot_enabled: bool = False

    # CRITICAL: Always start in dry-run mode for safety
    spread_bot_dry_run: bool = True

    # Minimum spread profit to trigger a trade (as decimal, e.g., 0.02 = 2%)
    spread_bot_min_profit: Decimal = Decimal("0.02")

    # Maximum position size per trade (in USD)
    spread_bot_max_position_size: Decimal = Decimal("100")

    # Maximum total exposure across all positions (in USD)
    spread_bot_max_total_exposure: Decimal = Decimal("500")

    # Starting balance for dry-run simulation
    spread_bot_starting_balance: Decimal = Decimal("10000")

    # Assets to monitor (comma-separated)
    # Crypto: BTC, ETH, SOL, XRP
    # Events: SPORTS (Super Bowl, etc.), UNKNOWN (political, other)
    spread_bot_assets: str = "BTC,ETH,SOL,XRP,SPORTS,UNKNOWN"

    # Maximum time to expiry to consider (in seconds)
    # Only monitor markets expiring within this window
    # 5184000 = 60 days (supports event markets like Super Bowl, elections)
    spread_bot_max_time_to_expiry: int = Field(default=5184000, ge=300, le=7776000)

    # Poll interval in seconds
    spread_bot_poll_interval: int = Field(default=1, ge=1, le=60)

    # Maximum orderbook age for market discovery (seconds)
    # Only markets with orderbooks fresher than this will be included
    # 30s - orderbook-stream reconnects every 20s to keep fresh
    max_orderbook_age_seconds: int = Field(default=30, ge=5, le=86400)

    # JSON log file path
    spread_bot_log_file: str = "logs/spread_bot_trades.json"

    # ═══════════════════════════════════════════════════════════════════════════
    # Edge Trader Bot Settings (Probability Gap Trading)
    # ═══════════════════════════════════════════════════════════════════════════

    # Enable/disable the edge trader bot
    edge_trader_enabled: bool = False

    # Dry run mode (simulate trades without execution)
    edge_trader_dry_run: bool = True

    # Probability estimation settings
    edge_trader_lookback_periods: int = Field(default=10, ge=3, le=100)
    edge_trader_momentum_weight: float = Field(default=0.6, ge=0.0, le=1.0)

    # Edge thresholds
    edge_trader_min_edge: Decimal = Decimal("0.05")  # 5% minimum edge to trade
    edge_trader_min_confidence: Decimal = Decimal("0.5")  # Skip low-confidence signals

    # Position sizing (Kelly-based)
    edge_trader_kelly_fraction: Decimal = Decimal("0.25")  # Use 25% Kelly
    edge_trader_max_position_pct: Decimal = Decimal("0.10")  # Max 10% bankroll per trade
    edge_trader_max_total_exposure: Decimal = Decimal("500")  # Max total exposure

    # Starting balance for dry-run simulation
    edge_trader_starting_balance: Decimal = Decimal("10000")

    # Expiry confidence adjustment multipliers
    edge_trader_early_confidence_mult: Decimal = Decimal("0.6")  # >80% time remaining
    edge_trader_sweet_spot_confidence: Decimal = Decimal("1.0")  # 20-80% time remaining
    edge_trader_late_confidence_mult: Decimal = Decimal("0.8")  # 7-20% time remaining
    edge_trader_near_expiry_mult: Decimal = Decimal("0.4")  # <7% time remaining

    # Trading fee rate (for EV calculations)
    edge_trader_fee_rate: Decimal = Decimal("0.02")  # 2% Polymarket fee

    # Target timeframes (comma-separated)
    edge_trader_timeframes: str = "15min"  # 15-minute markets

    # Target assets (comma-separated)
    edge_trader_assets: str = "BTC,ETH,SOL"

    # Poll interval in seconds
    edge_trader_poll_interval: int = Field(default=5, ge=1, le=60)

    # JSON log file path
    edge_trader_log_file: str = "logs/edge_trader_signals.json"

    def get_edge_trader_assets(self) -> list[str]:
        """Get list of assets for edge trader."""
        return [a.strip().upper() for a in self.edge_trader_assets.split(",") if a.strip()]

    def get_edge_trader_timeframes(self) -> list[str]:
        """Get list of timeframes for edge trader."""
        return [t.strip().lower() for t in self.edge_trader_timeframes.split(",") if t.strip()]

    def get_spread_bot_assets(self) -> list[str]:
        """Get list of assets to monitor."""
        return [a.strip().upper() for a in self.spread_bot_assets.split(",") if a.strip()]

    def get_safe_resolution_types(self) -> list[str]:
        """Get list of safe resolution types for cross-platform arbitrage."""
        return [
            t.strip().lower()
            for t in self.cross_platform_safe_resolution_types.split(",")
            if t.strip()
        ]

    def get_fee_rate(self, platform: str) -> Decimal:
        """Get trading fee rate for a platform."""
        fee_map = {
            "polymarket": self.polymarket_fee_rate,
            "kalshi": self.kalshi_fee_rate,
            "limitless": self.limitless_fee_rate,
            "binance": self.binance_fee_rate,
        }
        return fee_map.get(platform.lower(), Decimal("0.01"))  # Default 1% if unknown

    @property
    def has_polymarket_credentials(self) -> bool:
        """Check if Polymarket credentials are configured (only needed for trading)."""
        return bool(
            self.polymarket_api_key.get_secret_value()
            and self.polymarket_api_secret.get_secret_value()
        )

    @property
    def has_kalshi_credentials(self) -> bool:
        """Check if Kalshi credentials are configured (only needed for trading)."""
        return bool(
            self.kalshi_api_key.get_secret_value() and self.kalshi_api_secret.get_secret_value()
        )

    @property
    def has_limitless_credentials(self) -> bool:
        """Check if Limitless credentials are configured (optional for trading)."""
        return bool(self.limitless_api_key.get_secret_value())

    @property
    def has_binance_credentials(self) -> bool:
        """Check if Binance credentials are configured (only needed for trading)."""
        return bool(
            self.binance_api_key.get_secret_value() and self.binance_api_secret.get_secret_value()
        )

    @property
    def has_discord_alerts(self) -> bool:
        """Check if Discord alerts are configured."""
        return bool(self.discord_webhook_url.get_secret_value())

    @property
    def has_telegram_alerts(self) -> bool:
        """Check if Telegram alerts are configured."""
        return bool(self.telegram_bot_token.get_secret_value() and self.telegram_chat_id)

    @property
    def has_web3_credentials(self) -> bool:
        """Check if Web3/blockchain credentials are configured for redemption."""
        return bool(self.wallet_private_key.get_secret_value())


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
