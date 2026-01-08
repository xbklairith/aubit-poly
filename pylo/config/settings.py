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

    # Kalshi
    kalshi_api_key: SecretStr = SecretStr("")
    kalshi_api_secret: SecretStr = SecretStr("")

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
    min_cross_platform_arb_profit: Decimal = Decimal("0.02")
    min_hedging_arb_profit: Decimal = Decimal("0.03")

    # Platform trading fees (as decimals, e.g., 0.01 = 1%)
    # These are subtracted from profit calculations
    polymarket_fee_rate: Decimal = Decimal("0.00")  # 0% as of 2024
    kalshi_fee_rate: Decimal = Decimal("0.01")  # ~1% variable by market
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

    # Assets to monitor (comma-separated: BTC,ETH,SOL,XRP)
    spread_bot_assets: str = "BTC,ETH,SOL,XRP"

    # Maximum time to expiry to consider (in seconds)
    # Only monitor markets expiring within this window
    # 86400 = 24 hours (to support daily markets)
    spread_bot_max_time_to_expiry: int = Field(default=86400, ge=300, le=604800)

    # Poll interval in seconds
    spread_bot_poll_interval: int = Field(default=1, ge=1, le=60)

    # JSON log file path
    spread_bot_log_file: str = "logs/spread_bot_trades.json"

    def get_spread_bot_assets(self) -> list[str]:
        """Get list of assets to monitor."""
        return [a.strip().upper() for a in self.spread_bot_assets.split(",") if a.strip()]

    def get_fee_rate(self, platform: str) -> Decimal:
        """Get trading fee rate for a platform."""
        fee_map = {
            "polymarket": self.polymarket_fee_rate,
            "kalshi": self.kalshi_fee_rate,
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
            self.kalshi_api_key.get_secret_value()
            and self.kalshi_api_secret.get_secret_value()
        )

    @property
    def has_binance_credentials(self) -> bool:
        """Check if Binance credentials are configured (only needed for trading)."""
        return bool(
            self.binance_api_key.get_secret_value()
            and self.binance_api_secret.get_secret_value()
        )

    @property
    def has_discord_alerts(self) -> bool:
        """Check if Discord alerts are configured."""
        return bool(self.discord_webhook_url.get_secret_value())

    @property
    def has_telegram_alerts(self) -> bool:
        """Check if Telegram alerts are configured."""
        return bool(
            self.telegram_bot_token.get_secret_value() and self.telegram_chat_id
        )


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
