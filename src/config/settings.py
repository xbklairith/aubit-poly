"""Application settings using Pydantic."""

from decimal import Decimal
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Polymarket
    polymarket_api_key: str = ""
    polymarket_api_secret: str = ""
    polymarket_api_passphrase: str = ""
    polymarket_wallet_address: str = ""

    # Kalshi
    kalshi_api_key: str = ""
    kalshi_api_secret: str = ""

    # Binance
    binance_api_key: str = ""
    binance_api_secret: str = ""

    # Coinbase
    coinbase_api_key: str = ""
    coinbase_api_secret: str = ""
    coinbase_api_passphrase: str = ""

    # Alerts
    discord_webhook_url: str = ""
    telegram_bot_token: str = ""
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

    @property
    def has_polymarket_credentials(self) -> bool:
        """Check if Polymarket credentials are configured."""
        return bool(self.polymarket_api_key and self.polymarket_api_secret)

    @property
    def has_kalshi_credentials(self) -> bool:
        """Check if Kalshi credentials are configured."""
        return bool(self.kalshi_api_key and self.kalshi_api_secret)

    @property
    def has_binance_credentials(self) -> bool:
        """Check if Binance credentials are configured."""
        return bool(self.binance_api_key and self.binance_api_secret)

    @property
    def has_discord_alerts(self) -> bool:
        """Check if Discord alerts are configured."""
        return bool(self.discord_webhook_url)

    @property
    def has_telegram_alerts(self) -> bool:
        """Check if Telegram alerts are configured."""
        return bool(self.telegram_bot_token and self.telegram_chat_id)


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
