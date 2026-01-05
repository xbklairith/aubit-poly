"""Alert notification system for arbitrage opportunities."""

import logging
from abc import ABC, abstractmethod
from decimal import Decimal

import httpx

from src.config.settings import get_settings
from src.models.opportunity import ArbitrageOpportunity, ArbitrageType

logger = logging.getLogger(__name__)


class BaseNotifier(ABC):
    """Abstract base class for notification channels."""

    name: str = "base"

    @abstractmethod
    async def send(self, opportunity: ArbitrageOpportunity) -> bool:
        """
        Send notification for an arbitrage opportunity.

        Args:
            opportunity: The opportunity to notify about

        Returns:
            True if notification was sent successfully
        """
        ...

    @abstractmethod
    async def send_batch(self, opportunities: list[ArbitrageOpportunity]) -> int:
        """
        Send batch notification for multiple opportunities.

        Args:
            opportunities: List of opportunities

        Returns:
            Number of successfully sent notifications
        """
        ...


class DiscordNotifier(BaseNotifier):
    """Send notifications to Discord via webhook."""

    name = "discord"

    def __init__(self, webhook_url: str | None = None) -> None:
        """Initialize Discord notifier."""
        self.settings = get_settings()
        self.webhook_url = webhook_url or self.settings.discord_webhook_url
        self.logger = logging.getLogger(__name__)

    async def send(self, opportunity: ArbitrageOpportunity) -> bool:
        """Send a single opportunity to Discord."""
        if not self.webhook_url:
            self.logger.warning("Discord webhook URL not configured")
            return False

        try:
            embed = self._format_embed(opportunity)

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.webhook_url,
                    json={"embeds": [embed]},
                )
                response.raise_for_status()

            self.logger.info(f"Sent Discord alert: {opportunity.id}")
            return True

        except httpx.HTTPError as e:
            self.logger.error(f"Discord notification failed: {e}")
            return False

    async def send_batch(self, opportunities: list[ArbitrageOpportunity]) -> int:
        """Send multiple opportunities in a single message."""
        if not self.webhook_url or not opportunities:
            return 0

        try:
            # Discord allows max 10 embeds per message
            embeds = [self._format_embed(opp) for opp in opportunities[:10]]

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.webhook_url,
                    json={"embeds": embeds},
                )
                response.raise_for_status()

            return len(embeds)

        except httpx.HTTPError as e:
            self.logger.error(f"Discord batch notification failed: {e}")
            return 0

    def _format_embed(self, opportunity: ArbitrageOpportunity) -> dict:
        """Format an opportunity as a Discord embed."""
        # Color based on opportunity type
        colors = {
            ArbitrageType.INTERNAL: 0x00FF00,  # Green
            ArbitrageType.CROSS_PLATFORM: 0x0099FF,  # Blue
            ArbitrageType.HEDGING: 0xFF9900,  # Orange
        }

        color = colors.get(opportunity.type, 0x808080)

        # Build fields
        fields = [
            {
                "name": "Profit",
                "value": f"{opportunity.profit_percentage:.2%}",
                "inline": True,
            },
            {
                "name": "Type",
                "value": opportunity.type.value.replace("_", " ").title(),
                "inline": True,
            },
            {
                "name": "Confidence",
                "value": f"{opportunity.confidence:.0%}",
                "inline": True,
            },
        ]

        # Add platforms
        if opportunity.platforms:
            platforms = ", ".join(p.value.title() for p in opportunity.platforms)
            fields.append({
                "name": "Platforms",
                "value": platforms,
                "inline": False,
            })

        # Add instructions
        if opportunity.instructions:
            instructions = "\n".join(opportunity.instructions[:5])
            fields.append({
                "name": "Instructions",
                "value": f"```\n{instructions}\n```",
                "inline": False,
            })

        return {
            "title": "🎯 Arbitrage Opportunity",
            "description": opportunity.description,
            "color": color,
            "fields": fields,
            "timestamp": opportunity.detected_at.isoformat(),
            "footer": {"text": f"ID: {opportunity.id}"},
        }


class TelegramNotifier(BaseNotifier):
    """Send notifications to Telegram."""

    name = "telegram"

    def __init__(
        self,
        bot_token: str | None = None,
        chat_id: str | None = None,
    ) -> None:
        """Initialize Telegram notifier."""
        self.settings = get_settings()
        self.bot_token = bot_token or self.settings.telegram_bot_token
        self.chat_id = chat_id or self.settings.telegram_chat_id
        self.logger = logging.getLogger(__name__)

    @property
    def api_url(self) -> str:
        """Get Telegram API base URL."""
        return f"https://api.telegram.org/bot{self.bot_token}"

    async def send(self, opportunity: ArbitrageOpportunity) -> bool:
        """Send a single opportunity to Telegram."""
        if not self.bot_token or not self.chat_id:
            self.logger.warning("Telegram credentials not configured")
            return False

        try:
            message = self._format_message(opportunity)

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.api_url}/sendMessage",
                    json={
                        "chat_id": self.chat_id,
                        "text": message,
                        "parse_mode": "HTML",
                    },
                )
                response.raise_for_status()

            self.logger.info(f"Sent Telegram alert: {opportunity.id}")
            return True

        except httpx.HTTPError as e:
            self.logger.error(f"Telegram notification failed: {e}")
            return False

    async def send_batch(self, opportunities: list[ArbitrageOpportunity]) -> int:
        """Send multiple opportunities."""
        success = 0
        for opp in opportunities:
            if await self.send(opp):
                success += 1
        return success

    def _format_message(self, opportunity: ArbitrageOpportunity) -> str:
        """Format an opportunity as Telegram message."""
        emoji = {
            ArbitrageType.INTERNAL: "🟢",
            ArbitrageType.CROSS_PLATFORM: "🔵",
            ArbitrageType.HEDGING: "🟠",
        }.get(opportunity.type, "⚪")

        lines = [
            f"{emoji} <b>Arbitrage Alert</b>",
            "",
            f"<b>Profit:</b> {opportunity.profit_percentage:.2%}",
            f"<b>Type:</b> {opportunity.type.value.replace('_', ' ').title()}",
            "",
            f"<i>{opportunity.description}</i>",
            "",
        ]

        if opportunity.instructions:
            lines.append("<b>Instructions:</b>")
            for instruction in opportunity.instructions[:5]:
                lines.append(f"  {instruction}")

        return "\n".join(lines)


class ConsoleNotifier(BaseNotifier):
    """Print notifications to console (for development/testing)."""

    name = "console"

    async def send(self, opportunity: ArbitrageOpportunity) -> bool:
        """Print opportunity to console."""
        print(f"\n{'='*60}")
        print("🎯 ARBITRAGE OPPORTUNITY DETECTED")
        print(f"{'='*60}")
        print(f"Type: {opportunity.type.value}")
        print(f"Profit: {opportunity.profit_percentage:.2%}")
        print(f"Confidence: {opportunity.confidence:.0%}")
        print(f"\n{opportunity.description}")
        print("\nInstructions:")
        for instruction in opportunity.instructions:
            print(f"  {instruction}")
        print(f"{'='*60}\n")
        return True

    async def send_batch(self, opportunities: list[ArbitrageOpportunity]) -> int:
        """Print multiple opportunities."""
        for opp in opportunities:
            await self.send(opp)
        return len(opportunities)


class AlertManager:
    """
    Manages multiple notification channels.

    Sends alerts through all configured channels and handles
    deduplication to prevent alert spam.
    """

    def __init__(self) -> None:
        """Initialize the alert manager."""
        self.settings = get_settings()
        self.logger = logging.getLogger(__name__)
        self.notifiers: list[BaseNotifier] = []

        # Track sent alerts to prevent spam
        self._sent_alerts: set[str] = set()
        self._min_profit_threshold = Decimal("0.01")  # 1% minimum for alerts

        # Initialize configured notifiers
        self._setup_notifiers()

    def _setup_notifiers(self) -> None:
        """Set up notification channels based on configuration."""
        # Always include console for development
        self.notifiers.append(ConsoleNotifier())

        # Discord
        if self.settings.has_discord_alerts:
            self.notifiers.append(DiscordNotifier())
            self.logger.info("Discord notifications enabled")

        # Telegram
        if self.settings.has_telegram_alerts:
            self.notifiers.append(TelegramNotifier())
            self.logger.info("Telegram notifications enabled")

    async def notify(self, opportunity: ArbitrageOpportunity) -> None:
        """
        Send notification for a single opportunity.

        Args:
            opportunity: The opportunity to notify about
        """
        # Check if already sent
        if opportunity.id in self._sent_alerts:
            return

        # Check minimum threshold
        if opportunity.profit_percentage < self._min_profit_threshold:
            return

        # Send to all notifiers
        for notifier in self.notifiers:
            try:
                await notifier.send(opportunity)
            except Exception as e:
                self.logger.error(f"Notifier {notifier.name} failed: {e}")

        # Mark as sent
        self._sent_alerts.add(opportunity.id)

    async def notify_batch(
        self,
        opportunities: list[ArbitrageOpportunity],
        max_alerts: int = 5,
    ) -> None:
        """
        Send notifications for multiple opportunities.

        Args:
            opportunities: List of opportunities
            max_alerts: Maximum number of alerts to send
        """
        # Filter new opportunities above threshold
        new_opps = [
            opp
            for opp in opportunities
            if opp.id not in self._sent_alerts
            and opp.profit_percentage >= self._min_profit_threshold
        ][:max_alerts]

        if not new_opps:
            return

        # Send batch to all notifiers
        for notifier in self.notifiers:
            try:
                await notifier.send_batch(new_opps)
            except Exception as e:
                self.logger.error(f"Batch notifier {notifier.name} failed: {e}")

        # Mark all as sent
        for opp in new_opps:
            self._sent_alerts.add(opp.id)

    def clear_sent_alerts(self) -> None:
        """Clear the sent alerts cache."""
        self._sent_alerts.clear()
