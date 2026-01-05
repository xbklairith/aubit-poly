"""Base class for all data sources."""

import logging
from abc import ABC, abstractmethod

from src.models.market import Market

logger = logging.getLogger(__name__)


class BaseDataSource(ABC):
    """Abstract base class for all market data sources."""

    name: str = "base"

    def __init__(self) -> None:
        """Initialize the data source."""
        self._connected = False
        self.logger = logging.getLogger(f"{__name__}.{self.name}")

    @property
    def connected(self) -> bool:
        """Check if data source is connected."""
        return self._connected

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the data source."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to the data source."""
        ...

    @abstractmethod
    async def get_markets(self) -> list[Market]:
        """Fetch all available markets."""
        ...

    @abstractmethod
    async def get_market(self, market_id: str) -> Market | None:
        """Fetch a specific market by ID."""
        ...

    async def __aenter__(self) -> "BaseDataSource":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        """Async context manager exit."""
        await self.disconnect()

    def __repr__(self) -> str:
        """String representation."""
        status = "connected" if self._connected else "disconnected"
        return f"<{self.__class__.__name__} ({status})>"
