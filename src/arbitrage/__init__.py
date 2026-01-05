"""Arbitrage detection modules."""

from src.arbitrage.cross_platform import CrossPlatformDetector
from src.arbitrage.detector import ArbitrageEngine
from src.arbitrage.hedging import HedgingDetector
from src.arbitrage.internal import InternalArbDetector

__all__ = [
    "ArbitrageEngine",
    "InternalArbDetector",
    "CrossPlatformDetector",
    "HedgingDetector",
]
