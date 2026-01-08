"""Arbitrage detection modules."""

from pylo.arbitrage.cross_platform import CrossPlatformDetector
from pylo.arbitrage.detector import ArbitrageEngine
from pylo.arbitrage.hedging import HedgingDetector
from pylo.arbitrage.internal import InternalArbDetector

__all__ = [
    "ArbitrageEngine",
    "InternalArbDetector",
    "CrossPlatformDetector",
    "HedgingDetector",
]
