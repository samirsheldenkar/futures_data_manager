# futures_data_manager/config/__init__.py
"""
Configuration module for Futures Data Manager
"""

from futures_data_manager.config.instruments import (
    InstrumentConfig,
    InstrumentInfo, 
    AssetClass,
    Region,
    MAJOR_EQUITY_INDICES,
    MAJOR_BONDS,
    MAJOR_COMMODITIES,
    MAJOR_FX,
    CORE_PORTFOLIO,
    DEFAULT_ROLL_PARAMETERS
)

__all__ = [
    "InstrumentConfig",
    "InstrumentInfo",
    "AssetClass", 
    "Region",
    "MAJOR_EQUITY_INDICES",
    "MAJOR_BONDS", 
    "MAJOR_COMMODITIES",
    "MAJOR_FX",
    "CORE_PORTFOLIO",
    "DEFAULT_ROLL_PARAMETERS"
]