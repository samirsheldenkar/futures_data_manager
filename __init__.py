# futures_data_manager/__init__.py
"""
Futures Data Manager

A self-contained Python package for downloading and updating futures price series 
using Interactive Brokers, based on the pysystemtrade framework.
"""

from .main import FuturesDataManager
from .config.instruments import InstrumentConfig
from .data_sources.interactive_brokers import IBDataSource
from .data_storage.parquet_storage import ParquetStorage
from .roll_calendars.roll_calendar_generator import RollCalendarGenerator

__version__ = "1.0.0"
__author__ = "Futures Data Manager Team"

__all__ = [
    "FuturesDataManager",
    "InstrumentConfig", 
    "IBDataSource",
    "ParquetStorage",
    "RollCalendarGenerator",
]