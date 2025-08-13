# futures_data_manager/__init__.py
"""
Futures Data Manager

A self-contained Python package for downloading and updating futures price series 
using Interactive Brokers, based on the pysystemtrade framework.
"""

from futures_data_manager.main import FuturesDataManager
from futures_data_manager.config.instruments import InstrumentConfig
from futures_data_manager.data_sources.interactive_brokers import IBDataSource
from futures_data_manager.data_storage.parquet_storage import ParquetStorage
from futures_data_manager.roll_calendars.roll_calendar_generator import RollCalendarGenerator

__version__ = "1.0.0"
__author__ = "Futures Data Manager Team"

__all__ = [
    "FuturesDataManager",
    "InstrumentConfig", 
    "IBDataSource",
    "ParquetStorage",
    "RollCalendarGenerator",
]