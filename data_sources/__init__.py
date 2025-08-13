# futures_data_manager/data_sources/__init__.py
"""
Data sources module for Futures Data Manager
"""

from .interactive_brokers import IBDataSource, IBConnectionManager, download_multiple_instruments
from .base_data_source import BaseDataSource

__all__ = [
    "IBDataSource",
    "IBConnectionManager", 
    "download_multiple_instruments",
    "BaseDataSource"
]