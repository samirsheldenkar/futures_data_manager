# futures_data_manager/data_sources/__init__.py
"""
Data source modules for futures data providers.
"""

from futures_data_manager.data_sources.interactive_brokers import IBDataSource, IBConnectionManager, download_multiple_instruments
from futures_data_manager.data_sources.base_data_source import BaseDataSource

__all__ = [
    "IBDataSource",
    "IBConnectionManager", 
    "download_multiple_instruments",
    "BaseDataSource"
]