# futures_data_manager/price_processing/__init__.py
"""
Price processing modules for futures data.
"""

from futures_data_manager.price_processing.multiple_prices import MultiplePricesProcessor
from futures_data_manager.price_processing.adjusted_prices import AdjustedPricesProcessor
from futures_data_manager.price_processing.contract_stitcher import ContractStitcher

__all__ = [
    "MultiplePricesProcessor",
    "AdjustedPricesProcessor",
    "ContractStitcher"
]