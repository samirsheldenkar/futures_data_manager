"""
Data objects for representing different types of futures data.
"""

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from datetime import datetime
import pandas as pd


@dataclass
class ContractSpec:
    """Specification for a futures contract."""
    instrument_code: str
    contract_id: str
    symbol: str
    exchange: str
    currency: str
    multiplier: float
    expiry_date: Optional[datetime] = None
    first_notice_date: Optional[datetime] = None
    last_trading_date: Optional[datetime] = None


@dataclass
class RollCalendar:
    """Roll calendar data structure."""
    instrument_code: str
    roll_dates: pd.DataFrame  # Index: dates, Columns: current_contract, next_contract, carry_contract
    
    def add_roll(
        self,
        roll_date: datetime,
        current_contract: str,
        next_contract: str,
        carry_contract: str
    ) -> None:
        """Add a roll to the calendar."""
        new_row = pd.DataFrame({
            "current_contract": [current_contract],
            "next_contract": [next_contract],
            "carry_contract": [carry_contract]
        }, index=[roll_date])
        
        self.roll_dates = pd.concat([self.roll_dates, new_row]).sort_index()
    
    def get_contracts_on_date(self, date: datetime) -> Optional[Dict[str, str]]:
        """Get contract specifications for a given date."""
        if self.roll_dates.empty:
            return None
        
        # Find the appropriate row
        valid_dates = self.roll_dates.index <= date
        if not valid_dates.any():
            return None
        
        row = self.roll_dates.loc[valid_dates].iloc[-1]
        return {
            "current": row["current_contract"],
            "next": row["next_contract"], 
            "carry": row["carry_contract"]
        }


@dataclass
class PriceData:
    """Container for price data."""
    instrument_code: str
    contract_id: str
    prices: pd.DataFrame  # OHLCV data
    metadata: Dict[str, Any]
    
    @property
    def start_date(self) -> Optional[datetime]:
        """Get start date of price data."""
        return self.prices.index.min() if not self.prices.empty else None
    
    @property
    def end_date(self) -> Optional[datetime]:
        """Get end date of price data.""" 
        return self.prices.index.max() if not self.prices.empty else None
    
    @property
    def close_prices(self) -> pd.Series:
        """Get close prices series."""
        return self.prices["CLOSE"] if "CLOSE" in self.prices.columns else pd.Series()


@dataclass
class MultiplePrices:
    """Multiple prices data structure."""
    instrument_code: str
    prices: pd.DataFrame  # Columns: PRICE, FORWARD, CARRY + contract IDs
    
    @property
    def current_prices(self) -> pd.Series:
        """Get current contract prices."""
        return self.prices["PRICE"] if "PRICE" in self.prices.columns else pd.Series()
    
    @property
    def forward_prices(self) -> pd.Series:
        """Get forward contract prices."""
        return self.prices["FORWARD"] if "FORWARD" in self.prices.columns else pd.Series()
    
    @property
    def carry_prices(self) -> pd.Series:
        """Get carry contract prices."""
        return self.prices["CARRY"] if "CARRY" in self.prices.columns else pd.Series()


@dataclass
class AdjustedPrices:
    """Back-adjusted continuous price series."""
    instrument_code: str
    prices: pd.Series  # Continuous price series
    method: str  # Stitching method used
    
    @property
    def returns(self) -> pd.Series:
        """Calculate returns from adjusted prices."""
        return self.prices.pct_change().dropna()
    
    @property
    def log_returns(self) -> pd.Series:
        """Calculate log returns from adjusted prices."""
        return (self.prices / self.prices.shift(1)).apply(lambda x: math.log(x) if x > 0 else float('nan')).dropna()


@dataclass  
class InstrumentData:
    """Complete data set for a futures instrument."""
    instrument_code: str
    config: Dict[str, Any]
    contract_prices: Dict[str, PriceData]
    multiple_prices: Optional[MultiplePrices] = None
    adjusted_prices: Optional[AdjustedPrices] = None
    roll_calendar: Optional[RollCalendar] = None
    
    @property
    def available_contracts(self) -> List[str]:
        """Get list of available contract IDs."""
        return list(self.contract_prices.keys())
    
    def get_contract_prices(self, contract_id: str) -> Optional[PriceData]:
        """Get price data for a specific contract."""
        return self.contract_prices.get(contract_id)