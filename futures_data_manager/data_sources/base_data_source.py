"""
Base data source class for futures data providers.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
import pandas as pd


class BaseDataSource(ABC):
    """
    Abstract base class for futures data sources.
    """
    
    @abstractmethod
    async def connect(self) -> bool:
        """Connect to the data source."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the data source."""
        pass
    
    @abstractmethod
    async def get_historical_data(
        self,
        instrument_code: str,
        contract_month: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """Get historical price data for a futures contract."""
        pass
    
    async def get_contract_details(
        self,
        instrument_code: str,
        contract_month: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get detailed contract specifications.
        
        Args:
            instrument_code: Internal instrument code
            contract_month: Contract month (YYYYMM format)
            
        Returns:
            Contract details dictionary or None
        """
        return None
    
    async def get_active_contracts(
        self,
        instrument_code: str,
        max_contracts: int = 20
    ) -> List[str]:
        """
        Get list of active contract months for an instrument.
        
        Args:
            instrument_code: Internal instrument code
            max_contracts: Maximum number of contracts to return
            
        Returns:
            List of contract months (YYYYMM format)
        """
        return []
    
    async def check_data_availability(
        self,
        instrument_code: str,
        contract_month: str
    ) -> Dict[str, Any]:
        """
        Check data availability for a contract.
        
        Args:
            instrument_code: Internal instrument code  
            contract_month: Contract month (YYYYMM format)
            
        Returns:
            Dictionary with availability information
        """
        return {"available": False, "error": "Not implemented"}
    
    def validate_connection(self) -> bool:
        """Validate that the data source connection is working."""
        return False