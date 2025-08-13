"""
Parquet storage module for efficient storage and retrieval of futures price data.
"""

import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger


class ParquetStorage:
    """
    Efficient storage and retrieval of futures data using Apache Parquet format.
    
    Organizes data into a structured directory layout:
    - contract_prices/: Individual futures contract OHLCV data
    - multiple_prices/: Current/Forward/Carry price series
    - adjusted_prices/: Back-adjusted continuous series
    - roll_calendars/: Roll date schedules (CSV format)
    - fx_data/: Spot FX rates for currency conversion
    """
    
    def __init__(self, base_path: Union[str, Path]):
        """
        Initialize Parquet storage.
        
        Args:
            base_path: Base directory for data storage
        """
        self.base_path = Path(base_path)
        self._create_directory_structure()
        
        # Directory paths
        self.contract_prices_path = self.base_path / "contract_prices"
        self.multiple_prices_path = self.base_path / "multiple_prices"
        self.adjusted_prices_path = self.base_path / "adjusted_prices"
        self.roll_calendars_path = self.base_path / "roll_calendars"
        self.fx_data_path = self.base_path / "fx_data"
        
        logger.info(f"Initialized ParquetStorage at {base_path}")
    
    def _create_directory_structure(self) -> None:
        """Create the required directory structure."""
        directories = [
            "contract_prices",
            "multiple_prices",
            "adjusted_prices", 
            "roll_calendars",
            "fx_data",
            "metadata",
            "temp"
        ]
        
        for directory in directories:
            (self.base_path / directory).mkdir(parents=True, exist_ok=True)
    
    # Contract Prices Storage
    
    def write_contract_prices(
        self,
        instrument_code: str,
        contract_id: str,
        data: pd.DataFrame,
        compression: str = "snappy"
    ) -> None:
        """
        Store individual futures contract price data.
        
        Args:
            instrument_code: Instrument identifier (e.g., 'SP500')
            contract_id: Contract identifier (e.g., '20240315')
            data: OHLCV DataFrame with datetime index
            compression: Parquet compression method
        """
        if data.empty:
            logger.warning(f"Empty data for {instrument_code} {contract_id}, not writing")
            return
        
        try:
            # Validate data format
            data = self._validate_price_data(data)
            
            # Create filename
            filename = f"{instrument_code}_{contract_id}.parquet"
            filepath = self.contract_prices_path / filename
            
            # Add metadata
            data.attrs["instrument_code"] = instrument_code
            data.attrs["contract_id"] = contract_id
            data.attrs["last_updated"] = datetime.now().isoformat()
            
            # Write to parquet
            data.to_parquet(
                filepath,
                compression=compression,
                index=True,
                engine="pyarrow"
            )
            
            logger.debug(f"Wrote {len(data)} rows to {filepath}")
            
        except Exception as e:
            logger.error(f"Error writing contract prices for {instrument_code} {contract_id}: {e}")
            raise
    
    def read_contract_prices(
        self,
        instrument_code: str,
        contract_id: str
    ) -> pd.DataFrame:
        """
        Read individual futures contract price data.
        
        Args:
            instrument_code: Instrument identifier
            contract_id: Contract identifier
            
        Returns:
            OHLCV DataFrame
        """
        filename = f"{instrument_code}_{contract_id}.parquet"
        filepath = self.contract_prices_path / filename
        
        if not filepath.exists():
            logger.warning(f"Contract prices file not found: {filepath}")
            return pd.DataFrame()
        
        try:
            data = pd.read_parquet(filepath, engine="pyarrow")
            logger.debug(f"Read {len(data)} rows from {filepath}")
            return data
            
        except Exception as e:
            logger.error(f"Error reading contract prices from {filepath}: {e}")
            return pd.DataFrame()
    
    def contract_exists(self, instrument_code: str, contract_id: str) -> bool:
        """Check if contract data exists."""
        filename = f"{instrument_code}_{contract_id}.parquet"
        filepath = self.contract_prices_path / filename
        return filepath.exists()
    
    def list_contracts(self, instrument_code: str) -> List[str]:
        """List all available contracts for an instrument."""
        pattern = f"{instrument_code}_*.parquet"
        files = list(self.contract_prices_path.glob(pattern))
        
        contracts = []
        for file in files:
            # Extract contract ID from filename
            parts = file.stem.split("_")
            if len(parts) >= 2:
                contract_id = "_".join(parts[1:])
                contracts.append(contract_id)
        
        return sorted(contracts)
    
    # Multiple Prices Storage
    
    def write_multiple_prices(
        self,
        instrument_code: str,
        data: pd.DataFrame,
        compression: str = "snappy"
    ) -> None:
        """
        Store multiple prices (current/forward/carry) data.
        
        Args:
            instrument_code: Instrument identifier
            data: Multiple prices DataFrame
            compression: Parquet compression method
        """
        if data.empty:
            logger.warning(f"Empty multiple prices data for {instrument_code}, not writing")
            return
        
        try:
            # Validate multiple prices format
            data = self._validate_multiple_prices_data(data)
            
            filename = f"{instrument_code}_multiple.parquet"
            filepath = self.multiple_prices_path / filename
            
            # Add metadata
            data.attrs["instrument_code"] = instrument_code
            data.attrs["data_type"] = "multiple_prices"
            data.attrs["last_updated"] = datetime.now().isoformat()
            
            # Write to parquet
            data.to_parquet(
                filepath,
                compression=compression,
                index=True,
                engine="pyarrow"
            )
            
            logger.debug(f"Wrote {len(data)} rows of multiple prices to {filepath}")
            
        except Exception as e:
            logger.error(f"Error writing multiple prices for {instrument_code}: {e}")
            raise
    
    def read_multiple_prices(self, instrument_code: str) -> pd.DataFrame:
        """
        Read multiple prices data for an instrument.
        
        Args:
            instrument_code: Instrument identifier
            
        Returns:
            Multiple prices DataFrame
        """
        filename = f"{instrument_code}_multiple.parquet"
        filepath = self.multiple_prices_path / filename
        
        if not filepath.exists():
            logger.warning(f"Multiple prices file not found: {filepath}")
            return pd.DataFrame()
        
        try:
            data = pd.read_parquet(filepath, engine="pyarrow")
            logger.debug(f"Read {len(data)} rows of multiple prices from {filepath}")
            return data
            
        except Exception as e:
            logger.error(f"Error reading multiple prices from {filepath}: {e}")
            return pd.DataFrame()
    
    # Adjusted Prices Storage
    
    def write_adjusted_prices(
        self,
        instrument_code: str,
        data: pd.DataFrame,
        compression: str = "snappy"
    ) -> None:
        """
        Store back-adjusted continuous price series.
        
        Args:
            instrument_code: Instrument identifier
            data: Adjusted prices DataFrame
            compression: Parquet compression method
        """
        if data.empty:
            logger.warning(f"Empty adjusted prices data for {instrument_code}, not writing")
            return
        
        try:
            # Validate adjusted prices format
            data = self._validate_adjusted_prices_data(data)
            
            filename = f"{instrument_code}_adjusted.parquet"
            filepath = self.adjusted_prices_path / filename
            
            # Add metadata
            data.attrs["instrument_code"] = instrument_code
            data.attrs["data_type"] = "adjusted_prices"
            data.attrs["last_updated"] = datetime.now().isoformat()
            
            # Write to parquet
            data.to_parquet(
                filepath,
                compression=compression,
                index=True,
                engine="pyarrow"
            )
            
            logger.debug(f"Wrote {len(data)} rows of adjusted prices to {filepath}")
            
        except Exception as e:
            logger.error(f"Error writing adjusted prices for {instrument_code}: {e}")
            raise
    
    def read_adjusted_prices(self, instrument_code: str) -> pd.DataFrame:
        """
        Read back-adjusted continuous price series.
        
        Args:
            instrument_code: Instrument identifier
            
        Returns:
            Adjusted prices DataFrame
        """
        filename = f"{instrument_code}_adjusted.parquet"
        filepath = self.adjusted_prices_path / filename
        
        if not filepath.exists():
            logger.warning(f"Adjusted prices file not found: {filepath}")
            return pd.DataFrame()
        
        try:
            data = pd.read_parquet(filepath, engine="pyarrow")
            logger.debug(f"Read {len(data)} rows of adjusted prices from {filepath}")
            return data
            
        except Exception as e:
            logger.error(f"Error reading adjusted prices from {filepath}: {e}")
            return pd.DataFrame()
    
    # Roll Calendar Storage (CSV format for human readability)
    
    def write_roll_calendar(
        self,
        instrument_code: str,
        roll_calendar: pd.DataFrame
    ) -> None:
        """
        Store roll calendar in CSV format.
        
        Args:
            instrument_code: Instrument identifier
            roll_calendar: Roll calendar DataFrame
        """
        if roll_calendar.empty:
            logger.warning(f"Empty roll calendar for {instrument_code}, not writing")
            return
        
        try:
            filename = f"{instrument_code}_roll_calendar.csv"
            filepath = self.roll_calendars_path / filename
            
            # Write to CSV with proper formatting
            roll_calendar.to_csv(filepath, index=True, date_format="%Y-%m-%d")
            
            logger.debug(f"Wrote {len(roll_calendar)} roll dates to {filepath}")
            
        except Exception as e:
            logger.error(f"Error writing roll calendar for {instrument_code}: {e}")
            raise
    
    def read_roll_calendar(self, instrument_code: str) -> pd.DataFrame:
        """
        Read roll calendar for an instrument.
        
        Args:
            instrument_code: Instrument identifier
            
        Returns:
            Roll calendar DataFrame
        """
        filename = f"{instrument_code}_roll_calendar.csv"
        filepath = self.roll_calendars_path / filename
        
        if not filepath.exists():
            logger.warning(f"Roll calendar file not found: {filepath}")
            return pd.DataFrame()
        
        try:
            data = pd.read_csv(filepath, index_col=0, parse_dates=True)
            logger.debug(f"Read {len(data)} roll dates from {filepath}")
            return data
            
        except Exception as e:
            logger.error(f"Error reading roll calendar from {filepath}: {e}")
            return pd.DataFrame()
    
    # Utility Methods
    
    def get_existing_instruments(self) -> List[str]:
        """Get list of all instruments with data."""
        instruments = set()
        
        # Check adjusted prices (most comprehensive indicator)
        for file in self.adjusted_prices_path.glob("*_adjusted.parquet"):
            instrument_code = file.stem.replace("_adjusted", "")
            instruments.add(instrument_code)
        
        return sorted(list(instruments))
    
    def get_data_summary(self, instrument_code: str) -> Dict[str, Any]:
        """Get summary information about stored data for an instrument."""
        summary = {
            "instrument_code": instrument_code,
            "has_adjusted_prices": False,
            "has_multiple_prices": False,
            "has_roll_calendar": False,
            "contract_count": 0,
            "date_range": None,
            "last_updated": None
        }
        
        try:
            # Check adjusted prices
            adjusted_data = self.read_adjusted_prices(instrument_code)
            if not adjusted_data.empty:
                summary["has_adjusted_prices"] = True
                summary["date_range"] = (adjusted_data.index.min(), adjusted_data.index.max())
            
            # Check multiple prices
            multiple_data = self.read_multiple_prices(instrument_code)
            summary["has_multiple_prices"] = not multiple_data.empty
            
            # Check roll calendar
            roll_calendar = self.read_roll_calendar(instrument_code)
            summary["has_roll_calendar"] = not roll_calendar.empty
            
            # Count contracts
            contracts = self.list_contracts(instrument_code)
            summary["contract_count"] = len(contracts)
            
            # Get last update time from adjusted prices metadata
            if summary["has_adjusted_prices"]:
                filepath = self.adjusted_prices_path / f"{instrument_code}_adjusted.parquet"
                if filepath.exists():
                    stat = filepath.stat()
                    summary["last_updated"] = datetime.fromtimestamp(stat.st_mtime)
            
        except Exception as e:
            logger.error(f"Error getting data summary for {instrument_code}: {e}")
        
        return summary
    
    def _validate_price_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Validate and clean price data format."""
        if data.empty:
            return data
        
        # Ensure datetime index
        if not isinstance(data.index, pd.DatetimeIndex):
            if "date" in data.columns:
                data = data.set_index("date")
            else:
                raise ValueError("Data must have datetime index or 'date' column")
        
        # Check required columns
        required_columns = ["OPEN", "HIGH", "LOW", "CLOSE"]
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Ensure numeric data
        for col in required_columns + ["VOLUME"]:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors="coerce")
        
        # Basic validation
        if (data["HIGH"] < data["LOW"]).any():
            logger.warning("Found HIGH < LOW, fixing...")
            # Swap HIGH and LOW where necessary
            mask = data["HIGH"] < data["LOW"]
            data.loc[mask, ["HIGH", "LOW"]] = data.loc[mask, ["LOW", "HIGH"]].values
        
        # Ensure OHLC consistency
        data["HIGH"] = data[["OPEN", "HIGH", "LOW", "CLOSE"]].max(axis=1)
        data["LOW"] = data[["OPEN", "HIGH", "LOW", "CLOSE"]].min(axis=1)
        
        return data
    
    def _validate_multiple_prices_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Validate multiple prices data format."""
        required_columns = ["PRICE", "FORWARD", "CARRY", 
                          "PRICE_CONTRACT", "FORWARD_CONTRACT", "CARRY_CONTRACT"]
        
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            raise ValueError(f"Multiple prices missing required columns: {missing_columns}")
        
        return data
    
    def _validate_adjusted_prices_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Validate adjusted prices data format."""
        if "PRICE" not in data.columns and "CLOSE" not in data.columns:
            raise ValueError("Adjusted prices must have 'PRICE' or 'CLOSE' column")
        
        # Standardize to PRICE column
        if "CLOSE" in data.columns and "PRICE" not in data.columns:
            data["PRICE"] = data["CLOSE"]
        
        return data
    
    def delete_instrument_data(self, instrument_code: str) -> None:
        """Delete all data for an instrument."""
        try:
            # Delete adjusted prices
            adjusted_file = self.adjusted_prices_path / f"{instrument_code}_adjusted.parquet"
            if adjusted_file.exists():
                adjusted_file.unlink()
            
            # Delete multiple prices
            multiple_file = self.multiple_prices_path / f"{instrument_code}_multiple.parquet"
            if multiple_file.exists():
                multiple_file.unlink()
            
            # Delete roll calendar
            roll_file = self.roll_calendars_path / f"{instrument_code}_roll_calendar.csv"
            if roll_file.exists():
                roll_file.unlink()
            
            # Delete contract prices
            contracts = self.list_contracts(instrument_code)
            for contract_id in contracts:
                contract_file = self.contract_prices_path / f"{instrument_code}_{contract_id}.parquet"
                if contract_file.exists():
                    contract_file.unlink()
            
            logger.info(f"Deleted all data for {instrument_code}")
            
        except Exception as e:
            logger.error(f"Error deleting data for {instrument_code}: {e}")
            raise
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        stats = {
            "total_instruments": len(self.get_existing_instruments()),
            "total_files": 0,
            "total_size_mb": 0,
            "by_type": {}
        }
        
        type_paths = {
            "contract_prices": self.contract_prices_path,
            "multiple_prices": self.multiple_prices_path,
            "adjusted_prices": self.adjusted_prices_path,
            "roll_calendars": self.roll_calendars_path,
            "fx_data": self.fx_data_path
        }
        
        for data_type, path in type_paths.items():
            if path.exists():
                files = list(path.glob("*"))
                file_count = len(files)
                total_size = sum(f.stat().st_size for f in files if f.is_file())
                
                stats["by_type"][data_type] = {
                    "file_count": file_count,
                    "size_mb": total_size / (1024 * 1024)
                }
                
                stats["total_files"] += file_count
                stats["total_size_mb"] += total_size / (1024 * 1024)
        
        return stats