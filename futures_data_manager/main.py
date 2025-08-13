"""
Main module for Futures Data Manager
Orchestrates the downloading, processing, and storage of futures price data
"""

import os
import asyncio
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import pandas as pd
from loguru import logger

from futures_data_manager.config.instruments import InstrumentConfig
from futures_data_manager.data_sources.interactive_brokers import IBDataSource
from futures_data_manager.data_storage.parquet_storage import ParquetStorage
from futures_data_manager.roll_calendars.roll_calendar_generator import RollCalendarGenerator
from futures_data_manager.price_processing.multiple_prices import MultiplePricesProcessor
from futures_data_manager.price_processing.adjusted_prices import AdjustedPricesProcessor
from futures_data_manager.utils.date_utils import get_business_days_between


class FuturesDataManager:
    """
    Main class for managing futures data operations.
    
    This class orchestrates the entire data pipeline:
    1. Download individual contract prices from Interactive Brokers
    2. Generate roll calendars based on contract data and parameters
    3. Create multiple price series (current/forward/carry)
    4. Generate back-adjusted continuous price series
    5. Store all data in Parquet format
    """
    
    def __init__(
        self,
        data_path: str,
        ib_host: str = "127.0.0.1",
        ib_port: int = 7497,
        ib_client_id: int = 1,
        log_level: str = "INFO"
    ):
        """
        Initialize the Futures Data Manager.
        
        Args:
            data_path: Base path for storing data files
            ib_host: Interactive Brokers TWS/Gateway host
            ib_port: Interactive Brokers TWS/Gateway port
            ib_client_id: Unique client ID for IB connection
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.data_path = Path(data_path)
        self.ib_host = ib_host
        self.ib_port = ib_port
        self.ib_client_id = ib_client_id
        
        # Configure logging
        logger.remove()
        logger.add(
            self.data_path / "logs" / "futures_data_manager.log",
            level=log_level,
            rotation="10 MB",
            retention="30 days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}"
        )
        logger.add(lambda msg: print(msg), level=log_level)
        
        # Initialize components
        self.instrument_config = InstrumentConfig()
        self.ib_source = IBDataSource(ib_host, ib_port, ib_client_id)
        self.storage = ParquetStorage(data_path)
        self.roll_calendar_generator = RollCalendarGenerator()
        self.multiple_processor = MultiplePricesProcessor()
        self.adjusted_processor = AdjustedPricesProcessor()
        
        # Create directory structure
        self._create_directories()
        
        logger.info(f"Initialized FuturesDataManager with data path: {data_path}")
    
    def _create_directories(self) -> None:
        """Create the required directory structure."""
        directories = [
            "contract_prices",
            "multiple_prices", 
            "adjusted_prices",
            "roll_calendars",
            "fx_data",
            "logs",
            "temp"
        ]
        
        for directory in directories:
            (self.data_path / directory).mkdir(parents=True, exist_ok=True)
    
    async def download_and_process_instruments(
        self, 
        instruments: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        update_mode: bool = False
    ) -> None:
        """
        Download and process data for specified instruments.
        
        Args:
            instruments: List of instrument codes to process
            start_date: Start date for historical data (YYYYMMDD format)
            end_date: End date for historical data (YYYYMMDD format)
            update_mode: If True, only update existing data
        """
        logger.info(f"Starting processing for {len(instruments)} instruments: {instruments}")
        
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
        
        # Connect to Interactive Brokers
        await self.ib_source.connect()
        
        try:
            for instrument in instruments:
                logger.info(f"Processing instrument: {instrument}")
                await self._process_single_instrument(
                    instrument, start_date, end_date, update_mode
                )
        finally:
            await self.ib_source.disconnect()
    
    async def _process_single_instrument(
        self,
        instrument_code: str,
        start_date: str,
        end_date: str,
        update_mode: bool
    ) -> None:
        """Process a single instrument through the complete data pipeline."""
        try:
            # Get instrument configuration
            config = self.instrument_config.get_config(instrument_code)
            if not config:
                logger.error(f"No configuration found for instrument: {instrument_code}")
                return
            
            # Step 1: Download individual contract prices
            logger.info(f"Downloading contract prices for {instrument_code}")
            contract_prices = await self._download_contract_prices(
                instrument_code, config, start_date, end_date, update_mode
            )
            
            if not contract_prices:
                logger.warning(f"No contract prices downloaded for {instrument_code}")
                return
            
            # Step 2: Generate roll calendar
            logger.info(f"Generating roll calendar for {instrument_code}")
            roll_calendar = self._generate_roll_calendar(instrument_code, contract_prices, config)
            
            # Step 3: Create multiple prices
            logger.info(f"Creating multiple prices for {instrument_code}")
            multiple_prices = self._create_multiple_prices(
                instrument_code, contract_prices, roll_calendar
            )
            
            # Step 4: Create adjusted prices
            logger.info(f"Creating adjusted prices for {instrument_code}")
            adjusted_prices = self._create_adjusted_prices(instrument_code, multiple_prices)
            
            # Step 5: Store all data
            logger.info(f"Storing data for {instrument_code}")
            await self._store_instrument_data(
                instrument_code, contract_prices, multiple_prices, 
                adjusted_prices, roll_calendar
            )
            
            logger.success(f"Successfully processed {instrument_code}")
            
        except Exception as e:
            logger.error(f"Error processing {instrument_code}: {e}")
            raise
    
    async def _download_contract_prices(
        self,
        instrument_code: str,
        config: Dict[str, Any],
        start_date: str,
        end_date: str,
        update_mode: bool
    ) -> Dict[str, pd.DataFrame]:
        """Download historical prices for all contracts of an instrument."""
        
        # Get contract specifications
        contracts = self._get_contract_list(instrument_code, config, start_date, end_date)
        contract_prices = {}
        
        for contract_id in contracts:
            try:
                # Check if we already have this data and in update mode
                if update_mode and self.storage.contract_exists(instrument_code, contract_id):
                    existing_data = self.storage.read_contract_prices(instrument_code, contract_id)
                    if not existing_data.empty:
                        # Get only recent data to append
                        last_date = existing_data.index[-1]
                        update_start = (last_date + timedelta(days=1)).strftime("%Y%m%d")
                        
                        if update_start <= end_date:
                            logger.info(f"Updating {instrument_code} {contract_id} from {update_start}")
                            new_data = await self.ib_source.get_historical_data(
                                instrument_code, contract_id, update_start, end_date
                            )
                            
                            if not new_data.empty:
                                # Concatenate and deduplicate
                                combined_data = pd.concat([existing_data, new_data])
                                combined_data = combined_data[~combined_data.index.duplicated(keep='last')]
                                contract_prices[contract_id] = combined_data
                            else:
                                contract_prices[contract_id] = existing_data
                        else:
                            contract_prices[contract_id] = existing_data
                        continue
                
                # Download full historical data
                logger.info(f"Downloading {instrument_code} {contract_id}")
                data = await self.ib_source.get_historical_data(
                    instrument_code, contract_id, start_date, end_date
                )
                
                if not data.empty:
                    contract_prices[contract_id] = data
                    logger.debug(f"Downloaded {len(data)} bars for {contract_id}")
                else:
                    logger.warning(f"No data received for {contract_id}")
                    
            except Exception as e:
                logger.error(f"Error downloading {contract_id}: {e}")
                continue
        
        return contract_prices
    
    def _get_contract_list(
        self,
        instrument_code: str,
        config: Dict[str, Any],
        start_date: str,
        end_date: str
    ) -> List[str]:
        """Generate list of contract identifiers to download."""
        
        # Get roll cycle and generate contract months
        hold_cycle = config.get("hold_cycle", config.get("priced_cycle", "HMUZ"))
        
        # Convert dates
        start_dt = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d")
        
        # Generate contract list covering the date range plus buffer
        contracts = []
        current_year = start_dt.year - 1  # Start one year earlier for safety
        end_year = end_dt.year + 1        # End one year later for safety
        
        month_map = {
            'F': '01', 'G': '02', 'H': '03', 'J': '04', 'K': '05', 'M': '06',
            'N': '07', 'Q': '08', 'U': '09', 'V': '10', 'X': '11', 'Z': '12'
        }
        
        for year in range(current_year, end_year + 1):
            for month_code in hold_cycle:
                if month_code in month_map:
                    month = month_map[month_code]
                    contract_id = f"{year}{month}00"
                    contracts.append(contract_id)
        
        return sorted(contracts)
    
    def _generate_roll_calendar(
        self,
        instrument_code: str,
        contract_prices: Dict[str, pd.DataFrame],
        config: Dict[str, Any]
    ) -> pd.DataFrame:
        """Generate roll calendar for the instrument."""
        
        return self.roll_calendar_generator.generate_from_prices(
            instrument_code=instrument_code,
            contract_prices=contract_prices,
            roll_parameters={
                "hold_cycle": config.get("hold_cycle", "HMUZ"),
                "priced_cycle": config.get("priced_cycle", "HMUZ"),
                "roll_offset_days": config.get("roll_offset_days", -5),
                "carry_offset": config.get("carry_offset", -1),
                "expiry_offset": config.get("expiry_offset", 0)
            }
        )
    
    def _create_multiple_prices(
        self,
        instrument_code: str,
        contract_prices: Dict[str, pd.DataFrame],
        roll_calendar: pd.DataFrame
    ) -> pd.DataFrame:
        """Create multiple prices (current/forward/carry) from contract prices."""
        
        return self.multiple_processor.create_from_contract_prices(
            contract_prices=contract_prices,
            roll_calendar=roll_calendar
        )
    
    def _create_adjusted_prices(
        self,
        instrument_code: str,
        multiple_prices: pd.DataFrame
    ) -> pd.DataFrame:
        """Create back-adjusted continuous price series."""
        
        return self.adjusted_processor.create_from_multiple_prices(
            multiple_prices=multiple_prices,
            method="panama"  # Default stitching method
        )
    
    async def _store_instrument_data(
        self,
        instrument_code: str,
        contract_prices: Dict[str, pd.DataFrame],
        multiple_prices: pd.DataFrame,
        adjusted_prices: pd.DataFrame,
        roll_calendar: pd.DataFrame
    ) -> None:
        """Store all processed data for an instrument."""
        
        # Store individual contract prices
        for contract_id, prices in contract_prices.items():
            self.storage.write_contract_prices(instrument_code, contract_id, prices)
        
        # Store multiple prices
        self.storage.write_multiple_prices(instrument_code, multiple_prices)
        
        # Store adjusted prices
        self.storage.write_adjusted_prices(instrument_code, adjusted_prices)
        
        # Store roll calendar (CSV format for human readability)
        self.storage.write_roll_calendar(instrument_code, roll_calendar)
    
    # Public API methods
    
    async def update_all_instruments(self) -> None:
        """Update all existing instruments with latest data."""
        existing_instruments = self.storage.get_existing_instruments()
        logger.info(f"Updating {len(existing_instruments)} existing instruments")
        
        await self.download_and_process_instruments(
            instruments=existing_instruments,
            update_mode=True
        )
    
    async def update_instruments(self, instruments: List[str]) -> None:
        """Update specific instruments with latest data."""
        await self.download_and_process_instruments(
            instruments=instruments,
            update_mode=True
        )
    
    async def add_new_instrument(
        self,
        instrument_code: str,
        start_date: str,
        end_date: Optional[str] = None
    ) -> None:
        """Add a new instrument with full historical data."""
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
        
        await self.download_and_process_instruments(
            instruments=[instrument_code],
            start_date=start_date,
            end_date=end_date,
            update_mode=False
        )
    
    def get_adjusted_prices(self, instrument_code: str) -> pd.DataFrame:
        """Get back-adjusted continuous price series for an instrument."""
        return self.storage.read_adjusted_prices(instrument_code)
    
    def get_multiple_prices(self, instrument_code: str) -> pd.DataFrame:
        """Get multiple prices (current/forward/carry) for an instrument."""
        return self.storage.read_multiple_prices(instrument_code)
    
    def get_contract_prices(self, instrument_code: str, contract_id: str) -> pd.DataFrame:
        """Get prices for a specific contract."""
        return self.storage.read_contract_prices(instrument_code, contract_id)
    
    def list_available_instruments(self) -> List[str]:
        """List all available instruments in the dataset."""
        return self.storage.get_existing_instruments()
    
    def get_instrument_info(self, instrument_code: str) -> Dict[str, Any]:
        """Get configuration information for an instrument."""
        return self.instrument_config.get_config(instrument_code)


def main():
    """Command line interface for the futures data manager."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Futures Data Manager")
    parser.add_argument("--data-path", required=True, help="Path to store data files")
    parser.add_argument("--instruments", nargs="+", help="Instrument codes to process")
    parser.add_argument("--start-date", help="Start date (YYYYMMDD)")
    parser.add_argument("--end-date", help="End date (YYYYMMDD)")
    parser.add_argument("--update", action="store_true", help="Update existing data")
    parser.add_argument("--ib-host", default="127.0.0.1", help="IB TWS host")
    parser.add_argument("--ib-port", type=int, default=7497, help="IB TWS port")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    
    args = parser.parse_args()
    
    async def async_main():
        # Initialize manager
        manager = FuturesDataManager(
            data_path=args.data_path,
            ib_host=args.ib_host,
            ib_port=args.ib_port,
            log_level=args.log_level
        )
        
        if args.instruments:
            if args.update:
                await manager.update_instruments(args.instruments)
            else:
                await manager.download_and_process_instruments(
                    instruments=args.instruments,
                    start_date=args.start_date,
                    end_date=args.end_date
                )
        else:
            if args.update:
                await manager.update_all_instruments()
            else:
                logger.info("No instruments specified. Use --instruments to specify instruments to process.")
    
    asyncio.run(async_main())


if __name__ == "__main__":
    main()