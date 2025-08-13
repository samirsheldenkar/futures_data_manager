"""
Interactive Brokers data source module using ib_insync for futures data download.
"""

import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import pandas as pd
from loguru import logger

try:
    from ib_insync import IB, Future, util
    from ib_insync.contract import Contract
except ImportError:
    logger.error("ib_insync not installed. Please install it: pip install ib_insync")
    raise

from futures_data_manager.data_sources.base_data_source import BaseDataSource


class IBDataSource(BaseDataSource):
    """
    Interactive Brokers data source for downloading futures price data.
    Uses ib_insync library for reliable connection and data retrieval.
    """
    
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 7497,
        client_id: int = 1,
        timeout: int = 10,
        max_requests_per_second: int = 50
    ):
        """
        Initialize IB data source.
        
        Args:
            host: IB TWS/Gateway host address
            port: IB TWS/Gateway port (7497 for TWS paper, 7496 for live)
            client_id: Unique client ID for this connection
            timeout: Connection timeout in seconds
            max_requests_per_second: Rate limiting for API requests
        """
        super().__init__()
        
        self.host = host
        self.port = port
        self.client_id = client_id
        self.timeout = timeout
        self.max_requests_per_second = max_requests_per_second
        
        self.ib = IB()
        self.connected = False
        self._request_times = []
    
    async def connect(self) -> bool:
        """Connect to Interactive Brokers TWS/Gateway."""
        try:
            logger.info(f"Connecting to IB at {self.host}:{self.port}")
            await self.ib.connectAsync(
                host=self.host,
                port=self.port,
                clientId=self.client_id,
                timeout=self.timeout
            )
            
            self.connected = True
            logger.success(f"Connected to IB successfully. Client ID: {self.client_id}")
            
            # Verify connection
            account_summary = self.ib.accountSummary()
            logger.info(f"Account summary retrieved: {len(account_summary)} items")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to IB: {e}")
            self.connected = False
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from Interactive Brokers."""
        if self.connected:
            self.ib.disconnect()
            self.connected = False
            logger.info("Disconnected from IB")
    
    def _rate_limit(self) -> None:
        """Implement rate limiting to avoid API limits."""
        now = datetime.now()
        
        # Remove requests older than 1 second
        self._request_times = [
            req_time for req_time in self._request_times
            if (now - req_time).total_seconds() < 1.0
        ]
        
        # If we're at the limit, wait
        if len(self._request_times) >= self.max_requests_per_second:
            sleep_time = 1.0 - (now - self._request_times[0]).total_seconds()
            if sleep_time > 0:
                asyncio.sleep(sleep_time)
        
        self._request_times.append(now)
    
    def _create_futures_contract(
        self,
        instrument_code: str,
        contract_month: str,
        ib_specs: Dict[str, Any]
    ) -> Future:
        """
        Create IB futures contract object.
        
        Args:
            instrument_code: Internal instrument code
            contract_month: Contract month in YYYYMM format
            ib_specs: IB contract specifications
            
        Returns:
            IB Future contract object
        """
        # Extract year and month
        year = int(contract_month[:4])
        month = int(contract_month[4:6])
        
        # Create contract
        contract = Future(
            symbol=ib_specs["symbol"],
            lastTradeDateOrContractMonth=f"{year}{month:02d}",
            exchange=ib_specs["exchange"],
            currency=ib_specs.get("currency", "USD"),
            multiplier=ib_specs.get("multiplier")
        )
        
        return contract
    
    async def get_historical_data(
        self,
        instrument_code: str,
        contract_month: str,
        start_date: str,
        end_date: str,
        bar_size: str = "1 day",
        what_to_show: str = "TRADES"
    ) -> pd.DataFrame:
        """
        Download historical data for a specific futures contract.
        
        Args:
            instrument_code: Internal instrument code
            contract_month: Contract month (YYYYMM00 format)
            start_date: Start date (YYYYMMDD format)
            end_date: End date (YYYYMMDD format) 
            bar_size: IB bar size specification
            what_to_show: Type of data to retrieve
            
        Returns:
            DataFrame with OHLCV data
        """
        if not self.connected:
            raise RuntimeError("Not connected to IB. Call connect() first.")
        
        try:
            # Rate limiting
            self._rate_limit()
            
            # Get IB contract specs from instrument config
            from futures_data_manager.config.instruments import InstrumentConfig
            config = InstrumentConfig()
            ib_specs = config.get_ib_contract_specs(instrument_code)
            
            if not ib_specs:
                raise ValueError(f"No IB specifications for instrument: {instrument_code}")
            
            # Create contract
            contract_id = contract_month[:6]  # Remove '00' suffix
            contract = self._create_futures_contract(instrument_code, contract_id, ib_specs)
            
            # Qualify contract to get detailed specifications
            qualified_contracts = await self.ib.qualifyContractsAsync(contract)
            if not qualified_contracts:
                logger.warning(f"Could not qualify contract {instrument_code} {contract_id}")
                return pd.DataFrame()
            
            contract = qualified_contracts[0]
            logger.debug(f"Qualified contract: {contract}")
            
            # Calculate duration
            start_dt = datetime.strptime(start_date, "%Y%m%d")
            end_dt = datetime.strptime(end_date, "%Y%m%d")
            duration_days = (end_dt - start_dt).days
            
            # IB duration string
            if duration_days <= 365:
                duration = f"{duration_days} D"
            else:
                duration = f"{duration_days // 365} Y"
            
            # Request historical data
            logger.info(f"Requesting {duration} of data for {instrument_code} {contract_id}")
            
            bars = await self.ib.reqHistoricalDataAsync(
                contract=contract,
                endDateTime=end_date,
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow=what_to_show,
                useRTH=True,  # Regular trading hours only
                formatDate=1   # Return as datetime
            )
            
            if not bars:
                logger.warning(f"No data returned for {instrument_code} {contract_id}")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = util.df(bars)
            
            if df.empty:
                logger.warning(f"Empty DataFrame for {instrument_code} {contract_id}")
                return df
            
            # Clean and format data
            df = self._format_price_data(df)
            
            # Filter by date range
            df = df.loc[start_dt:end_dt]
            
            logger.info(f"Downloaded {len(df)} bars for {instrument_code} {contract_id}")
            return df
            
        except Exception as e:
            logger.error(f"Error downloading data for {instrument_code} {contract_month}: {e}")
            return pd.DataFrame()
    
    def _format_price_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Format raw IB data to standard format.
        
        Args:
            df: Raw DataFrame from IB
            
        Returns:
            Formatted DataFrame with standard column names
        """
        if df.empty:
            return df
        
        # Rename columns to standard format
        column_mapping = {
            "open": "OPEN",
            "high": "HIGH", 
            "low": "LOW",
            "close": "CLOSE",
            "volume": "VOLUME"
        }
        
        df = df.rename(columns=column_mapping)
        
        # Ensure datetime index
        if "date" in df.columns:
            df = df.set_index("date")
        
        # Convert to numeric
        price_columns = ["OPEN", "HIGH", "LOW", "CLOSE"]
        for col in price_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        if "VOLUME" in df.columns:
            df["VOLUME"] = pd.to_numeric(df["VOLUME"], errors="coerce").fillna(0)
        
        # Remove any rows with all NaN prices
        df = df.dropna(subset=price_columns, how="all")
        
        # Forward fill missing prices (common for low-volume contracts)
        df[price_columns] = df[price_columns].fillna(method="ffill")
        
        return df
    
    async def get_contract_details(
        self,
        instrument_code: str,
        contract_month: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get detailed contract specifications from IB.
        
        Args:
            instrument_code: Internal instrument code
            contract_month: Contract month (YYYYMM format)
            
        Returns:
            Contract details dictionary
        """
        if not self.connected:
            raise RuntimeError("Not connected to IB")
        
        try:
            # Get IB contract specs
            from futures_data_manager.config.instruments import InstrumentConfig
            config = InstrumentConfig()
            ib_specs = config.get_ib_contract_specs(instrument_code)
            
            if not ib_specs:
                return None
            
            # Create and qualify contract
            contract = self._create_futures_contract(instrument_code, contract_month, ib_specs)
            qualified_contracts = await self.ib.qualifyContractsAsync(contract)
            
            if not qualified_contracts:
                return None
            
            contract = qualified_contracts[0]
            
            # Get contract details
            details = await self.ib.reqContractDetailsAsync(contract)
            
            if not details:
                return None
            
            detail = details[0]
            
            return {
                "symbol": contract.symbol,
                "exchange": contract.exchange,
                "currency": contract.currency,
                "multiplier": contract.multiplier,
                "expiry": detail.contract.lastTradeDateOrContractMonth,
                "trading_hours": detail.tradingHours,
                "time_zone": detail.timeZoneId,
                "min_tick": detail.minTick,
                "contract_month": detail.contractMonth,
                "market_name": detail.marketName,
                "long_name": detail.longName
            }
            
        except Exception as e:
            logger.error(f"Error getting contract details for {instrument_code} {contract_month}: {e}")
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
        if not self.connected:
            raise RuntimeError("Not connected to IB")
        
        try:
            # Get IB contract specs
            from futures_data_manager.config.instruments import InstrumentConfig
            config = InstrumentConfig()
            ib_specs = config.get_ib_contract_specs(instrument_code)
            
            if not ib_specs:
                return []
            
            # Create generic contract for scanning
            contract = Future(
                symbol=ib_specs["symbol"],
                exchange=ib_specs["exchange"],
                currency=ib_specs.get("currency", "USD")
            )
            
            # Get contract details for all available months
            details = await self.ib.reqContractDetailsAsync(contract)
            
            # Extract contract months and sort
            contract_months = []
            for detail in details[:max_contracts]:
                last_trade_date = detail.contract.lastTradeDateOrContractMonth
                if len(last_trade_date) >= 6:
                    contract_month = last_trade_date[:6]  # YYYYMM
                    contract_months.append(contract_month)
            
            # Sort by date
            contract_months.sort()
            
            logger.info(f"Found {len(contract_months)} active contracts for {instrument_code}")
            return contract_months
            
        except Exception as e:
            logger.error(f"Error getting active contracts for {instrument_code}: {e}")
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
        try:
            # Try to get a small sample of recent data
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
            
            sample_data = await self.get_historical_data(
                instrument_code=instrument_code,
                contract_month=contract_month + "00",
                start_date=start_date,
                end_date=end_date
            )
            
            return {
                "available": not sample_data.empty,
                "last_date": sample_data.index[-1] if not sample_data.empty else None,
                "data_points": len(sample_data),
                "sample_price": sample_data["CLOSE"].iloc[-1] if not sample_data.empty else None
            }
            
        except Exception as e:
            logger.error(f"Error checking data availability for {instrument_code} {contract_month}: {e}")
            return {"available": False, "error": str(e)}


class IBConnectionManager:
    """Context manager for IB connections to ensure proper cleanup."""
    
    def __init__(self, ib_source: IBDataSource):
        self.ib_source = ib_source
    
    async def __aenter__(self):
        await self.ib_source.connect()
        return self.ib_source
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.ib_source.disconnect()


# Utility functions for common IB operations

async def download_multiple_instruments(
    instruments: List[str],
    start_date: str,
    end_date: str,
    ib_source: IBDataSource,
    max_concurrent: int = 5
) -> Dict[str, Dict[str, pd.DataFrame]]:
    """
    Download data for multiple instruments concurrently.
    
    Args:
        instruments: List of instrument codes
        start_date: Start date (YYYYMMDD)
        end_date: End date (YYYYMMDD)
        ib_source: Connected IB data source
        max_concurrent: Maximum concurrent downloads
        
    Returns:
        Dictionary mapping instrument -> contract -> DataFrame
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def download_instrument(instrument_code: str):
        async with semaphore:
            try:
                # Get active contracts
                contracts = await ib_source.get_active_contracts(instrument_code)
                
                # Download each contract
                contract_data = {}
                for contract_month in contracts:
                    data = await ib_source.get_historical_data(
                        instrument_code=instrument_code,
                        contract_month=contract_month + "00",
                        start_date=start_date,
                        end_date=end_date
                    )
                    if not data.empty:
                        contract_data[contract_month] = data
                
                return instrument_code, contract_data
                
            except Exception as e:
                logger.error(f"Error downloading {instrument_code}: {e}")
                return instrument_code, {}
    
    # Start all downloads
    tasks = [download_instrument(instrument) for instrument in instruments]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Collect results
    all_data = {}
    for result in results:
        if isinstance(result, tuple):
            instrument_code, contract_data = result
            all_data[instrument_code] = contract_data
        else:
            logger.error(f"Download task failed: {result}")
    
    return all_data