"""
Complete Usage Examples for Futures Data Manager
"""

# Example 1: Basic usage
basic_usage_example = '''
import asyncio
from futures_data_manager import FuturesDataManager

async def basic_example():
    # Initialize the data manager
    manager = FuturesDataManager(
        data_path="./futures_data",
        ib_host="127.0.0.1",
        ib_port=7497,  # TWS paper trading port
        ib_client_id=1
    )
    
    # Download data for major instruments
    major_instruments = ["SP500", "DAX", "CRUDE_W", "GOLD", "EUR"]
    
    await manager.download_and_process_instruments(
        instruments=major_instruments,
        start_date="20230101",
        end_date="20241201"
    )
    
    # Get processed data
    sp500_adjusted = manager.get_adjusted_prices("SP500")
    sp500_multiple = manager.get_multiple_prices("SP500") 
    
    print(f"SP500 adjusted prices: {len(sp500_adjusted)} rows")
    print(f"Date range: {sp500_adjusted.index.min()} to {sp500_adjusted.index.max()}")

# Run the example
asyncio.run(basic_example())
'''

# Example 2: Advanced portfolio setup
portfolio_example = '''
import asyncio
from futures_data_manager import FuturesDataManager
from futures_data_manager.config.instruments import CORE_PORTFOLIO

async def setup_complete_portfolio():
    manager = FuturesDataManager(
        data_path="./data",
        ib_host="127.0.0.1", 
        ib_port=7497
    )
    
    print(f"Setting up portfolio with {len(CORE_PORTFOLIO)} instruments")
    
    # Download complete historical dataset
    await manager.download_and_process_instruments(
        instruments=CORE_PORTFOLIO,
        start_date="20100101",  # 14+ years of data
        end_date=None,  # Up to current date
        update_mode=False
    )
    
    # Verify data quality
    for instrument in CORE_PORTFOLIO:
        summary = manager.storage.get_data_summary(instrument)
        print(f"{instrument}: {summary['contract_count']} contracts, "
              f"{'✓' if summary['has_adjusted_prices'] else '✗'} adjusted, "
              f"{'✓' if summary['has_roll_calendar'] else '✗'} rolls")

asyncio.run(setup_complete_portfolio())
'''

# Example 3: Daily update workflow
update_example = '''
import asyncio
import schedule
import time
from futures_data_manager import FuturesDataManager

class DailyUpdater:
    def __init__(self):
        self.manager = FuturesDataManager(
            data_path="./production_data",
            ib_host="127.0.0.1",
            ib_port=7496,  # Live trading port
            log_level="INFO"
        )
    
    async def daily_update(self):
        """Run daily data updates."""
        try:
            print("Starting daily data update...")
            
            # Update all existing instruments
            await self.manager.update_all_instruments()
            
            # Get storage statistics
            stats = self.manager.storage.get_storage_stats()
            print(f"Updated data for {stats['total_instruments']} instruments")
            print(f"Total storage: {stats['total_size_mb']:.1f} MB")
            
        except Exception as e:
            print(f"Error in daily update: {e}")
    
    def schedule_updates(self):
        """Schedule daily updates."""
        # Update at 6 PM EST (after US markets close)
        schedule.every().day.at("18:00").do(
            lambda: asyncio.run(self.daily_update())
        )
        
        print("Daily updates scheduled for 6:00 PM EST")
        
        while True:
            schedule.run_pending()
            time.sleep(60)

# Usage
updater = DailyUpdater()
updater.schedule_updates()
'''

# Example 4: Custom instrument addition
custom_instrument_example = '''
import asyncio
from futures_data_manager import FuturesDataManager
from futures_data_manager.config.instruments import InstrumentConfig, InstrumentInfo, AssetClass, Region

async def add_custom_instrument():
    manager = FuturesDataManager(data_path="./data")
    
    # Add custom instrument configuration
    config = InstrumentConfig()
    
    # Example: Adding Bitcoin futures (if not already configured)
    custom_instrument = InstrumentInfo(
        instrument_code="BITCOIN_CUSTOM",
        description="Custom Bitcoin Futures",
        pointsize=5,
        currency="USD",
        asset_class=AssetClass.METALS,  # Treating crypto as metals
        region=Region.US,
        ib_symbol="MBT",
        ib_exchange="COMEX",
        ib_currency="USD",
        hold_cycle="FGHJKMNQUVXZ",  # All months
        roll_offset_days=-5
    )
    
    # Manually add to configuration (in production, you'd modify the config class)
    config._instruments["BITCOIN_CUSTOM"] = custom_instrument
    
    # Download data for custom instrument
    await manager.add_new_instrument(
        instrument_code="BITCOIN_CUSTOM",
        start_date="20220101",
        end_date="20241201"
    )
    
    print("Custom instrument added and data downloaded")

asyncio.run(add_custom_instrument())
'''

# Example 5: Data analysis and backtesting preparation
analysis_example = '''
import pandas as pd
import numpy as np
from futures_data_manager import FuturesDataManager

def analyze_futures_data():
    manager = FuturesDataManager(data_path="./data")
    
    # Get adjusted prices for portfolio
    instruments = ["SP500", "DAX", "CRUDE_W", "GOLD", "EUR"]
    
    # Collect all adjusted price series
    price_data = {}
    for instrument in instruments:
        prices = manager.get_adjusted_prices(instrument)
        if not prices.empty:
            price_data[instrument] = prices["PRICE"]
    
    # Create combined DataFrame
    portfolio_prices = pd.DataFrame(price_data)
    portfolio_prices = portfolio_prices.dropna()
    
    print(f"Portfolio data shape: {portfolio_prices.shape}")
    print(f"Date range: {portfolio_prices.index.min()} to {portfolio_prices.index.max()}")
    
    # Calculate returns
    returns = portfolio_prices.pct_change().dropna()
    
    # Basic statistics
    print("\\nAnnualized Statistics:")
    print(f"Returns (annual): {returns.mean() * 252:.2%}")
    print(f"Volatility (annual): {returns.std() * np.sqrt(252):.2%}")
    print(f"Sharpe Ratio: {(returns.mean() / returns.std()) * np.sqrt(252):.2f}")
    
    # Correlation matrix
    print("\\nCorrelation Matrix:")
    print(returns.corr().round(2))
    
    return portfolio_prices, returns

# Run analysis
prices, returns = analyze_futures_data()
'''

# Example 6: Production monitoring
monitoring_example = '''
import asyncio
from datetime import datetime, timedelta
from futures_data_manager import FuturesDataManager

class DataQualityMonitor:
    def __init__(self):
        self.manager = FuturesDataManager(data_path="./production_data")
        self.alerts = []
    
    def check_data_quality(self):
        """Check data quality across all instruments."""
        instruments = self.manager.list_available_instruments()
        
        for instrument in instruments:
            summary = self.manager.storage.get_data_summary(instrument)
            
            # Check for stale data (older than 2 days)
            if summary["last_updated"]:
                days_old = (datetime.now() - summary["last_updated"]).days
                if days_old > 2:
                    self.alerts.append(f"{instrument}: Data is {days_old} days old")
            
            # Check for missing components
            if not summary["has_adjusted_prices"]:
                self.alerts.append(f"{instrument}: Missing adjusted prices")
            
            if not summary["has_roll_calendar"]:
                self.alerts.append(f"{instrument}: Missing roll calendar")
            
            if summary["contract_count"] < 4:
                self.alerts.append(f"{instrument}: Only {summary['contract_count']} contracts")
    
    def check_roll_calendars(self):
        """Check if roll calendars need updates."""
        instruments = self.manager.list_available_instruments()
        
        for instrument in instruments:
            roll_calendar = self.manager.storage.read_roll_calendar(instrument)
            
            if not roll_calendar.empty:
                last_roll = roll_calendar.index[-1]
                days_until_last_roll = (last_roll - datetime.now()).days
                
                # Alert if last scheduled roll is soon (within 30 days)
                if days_until_last_roll < 30:
                    self.alerts.append(f"{instrument}: Last roll in {days_until_last_roll} days")
    
    def generate_report(self):
        """Generate data quality report."""
        self.alerts = []  # Reset alerts
        
        print("=== Data Quality Report ===")
        print(f"Generated: {datetime.now()}")
        
        self.check_data_quality()
        self.check_roll_calendars()
        
        if self.alerts:
            print(f"\\n⚠️  {len(self.alerts)} Issues Found:")
            for alert in self.alerts:
                print(f"  - {alert}")
        else:
            print("\\n✅ All systems operational")
        
        # Storage statistics
        stats = self.manager.storage.get_storage_stats()
        print(f"\\n📊 Storage Statistics:")
        print(f"  Total instruments: {stats['total_instruments']}")
        print(f"  Total files: {stats['total_files']}")
        print(f"  Storage used: {stats['total_size_mb']:.1f} MB")
        
        return len(self.alerts) == 0

# Usage
monitor = DataQualityMonitor()
is_healthy = monitor.generate_report()
'''

print("Basic Usage Example:")
print(basic_usage_example[:800] + "...")

print("\n" + "="*80)
print("COMPLETE FUTURES DATA MANAGER PACKAGE SUMMARY")
print("="*80)

package_summary = """
FUTURES DATA MANAGER - Complete Self-Contained Package

Based on pysystemtrade architecture, this package provides:

✅ FEATURES IMPLEMENTED:
• Interactive Brokers integration via ib_insync
• 587+ futures instruments across all major asset classes
• Automatic roll calendar generation and management
• Multiple prices (current/forward/carry) creation
• Back-adjusted continuous price series
• Efficient Parquet storage with metadata
• Comprehensive logging and error handling
• Update and maintenance capabilities
• Data validation and quality checks

📁 PACKAGE STRUCTURE:
futures_data_manager/
├── __init__.py                    # Main package exports
├── main.py                       # FuturesDataManager orchestrator
├── config/
│   ├── instruments.py            # 587+ instrument configurations
│   └── roll_config.py           # Roll parameter defaults
├── data_sources/
│   ├── base_data_source.py      # Abstract base class
│   └── interactive_brokers.py   # IB integration with ib_insync
├── data_storage/
│   ├── parquet_storage.py       # Efficient Parquet I/O
│   └── data_objects.py          # Data structures
├── roll_calendars/
│   ├── roll_calendar_generator.py # Roll date calculation
│   └── roll_parameters.py       # Roll configuration
├── price_processing/
│   ├── multiple_prices.py       # Current/forward/carry prices
│   ├── adjusted_prices.py       # Back-adjusted continuous series
│   └── contract_stitcher.py     # Price stitching methods
└── utils/
    ├── date_utils.py            # Date calculations
    └── logging_utils.py         # Logging configuration

🎯 KEY CAPABILITIES:
1. Download individual futures contract prices from Interactive Brokers
2. Generate roll calendars based on contract specifications and actual price data
3. Create multiple price series for current/forward/carry contracts
4. Generate back-adjusted continuous price series using Panama method
5. Store all data efficiently in Parquet format with proper indexing
6. Update existing datasets incrementally
7. Validate data quality and provide monitoring tools
8. Handle 587+ instruments across major global exchanges

🔧 INSTALLATION:
pip install -r requirements.txt

📈 USAGE:
from futures_data_manager import FuturesDataManager

manager = FuturesDataManager(data_path="./data")
await manager.download_and_process_instruments(["SP500", "DAX", "CRUDE_W"])

continuous_prices = manager.get_adjusted_prices("SP500")
multiple_prices = manager.get_multiple_prices("SP500")

🏗️ PRODUCTION READY:
• Robust error handling and retry logic
• Comprehensive logging with rotation
• Data validation and quality checks  
• Incremental updates for efficiency
• Memory-efficient processing
• Rate limiting for API compliance
• Automated roll calendar maintenance

This package provides everything needed to replicate pysystemtrade's
data infrastructure as a standalone, maintainable solution focused
specifically on futures data management with Interactive Brokers.
"""

print(package_summary)