# Futures Data Manager - Complete Package Deployment Guide

## Overview

This document provides the complete deployment guide for the **Futures Data Manager** package - a self-contained Python solution for downloading and updating futures price series using Interactive Brokers, based on the pysystemtrade framework.

## Package Architecture

The package implements the complete data pipeline from pysystemtrade:

```
Raw IB Data → Individual Contracts → Roll Calendars → Multiple Prices → Adjusted Prices
```

### Core Components

1. **Interactive Brokers Integration** (`data_sources/interactive_brokers.py`)
   - Uses `ib_insync` for reliable connection to TWS/Gateway
   - Handles rate limiting and error recovery
   - Downloads OHLCV data for individual futures contracts

2. **Instrument Configuration** (`config/instruments.py`)
   - 587+ futures instruments across all major asset classes
   - Complete IB contract specifications
   - Roll parameters for each instrument

3. **Roll Calendar Generation** (`roll_calendars/roll_calendar_generator.py`)
   - Determines optimal roll dates based on parameters and actual price data
   - Handles volume analysis and liquidity considerations
   - Validates roll calendars against available price data

4. **Price Processing** (`price_processing/`)
   - **Multiple Prices**: Creates current/forward/carry price series
   - **Adjusted Prices**: Back-adjusted continuous series using Panama method
   - Handles contract stitching and gap adjustments

5. **Parquet Storage** (`data_storage/parquet_storage.py`)
   - Efficient storage using Apache Parquet format
   - Organized directory structure with metadata
   - Incremental updates and data validation

## Supported Markets

The package supports **587+ futures instruments** across:

### Asset Classes
- **Equity Indices** (150+): SP500, DAX, FTSE100, NIKKEI, Hang Seng, etc.
- **Government Bonds** (80+): US Treasuries, German Bunds, JGBs, Gilts, etc.
- **Commodities** (120+): Gold, Silver, Crude Oil, Natural Gas, Agricultural products
- **FX Futures** (60+): EUR, GBP, JPY, AUD, CAD, CHF, etc.
- **Interest Rates** (40+): Eurodollar, SOFR, SONIA, STIR products
- **Volatility** (10+): VIX, V2X, VHANG, etc.
- **Sectors** (80+): Technology, Finance, Energy, Healthcare, etc.
- **Other** (50+): Bitcoin, Emissions, Weather, Single Stocks

### Regional Coverage
- **US Markets**: CME, COMEX, NYMEX, CFE, etc.
- **European Markets**: Eurex, ICE, LIFFE, etc.
- **Asian Markets**: SGX, OSE, HKFE, etc.

## Installation

### 1. Create Package Structure

```bash
mkdir futures_data_manager
cd futures_data_manager

# Create directory structure
mkdir -p config data_sources data_storage roll_calendars price_processing utils
```

### 2. Install Dependencies

Create `requirements.txt`:
```
ib_insync>=0.9.70
pandas>=1.5.0
pyarrow>=10.0.0
numpy>=1.24.0
loguru>=0.7.0
pydantic>=2.0.0
python-dateutil>=2.8.0
pytz>=2022.1
pydantic-settings>=2.0.0
typing-extensions>=4.0.0
numba>=0.57.0
```

Install dependencies:
```bash
pip install -r requirements.txt
```

### 3. Deploy Code Files

Deploy the following files from the created modules:

1. **Package Root**:
   - `__init__.py` - Package initialization and exports
   - `main.py` - FuturesDataManager main class
   - `setup.py` - Package installation configuration
   - `requirements.txt` - Dependencies

2. **Config Module** (`config/`):
   - `__init__.py`
   - `instruments.py` - Complete instrument configuration
   - `roll_config.py` - Roll parameter defaults

3. **Data Sources** (`data_sources/`):
   - `__init__.py`
   - `base_data_source.py` - Abstract base class
   - `interactive_brokers.py` - IB integration

4. **Data Storage** (`data_storage/`):
   - `__init__.py`
   - `parquet_storage.py` - Parquet I/O operations
   - `data_objects.py` - Data structures

5. **Roll Calendars** (`roll_calendars/`):
   - `__init__.py`
   - `roll_calendar_generator.py` - Roll date calculation
   - `roll_parameters.py` - Roll configuration

6. **Price Processing** (`price_processing/`):
   - `__init__.py`
   - `multiple_prices.py` - Multiple prices processor
   - `adjusted_prices.py` - Adjusted prices processor
   - `contract_stitcher.py` - Price stitching methods

7. **Utils** (`utils/`):
   - `__init__.py`
   - `date_utils.py` - Date utility functions
   - `logging_utils.py` - Logging configuration

## Configuration

### Interactive Brokers Setup

1. **Install TWS or Gateway**
   - Download from Interactive Brokers website
   - Enable API connections in configuration
   - Set appropriate ports (7497 for paper, 7496 for live)

2. **API Configuration**
   ```python
   manager = FuturesDataManager(
       data_path="./data",
       ib_host="127.0.0.1",
       ib_port=7497,  # TWS paper trading
       ib_client_id=1
   )
   ```

### Data Directory Structure

The package will create this structure:
```
data/
├── contract_prices/          # Individual contract OHLCV
│   ├── SP500_20240315.parquet
│   ├── SP500_20240615.parquet
│   └── ...
├── multiple_prices/          # Current/Forward/Carry
│   ├── SP500_multiple.parquet
│   └── ...
├── adjusted_prices/          # Back-adjusted continuous
│   ├── SP500_adjusted.parquet
│   └── ...
├── roll_calendars/          # Roll schedules (CSV)
│   ├── SP500_roll_calendar.csv
│   └── ...
├── fx_data/                 # Spot FX rates
│   ├── EURUSD.parquet
│   └── ...
└── logs/                    # System logs
    └── futures_data_manager.log
```

## Usage Examples

### Basic Setup

```python
import asyncio
from futures_data_manager import FuturesDataManager

async def main():
    # Initialize
    manager = FuturesDataManager(data_path="./data")
    
    # Download major instruments
    instruments = ["SP500", "DAX", "CRUDE_W", "GOLD", "EUR"]
    
    await manager.download_and_process_instruments(
        instruments=instruments,
        start_date="20230101"
    )
    
    # Access processed data
    sp500_continuous = manager.get_adjusted_prices("SP500")
    sp500_multiple = manager.get_multiple_prices("SP500")

asyncio.run(main())
```

### Production Setup

```python
import asyncio
from futures_data_manager import FuturesDataManager
from futures_data_manager.config.instruments import CORE_PORTFOLIO

async def production_setup():
    manager = FuturesDataManager(
        data_path="/production/data",
        ib_host="127.0.0.1",
        ib_port=7496,  # Live trading port
        log_level="INFO"
    )
    
    # Setup complete portfolio
    await manager.download_and_process_instruments(
        instruments=CORE_PORTFOLIO,  # 100+ core instruments
        start_date="20150101",       # 9+ years of data
        update_mode=False
    )

asyncio.run(production_setup())
```

### Daily Updates

```python
import asyncio
from futures_data_manager import FuturesDataManager

async def daily_update():
    manager = FuturesDataManager(data_path="./data")
    
    # Update all existing instruments
    await manager.update_all_instruments()
    
    # Or update specific instruments
    await manager.update_instruments(["SP500", "DAX", "CRUDE_W"])

# Schedule with cron or task scheduler
asyncio.run(daily_update())
```

## Data Processing Pipeline

### 1. Individual Contract Download
- Connect to Interactive Brokers via ib_insync
- Download OHLCV data for each futures contract
- Store in Parquet format with metadata
- Handle rate limiting and error recovery

### 2. Roll Calendar Generation
- Analyze contract specifications and roll parameters
- Determine optimal roll dates based on volume and liquidity
- Create roll calendar mapping current → next → carry contracts
- Validate against actual price data availability

### 3. Multiple Prices Creation
- Combine individual contracts according to roll calendar
- Create price series for:
  - **PRICE**: Current holding contract
  - **FORWARD**: Next contract to roll to
  - **CARRY**: Contract used for carry calculation
- Maintain contract identifiers for each price series

### 4. Adjusted Prices Creation
- Apply Panama (gap-adjusted) stitching method
- Remove price discontinuities at roll dates
- Create continuous price series suitable for backtesting
- Maintain proper adjustment factors

### 5. Data Storage & Validation
- Store in efficient Parquet format
- Add comprehensive metadata
- Validate data quality and completeness
- Provide monitoring and alerting capabilities

## Performance & Scalability

### Storage Efficiency
- Parquet format provides 50-90% space savings vs CSV
- Columnar storage enables fast analytical queries
- Built-in compression with metadata preservation

### Update Performance
- Incremental updates only download new data
- Parallel processing for multiple instruments
- Memory-efficient streaming for large datasets

### API Rate Limiting
- Respects Interactive Brokers API limits
- Automatic retry with exponential backoff
- Request queuing and throttling

## Monitoring & Maintenance

### Data Quality Checks
- Validate price continuity and reasonableness
- Check for missing data and gaps
- Monitor roll calendar accuracy
- Alert on data staleness

### System Health
- Comprehensive logging with rotation
- Storage usage monitoring
- API connection health checks
- Performance metrics tracking

### Automated Maintenance
- Roll calendar extensions
- Contract expiry handling
- Data archival and cleanup
- System backup procedures

## Deployment Checklist

### Pre-deployment
- [ ] Interactive Brokers account setup with API access
- [ ] TWS/Gateway installed and configured
- [ ] Python environment with required packages
- [ ] Data storage location with sufficient space
- [ ] Network connectivity to IB servers

### Initial Setup
- [ ] Deploy package code and configuration
- [ ] Test IB connection with sample instrument
- [ ] Download initial dataset for key instruments
- [ ] Verify data quality and completeness
- [ ] Configure logging and monitoring

### Production Deployment
- [ ] Set up automated daily updates
- [ ] Configure monitoring and alerting
- [ ] Implement backup and recovery procedures
- [ ] Document operational procedures
- [ ] Train operations team on system usage

## Support & Troubleshooting

### Common Issues

1. **IB Connection Failures**
   - Check TWS/Gateway is running
   - Verify API settings are enabled
   - Ensure correct host/port configuration

2. **Missing Price Data**
   - Verify instrument configuration
   - Check contract specifications
   - Ensure sufficient historical data range

3. **Roll Calendar Issues**
   - Validate roll parameters
   - Check for data gaps around roll dates
   - Review contract availability

### Performance Tuning

1. **Batch Size Optimization**
   - Adjust concurrent download limits
   - Balance API rate limits with throughput
   - Monitor memory usage during processing

2. **Storage Optimization**
   - Choose appropriate Parquet compression
   - Partition large datasets by date/instrument
   - Regular cleanup of temporary files

This comprehensive package provides a production-ready solution for futures data management, replicating pysystemtrade's robust data infrastructure in a focused, maintainable package.