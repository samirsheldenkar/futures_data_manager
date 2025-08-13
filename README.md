# Futures Data Manager

Samir Sheldenkar

2025-08-13

A self-contained Python package for downloading and updating futures price series using Interactive Brokers, based on [Rob Carver's pysystemtrade](https://github.com/robcarver17/pysystemtrade) framework. This package handles the full set of markets used in pysystemtrade, identifies roll calendars, downloads individual futures contract price data, and creates contiguous adjusted futures price series stored in Parquet files.

## Features

- **Complete Market Coverage**: Supports 587+ futures instruments across multiple asset classes (Equities, Bonds, Commodities, FX, etc.)
- **Interactive Brokers Integration**: Uses ib_insync for reliable data download from IBKR
- **Roll Calendar Management**: Automatically identifies and manages futures contract roll dates
- **Multiple Price Series**: Creates current, forward, and carry contract price series
- **Back-Adjusted Prices**: Generates continuous price series for backtesting
- **Parquet Storage**: Efficient data storage using Apache Parquet format
- **Update Capability**: Can both generate new files and update existing datasets
- **Production Ready**: Based on proven pysystemtrade architecture

## Installation

```bash
pip install -r requirements.txt
```

## Dependencies

- ib_insync >= 0.9.70
- pandas >= 1.5.0
- pyarrow >= 10.0.0
- numpy >= 1.24.0
- loguru >= 0.7.0
- pydantic >= 2.0.0

## Quick Start

```python
from futures_data_manager import FuturesDataManager

# Initialize the data manager
manager = FuturesDataManager(
    data_path="./data",
    ib_host="127.0.0.1",
    ib_port=7497,  # TWS paper trading port
    ib_client_id=1
)

# Download data for specific instruments
instruments = ["SP500", "DAX", "CRUDE_W", "GAS_US", "GOLD"]
manager.download_and_process_instruments(instruments)

# Update existing data
manager.update_all_instruments()
```

## Configuration

### Instrument Configuration

The package includes comprehensive instrument configuration covering:

- **Equity Indices**: SP500, DAX, FTSE100, NIKKEI, etc.
- **Bonds**: US Treasuries, German Bunds, JGBs, etc.
- **Commodities**: Crude Oil, Natural Gas, Gold, Silver, Agricultural products
- **FX Futures**: Major currency pairs
- **Volatility**: VIX, V2X, etc.

### Roll Parameters

Each instrument has customized roll parameters:
- Roll offset days (when to roll before expiry)
- Expiry offset (contract expiry timing)
- Carry offset (which contract to use for carry calculation)
- Hold and priced cycles (which months are traded/held)

## Data Structure

```
data/
├── contract_prices/          # Individual futures contract OHLCV data
│   ├── SP500_20240315.parquet
│   ├── SP500_20240615.parquet
│   └── ...
├── multiple_prices/          # Current/Forward/Carry price series
│   ├── SP500_multiple.parquet
│   ├── DAX_multiple.parquet
│   └── ...
├── adjusted_prices/          # Back-adjusted continuous series
│   ├── SP500_adjusted.parquet
│   ├── DAX_adjusted.parquet
│   └── ...
├── roll_calendars/          # Roll date schedules (CSV format)
│   ├── SP500_roll_calendar.csv
│   ├── DAX_roll_calendar.csv
│   └── ...
└── fx_data/                 # Spot FX rates for currency conversion
    ├── EURUSD.parquet
    ├── GBPUSD.parquet
    └── ...
```

## Main Components

### 1. Interactive Brokers Data Source

```python
from futures_data_manager.data_sources import IBDataSource

ib_source = IBDataSource(host="127.0.0.1", port=7497, client_id=1)
historical_data = ib_source.get_historical_data("SP500", "20240101", "20241201")
```

### 2. Roll Calendar Generation

```python
from futures_data_manager.roll_calendars import RollCalendarGenerator

generator = RollCalendarGenerator()
roll_calendar = generator.generate_from_prices("SP500", contract_prices_dict)
```

### 3. Price Processing

```python
from futures_data_manager.price_processing import MultiplePrice, AdjustedPrice

# Create multiple prices (current/forward/carry)
multiple_processor = MultiplePrice()
multiple_prices = multiple_processor.create_from_contracts(
    contract_prices, roll_calendar
)

# Create back-adjusted continuous series
adjusted_processor = AdjustedPrice()
adjusted_prices = adjusted_processor.create_from_multiple(multiple_prices)
```

### 4. Data Storage

```python
from futures_data_manager.data_storage import ParquetStorage

storage = ParquetStorage("./data")

# Store individual contract data
storage.write_contract_prices("SP500", "20240315", ohlcv_data)

# Store processed price series
storage.write_multiple_prices("SP500", multiple_prices)
storage.write_adjusted_prices("SP500", adjusted_prices)
```

## Usage Examples

### Download Complete Dataset for New Instrument

```python
manager = FuturesDataManager(data_path="./data")

# Add new instrument with full historical data
manager.add_new_instrument(
    instrument_code="BITCOIN",
    start_date="20210101",
    end_date="20241201"
)
```

### Daily Updates

```python
# Update all existing instruments with latest data
manager.update_all_instruments()

# Update specific instruments
manager.update_instruments(["SP500", "DAX", "CRUDE_W"])
```

### Custom Roll Calendar

```python
# Generate roll calendar from actual price data
roll_calendar = manager.generate_roll_calendar(
    instrument_code="CUSTOM_FUTURE",
    roll_offset_days=-5,
    carry_offset=-1
)

# Manually edit if needed
roll_calendar.add_roll("20241215", "20241200", "20250300", "20250300")
manager.save_roll_calendar("CUSTOM_FUTURE", roll_calendar)
```

### Backtesting Integration

```python
# Get continuous price series for backtesting
adjusted_prices = manager.get_adjusted_prices("SP500")
multiple_prices = manager.get_multiple_prices("SP500")  # For carry trading

# Convert to returns for analysis
returns = adjusted_prices.pct_change().dropna()
```

## Data Quality & Validation

- Automatic data validation and cleaning
- Missing data detection and interpolation options
- Roll date verification against actual contract prices
- Volume-based roll timing validation
- Price spike detection and handling

## Monitoring & Logging

- Comprehensive logging using loguru
- Data quality metrics tracking
- Update status monitoring
- Error reporting and recovery

## Performance

- Parallel downloading for multiple instruments
- Efficient Parquet storage for fast I/O
- Incremental updates to minimize data transfer
- Memory-efficient processing for large datasets

## Supported Asset Classes

| Asset Class | Count | Examples |
|-------------|-------|----------|
| Equity Indices | 150+ | SP500, DAX, FTSE100, NIKKEI, etc. |
| Government Bonds | 80+ | US Treasuries, Bunds, JGBs, etc. |
| Commodities | 120+ | Crude Oil, Gold, Natural Gas, Corn, etc. |
| FX Futures | 60+ | EUR, GBP, JPY, AUD, CAD, etc. |
| Interest Rate | 40+ | Eurodollar, SOFR, SONIA, etc. |
| Volatility | 10+ | VIX, V2X, VHANG, etc. |
| Sectors | 80+ | Technology, Finance, Energy, etc. |
| Other | 50+ | Bitcoin, Emissions, Weather, etc. |

## API Reference

### FuturesDataManager

Main class for managing futures data operations.

```python
class FuturesDataManager:
    def __init__(self, data_path: str, ib_host: str = "127.0.0.1", 
                 ib_port: int = 7497, ib_client_id: int = 1)
    
    def download_and_process_instruments(self, instruments: List[str]) -> None
    def update_all_instruments(self) -> None
    def update_instruments(self, instruments: List[str]) -> None
    def add_new_instrument(self, instrument_code: str, start_date: str, 
                          end_date: str = None) -> None
    def get_adjusted_prices(self, instrument_code: str) -> pd.DataFrame
    def get_multiple_prices(self, instrument_code: str) -> pd.DataFrame
    def generate_roll_calendar(self, instrument_code: str) -> RollCalendar
```

## Error Handling

- Robust error handling for IB connection issues
- Automatic retry logic for failed downloads
- Data validation and correction procedures
- Comprehensive error logging and reporting

## License

MIT License - Based on pysystemtrade (GNU GPL v3)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## Support

For issues related to:
- Interactive Brokers connection: Check IB TWS/Gateway setup
- Data quality: Review instrument configuration
- Performance: Adjust batch sizes and parallel settings
- Roll calendars: Validate roll parameters for your instruments

## Disclaimer

This software is for educational and research purposes. Trading futures involves substantial risk. Past performance does not guarantee future results. Use at your own risk.