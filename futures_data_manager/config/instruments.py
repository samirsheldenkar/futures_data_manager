"""
Instrument configuration module containing all futures instruments supported by the system.
Reads configuration from CSV files for maintainability and comprehensive coverage.
"""

import csv
import os
from pathlib import Path
from typing import Dict, Any, Optional, List, Set
from dataclasses import dataclass
from enum import Enum


class AssetClass(Enum):
    """Asset class enumeration."""
    EQUITY = "Equity"
    BOND = "Bond"
    FX = "FX"
    METALS = "Metals"
    OILGAS = "OilGas"
    AGS = "Ags"
    STIR = "STIR"
    SECTOR = "Sector"
    VOL = "Vol"
    HOUSING = "Housing"
    SINGLE_STOCK = "SingleStock"
    WEATHER = "Weather"
    OTHER = "Other"
    COMMODITY_INDEX = "CommodityIndex"


class Region(Enum):
    """Regional enumeration."""
    US = "US"
    EMEA = "EMEA"
    ASIA = "ASIA"


@dataclass
class InstrumentInfo:
    """Container for instrument configuration data."""
    instrument_code: str
    description: str
    pointsize: float
    currency: str
    asset_class: AssetClass
    region: Region
    per_block: float = 0.0
    percentage: float = 0.0
    per_trade: float = 0.0
    
    # Additional classification fields
    subclass: Optional[str] = None
    sub_subclass: Optional[str] = None
    style: Optional[str] = None
    country: Optional[str] = None
    duration: Optional[str] = None
    
    # Roll parameters
    hold_cycle: str = "HMUZ"
    priced_cycle: str = "HMUZ"
    roll_offset_days: int = -5
    expiry_offset: int = 0
    carry_offset: int = -1
    
    # IB contract specifications (placeholder for future use)
    ib_symbol: Optional[str] = None
    ib_exchange: Optional[str] = None
    ib_currency: Optional[str] = None
    ib_multiplier: Optional[int] = None


class InstrumentConfig:
    """
    Comprehensive instrument configuration for all futures markets.
    Reads configuration from CSV files for maintainability and comprehensive coverage.
    """
    
    def __init__(self, config_dir: Optional[str] = None):
        """
        Initialize with comprehensive instrument database from CSV files.
        
        Args:
            config_dir: Directory containing configuration CSV files. 
                       Defaults to the directory containing this file.
        """
        if config_dir is None:
            config_dir = Path(__file__).parent
        
        self.config_dir = Path(config_dir)
        self._instruments = {}
        self._roll_configs = {}
        self._additional_info = {}
        
        self._load_instrument_config()
        self._load_roll_config()
        self._load_additional_info()
    
    def _load_instrument_config(self):
        """Load basic instrument configuration from instrumentconfig.csv."""
        config_file = self.config_dir / "instrumentconfig.csv"
        
        if not config_file.exists():
            raise FileNotFoundError(f"Instrument configuration file not found: {config_file}")
        
        with open(config_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                try:
                    # Parse basic fields
                    instrument_code = row['Instrument']
                    description = row['Description']
                    pointsize = float(row['Pointsize'])
                    currency = row['Currency']
                    asset_class_str = row['AssetClass']
                    per_block = float(row['PerBlock']) if row['PerBlock'] else 0.0
                    percentage = float(row['Percentage']) if row['Percentage'] else 0.0
                    per_trade = float(row['PerTrade']) if row['PerTrade'] else 0.0
                    region_str = row['Region']
                    
                    # Map asset class string to enum
                    asset_class = self._map_asset_class(asset_class_str)
                    
                    # Map region string to enum
                    region = self._map_region(region_str)
                    
                    # Create instrument info
                    self._instruments[instrument_code] = InstrumentInfo(
                        instrument_code=instrument_code,
                        description=description,
                        pointsize=pointsize,
                        currency=currency,
                        asset_class=asset_class,
                        region=region,
                        per_block=per_block,
                        percentage=percentage,
                        per_trade=per_trade
                    )
                    
                except (ValueError, KeyError) as e:
                    print(f"Warning: Error parsing instrument {row.get('Instrument', 'Unknown')}: {e}")
                    continue
    
    def _load_roll_config(self):
        """Load roll configuration from rollconfig.csv."""
        roll_file = self.config_dir / "rollconfig.csv"
        
        if not roll_file.exists():
            print(f"Warning: Roll configuration file not found: {roll_file}")
            return
        
        with open(roll_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                try:
                    instrument_code = row['Instrument']
                    
                    if instrument_code in self._instruments:
                        # Update roll parameters
                        instrument = self._instruments[instrument_code]
                        instrument.hold_cycle = row['HoldRollCycle']
                        instrument.priced_cycle = row['PricedRollCycle']
                        instrument.roll_offset_days = int(row['RollOffsetDays'])
                        instrument.expiry_offset = int(row['ExpiryOffset'])
                        instrument.carry_offset = int(row['CarryOffset'])
                        
                        # Store roll config for quick access
                        self._roll_configs[instrument_code] = {
                            'hold_cycle': row['HoldRollCycle'],
                            'priced_cycle': row['PricedRollCycle'],
                            'roll_offset_days': int(row['RollOffsetDays']),
                            'expiry_offset': int(row['ExpiryOffset']),
                            'carry_offset': int(row['CarryOffset'])
                        }
                        
                except (ValueError, KeyError) as e:
                    print(f"Warning: Error parsing roll config for {row.get('Instrument', 'Unknown')}: {e}")
                    continue
    
    def _load_additional_info(self):
        """Load additional instrument information from moreinstrumentinfo.csv."""
        info_file = self.config_dir / "moreinstrumentinfo.csv"
        
        if not info_file.exists():
            print(f"Warning: Additional instrument info file not found: {info_file}")
            return
        
        with open(info_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                try:
                    instrument_code = row['Instrument']
                    
                    if instrument_code in self._instruments:
                        # Update additional fields
                        instrument = self._instruments[instrument_code]
                        instrument.subclass = row.get('SubClass')
                        instrument.sub_subclass = row.get('SubSubClass')
                        instrument.style = row.get('Style')
                        instrument.country = row.get('Country')
                        instrument.duration = row.get('Duration')
                        
                        # Store additional info for quick access
                        self._additional_info[instrument_code] = {
                            'subclass': row.get('SubClass'),
                            'sub_subclass': row.get('SubSubClass'),
                            'style': row.get('Style'),
                            'country': row.get('Country'),
                            'duration': row.get('Duration')
                        }
                        
                except (KeyError) as e:
                    print(f"Warning: Error parsing additional info for {row.get('Instrument', 'Unknown')}: {e}")
                    continue
    
    def _map_asset_class(self, asset_class_str: str) -> AssetClass:
        """Map asset class string to AssetClass enum."""
        mapping = {
            'Equity': AssetClass.EQUITY,
            'Bond': AssetClass.BOND,
            'FX': AssetClass.FX,
            'Metals': AssetClass.METALS,
            'OilGas': AssetClass.OILGAS,
            'Ags': AssetClass.AGS,
            'STIR': AssetClass.STIR,
            'Sector': AssetClass.SECTOR,
            'Vol': AssetClass.VOL,
            'Housing': AssetClass.HOUSING,
            'SingleStock': AssetClass.SINGLE_STOCK,
            'Weather': AssetClass.WEATHER,
            'Other': AssetClass.OTHER,
            'CommodityIndex': AssetClass.COMMODITY_INDEX
        }
        
        return mapping.get(asset_class_str, AssetClass.OTHER)
    
    def _map_region(self, region_str: str) -> Region:
        """Map region string to Region enum."""
        mapping = {
            'US': Region.US,
            'EMEA': Region.EMEA,
            'ASIA': Region.ASIA
        }
        
        return mapping.get(region_str, Region.US)
    
    def get_config(self, instrument_code: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a specific instrument."""
        instrument = self._instruments.get(instrument_code)
        if not instrument:
            return None
        
        return {
            "instrument_code": instrument.instrument_code,
            "description": instrument.description,
            "pointsize": instrument.pointsize,
            "currency": instrument.currency,
            "asset_class": instrument.asset_class.value,
            "region": instrument.region.value,
            "per_block": instrument.per_block,
            "percentage": instrument.percentage,
            "per_trade": instrument.per_trade,
            "hold_cycle": instrument.hold_cycle,
            "priced_cycle": instrument.priced_cycle,
            "roll_offset_days": instrument.roll_offset_days,
            "expiry_offset": instrument.expiry_offset,
            "carry_offset": instrument.carry_offset,
            "subclass": instrument.subclass,
            "sub_subclass": instrument.sub_subclass,
            "style": instrument.style,
            "country": instrument.country,
            "duration": instrument.duration,
            "ib_symbol": instrument.ib_symbol,
            "ib_exchange": instrument.ib_exchange,
            "ib_currency": instrument.ib_currency,
            "ib_multiplier": instrument.ib_multiplier,
        }
    
    def get_all_instruments(self) -> List[str]:
        """Get list of all available instrument codes."""
        return list(self._instruments.keys())
    
    def get_instruments_by_asset_class(self, asset_class: AssetClass) -> List[str]:
        """Get instruments filtered by asset class."""
        return [
            code for code, instrument in self._instruments.items()
            if instrument.asset_class == asset_class
        ]
    
    def get_instruments_by_region(self, region: Region) -> List[str]:
        """Get instruments filtered by region."""
        return [
            code for code, instrument in self._instruments.items()
            if instrument.region == region
        ]
    
    def get_instruments_by_currency(self, currency: str) -> List[str]:
        """Get instruments filtered by currency."""
        return [
            code for code, instrument in self._instruments.items()
            if instrument.currency == currency
        ]
    
    def get_instruments_by_subclass(self, subclass: str) -> List[str]:
        """Get instruments filtered by subclass."""
        return [
            code for code, instrument in self._instruments.items()
            if instrument.subclass == subclass
        ]
    
    def get_instruments_by_style(self, style: str) -> List[str]:
        """Get instruments filtered by style."""
        return [
            code for code, instrument in self._instruments.items()
            if instrument.style == style
        ]
    
    def get_instruments_by_country(self, country: str) -> List[str]:
        """Get instruments filtered by country."""
        return [
            code for code, instrument in self._instruments.items()
            if instrument.country == country
        ]
    
    def validate_instrument(self, instrument_code: str) -> bool:
        """Check if an instrument code is valid."""
        return instrument_code in self._instruments
    
    def get_roll_config(self, instrument_code: str) -> Optional[Dict[str, Any]]:
        """Get roll configuration for a specific instrument."""
        return self._roll_configs.get(instrument_code)
    
    def get_additional_info(self, instrument_code: str) -> Optional[Dict[str, Any]]:
        """Get additional classification information for an instrument."""
        return self._additional_info.get(instrument_code)
    
    def get_ib_contract_specs(self, instrument_code: str) -> Optional[Dict[str, Any]]:
        """Get Interactive Brokers contract specifications for an instrument."""
        instrument = self._instruments.get(instrument_code)
        if not instrument:
            return None
        
        return {
            "symbol": instrument.ib_symbol,
            "exchange": instrument.ib_exchange,
            "currency": instrument.ib_currency,
            "multiplier": instrument.ib_multiplier or instrument.pointsize,
            "secType": "FUT"
        }
    
    def get_instrument_count(self) -> int:
        """Get total number of instruments."""
        return len(self._instruments)
    
    def get_asset_class_distribution(self) -> Dict[str, int]:
        """Get distribution of instruments by asset class."""
        distribution = {}
        for instrument in self._instruments.values():
            asset_class = instrument.asset_class.value
            distribution[asset_class] = distribution.get(asset_class, 0) + 1
        return distribution
    
    def get_region_distribution(self) -> Dict[str, int]:
        """Get distribution of instruments by region."""
        distribution = {}
        for instrument in self._instruments.values():
            region = instrument.region.value
            distribution[region] = distribution.get(region, 0) + 1
        return distribution
    
    def search_instruments(self, query: str) -> List[str]:
        """Search instruments by description or code."""
        query = query.lower()
        results = []
        
        for code, instrument in self._instruments.items():
            if (query in code.lower() or 
                query in instrument.description.lower() or
                (instrument.subclass and query in instrument.subclass.lower()) or
                (instrument.country and query in instrument.country.lower())):
                results.append(code)
        
        return results


# Predefined instrument groups for easy access
def get_major_equity_indices(config: InstrumentConfig) -> List[str]:
    """Get major equity indices."""
    return [
        "SP500", "DAX", "CAC", "EUROSTX", "FTSE100", "NIKKEI", 
        "ASX", "HANG", "RUSSELL", "DOW", "NASDAQ", "FTSE100"
    ]

def get_major_bonds(config: InstrumentConfig) -> List[str]:
    """Get major government bonds."""
    return [
        "US10", "US20", "US30", "US5", "US2", "BUND", "BOBL", 
        "SHATZ", "OAT", "BTP", "JGB", "GILT"
    ]

def get_major_commodities(config: InstrumentConfig) -> List[str]:
    """Get major commodity contracts."""
    return [
        "GOLD", "SILVER", "COPPER", "CRUDE_W", "GAS_US", "BRENT",
        "CORN", "WHEAT", "SOYBEAN", "SUGAR11", "COFFEE"
    ]

def get_major_fx(config: InstrumentConfig) -> List[str]:
    """Get major currency pairs."""
    return [
        "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "NZD", "MXP"
    ]

def get_core_portfolio(config: InstrumentConfig) -> List[str]:
    """Get core portfolio instruments."""
    return (get_major_equity_indices(config) + 
            get_major_bonds(config) + 
            get_major_commodities(config) + 
            get_major_fx(config))


# Default roll parameters by asset class (fallback when CSV not available)
DEFAULT_ROLL_PARAMETERS = {
    AssetClass.EQUITY: {
        "hold_cycle": "HMUZ",
        "priced_cycle": "HMUZ", 
        "roll_offset_days": -5,
        "expiry_offset": 0,
        "carry_offset": -1
    },
    AssetClass.BOND: {
        "hold_cycle": "HMUZ",
        "priced_cycle": "HMUZ",
        "roll_offset_days": -5,
        "expiry_offset": 0,
        "carry_offset": -1
    },
    AssetClass.FX: {
        "hold_cycle": "HMUZ",
        "priced_cycle": "HMUZ",
        "roll_offset_days": -5,
        "expiry_offset": 0,
        "carry_offset": -1
    },
    AssetClass.METALS: {
        "hold_cycle": "GJMQVZ",
        "priced_cycle": "GJMQVZ",
        "roll_offset_days": -5,
        "expiry_offset": 0,
        "carry_offset": -1
    },
    AssetClass.OILGAS: {
        "hold_cycle": "FGHJKMNQUVXZ",
        "priced_cycle": "FGHJKMNQUVXZ",
        "roll_offset_days": -3,
        "expiry_offset": 0,
        "carry_offset": -1
    },
    AssetClass.AGS: {
        "hold_cycle": "HKNUZ",
        "priced_cycle": "FGHJKMNQUVXZ",
        "roll_offset_days": -5,
        "expiry_offset": 0,
        "carry_offset": -1
    },
    AssetClass.VOL: {
        "hold_cycle": "FGHJKMNQUVXZ",
        "priced_cycle": "FGHJKMNQUVXZ",
        "roll_offset_days": -30,
        "expiry_offset": 0,
        "carry_offset": -1
    },
    AssetClass.STIR: {
        "hold_cycle": "HMUZ",
        "priced_cycle": "FGHJKMNQUVXZ",
        "roll_offset_days": -1000,  # Very early roll for STIR
        "expiry_offset": 0,
        "carry_offset": -1
    }
}


def get_default_roll_parameters(asset_class: AssetClass) -> Dict[str, Any]:
    """
    Get default roll parameters for an asset class.
    
    Args:
        asset_class: Asset class enum
        
    Returns:
        Dictionary of roll parameters
    """
    return DEFAULT_ROLL_PARAMETERS.get(asset_class, DEFAULT_ROLL_PARAMETERS[AssetClass.EQUITY])


# Create a default instance for backward compatibility
DEFAULT_CONFIG = InstrumentConfig()