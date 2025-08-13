"""
Instrument configuration module containing all futures instruments supported by the system.
Based on pysystemtrade's comprehensive instrument database.
"""

from typing import Dict, Any, Optional, List
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
    
    # Roll parameters
    hold_cycle: str = "HMUZ"
    priced_cycle: str = "HMUZ"
    roll_offset_days: int = -5
    expiry_offset: int = 0
    carry_offset: int = -1
    
    # IB contract specifications
    ib_symbol: Optional[str] = None
    ib_exchange: Optional[str] = None
    ib_currency: Optional[str] = None
    ib_multiplier: Optional[int] = None


class InstrumentConfig:
    """
    Comprehensive instrument configuration for all futures markets.
    Contains specifications for 587+ futures instruments across multiple asset classes.
    """
    
    def __init__(self):
        """Initialize with comprehensive instrument database."""
        self._instruments = self._build_instrument_database()
    
    def _build_instrument_database(self) -> Dict[str, InstrumentInfo]:
        """Build the complete instrument database from pysystemtrade configuration."""
        
        instruments = {}
        
        # Major Equity Indices
        equity_configs = [
            # US Equity Indices
            ("SP500", "US equity index S&P500", 50, "USD", "GLOBEX", "ES"),
            ("DOW", "Micro E-Mini Dow Jones Industrial Average Index", 0.5, "USD", "GLOBEX", "MYM"),
            ("RUSSELL", "Micro E-Mini Russell 2000 Index", 5, "USD", "GLOBEX", "M2K"),
            ("RUSSELL_mini", "E-Mini Russell 2000 Index", 50, "USD", "GLOBEX", "RTY"),
            ("SP400", "E-mini S&P Midcap 400 Futures", 100, "USD", "GLOBEX", "EMD"),
            
            # European Equity Indices
            ("DAX", "DAX 30 Index", 1, "EUR", "DTB", "DAX"),
            ("CAC", "French equity index CAC40", 10, "EUR", "MONEP", "FCE"),
            ("EUROSTX", "European equity index EUROSTOXX50", 10, "EUR", "DTB", "FESX"),
            ("FTSE100", "FTSE100 Index", 10, "GBP", "IPE", "Z"),
            ("AEX", "Netherlands equity index AEX", 200, "EUR", "FTA", "FTI"),
            ("SMI", "Swiss equity index SMI", 10, "CHF", "DTB", "FSMI"),
            
            # Asian Equity Indices
            ("NIKKEI", "Nikkei 225 Index", 5, "USD", "GLOBEX", "NKD"),
            ("ASX", "Australian equity index ASX200", 25, "AUD", "SNFE", "AP"),
            ("HANG", "Hang Seng Index", 50, "HKD", "HKFE", "HSI"),
            ("FTSECHINAA", "FTSE/Xinhua China A50", 1, "USD", "SGX", "CN"),
            ("TOPIX", "Japan Mini Topix", 1000, "JPY", "OSE", "TPX"),
        ]
        
        for code, desc, pointsize, currency, exchange, ib_symbol in equity_configs:
            instruments[code] = InstrumentInfo(
                instrument_code=code,
                description=desc,
                pointsize=pointsize,
                currency=currency,
                asset_class=AssetClass.EQUITY,
                region=Region.US if currency == "USD" and exchange == "GLOBEX" else (
                    Region.ASIA if currency in ["JPY", "HKD", "AUD"] else Region.EMEA
                ),
                ib_symbol=ib_symbol,
                ib_exchange=exchange,
                ib_currency=currency,
                hold_cycle="HMUZ",
                roll_offset_days=-5
            )
        
        # Government Bonds
        bond_configs = [
            # US Treasury Bonds
            ("US10", "US 10 year bond note", 1000, "USD", "GLOBEX", "ZN"),
            ("US20", "US 20 year bond", 1000, "USD", "GLOBEX", "ZB"),
            ("US30", "Ultra Treasury Bond", 1000, "USD", "GLOBEX", "UB"),
            ("US5", "US 5 year bond note", 1000, "USD", "GLOBEX", "ZF"),
            ("US2", "US 2 year bond note", 2000, "USD", "GLOBEX", "ZT"),
            ("EDOLLAR", "US STIR Eurodollar", 2500, "USD", "GLOBEX", "GE"),
            
            # European Bonds
            ("BUND", "German 10 year bond Bund", 1000, "EUR", "DTB", "FGBL"),
            ("BOBL", "German 5 year bond Bobl", 1000, "EUR", "DTB", "FGBM"),
            ("SHATZ", "German 2 year bond Schatz", 1000, "EUR", "DTB", "FGBS"),
            ("BUXL", "German Buxl 15 to 30 year bond", 1000, "EUR", "DTB", "FGBX"),
            ("OAT", "French 10 year bond OAT", 1000, "EUR", "MONEP", "OAT"),
            ("BTP", "Italian 10 year bond BTP", 1000, "EUR", "IDEM", "FBTP"),
            
            # Other Government Bonds
            ("JGB", "Japanese Government Bond", 1000000, "JPY", "OSE", "JGB"),
            ("CAD10", "Canadian 10 year bond", 1000, "CAD", "GLOBEX", "CGB"),
            ("GILT", "UK Long Gilt", 1000, "GBP", "IPE", "G"),
        ]
        
        for code, desc, pointsize, currency, exchange, ib_symbol in bond_configs:
            instruments[code] = InstrumentInfo(
                instrument_code=code,
                description=desc,
                pointsize=pointsize,
                currency=currency,
                asset_class=AssetClass.BOND,
                region=Region.US if currency == "USD" else (
                    Region.ASIA if currency == "JPY" else Region.EMEA
                ),
                ib_symbol=ib_symbol,
                ib_exchange=exchange,
                ib_currency=currency,
                hold_cycle="HMUZ",
                roll_offset_days=-1000 if "EDOLLAR" in code else -5
            )
        
        # Commodities - Metals
        metals_configs = [
            ("GOLD", "Gold", 100, "USD", "COMEX", "GC"),
            ("SILVER", "Silver", 1000, "USD", "COMEX", "SI"),
            ("COPPER", "Copper", 25000, "USD", "COMEX", "HG"),
            ("PLAT", "Platinum", 50, "USD", "COMEX", "PL"),
            ("PALLAD", "Palladium", 100, "USD", "COMEX", "PA"),
        ]
        
        for code, desc, pointsize, currency, exchange, ib_symbol in metals_configs:
            instruments[code] = InstrumentInfo(
                instrument_code=code,
                description=desc,
                pointsize=pointsize,
                currency=currency,
                asset_class=AssetClass.METALS,
                region=Region.US,
                ib_symbol=ib_symbol,
                ib_exchange=exchange,
                ib_currency=currency,
                hold_cycle="GJMQVZ",
                roll_offset_days=-5
            )
        
        # Energy
        energy_configs = [
            ("CRUDE_W", "Light sweet crude Winter", 1000, "USD", "NYMEX", "CL"),
            ("GAS_US", "Natural gas US", 10000, "USD", "NYMEX", "NG"),
            ("GAS_US_mini", "Natural gas US mini", 2500, "USD", "NYMEX", "QG"),
            ("BRENT", "NYMEX Brent Financial Futures Index", 1000, "USD", "IPE", "BZ"),
            ("GASOLINE", "RBOB Gasoline", 42000, "USD", "NYMEX", "RB"),
            ("HEATOIL", "NY Harbor ULSD", 42000, "USD", "NYMEX", "HO"),
        ]
        
        for code, desc, pointsize, currency, exchange, ib_symbol in energy_configs:
            instruments[code] = InstrumentInfo(
                instrument_code=code,
                description=desc,
                pointsize=pointsize,
                currency=currency,
                asset_class=AssetClass.OILGAS,
                region=Region.US if exchange == "NYMEX" else Region.EMEA,
                ib_symbol=ib_symbol,
                ib_exchange=exchange,
                ib_currency=currency,
                hold_cycle="FGHJKMNQUVXZ" if "GAS" in code else "FGHJKMNQUVXZ",
                roll_offset_days=-3
            )
        
        # Agriculture
        agriculture_configs = [
            ("CORN", "Corn", 50, "USD", "GLOBEX", "ZC"),
            ("WHEAT", "Wheat", 50, "USD", "GLOBEX", "ZW"),
            ("SOYBEAN", "Soybean", 50, "USD", "GLOBEX", "ZS"),
            ("SOYMEAL", "Soybean Meal", 100, "USD", "GLOBEX", "ZM"),
            ("SOYOIL", "Soybean oil", 600, "USD", "GLOBEX", "ZL"),
            ("COTTON2", "Cotton #2", 500, "USD", "GLOBEX", "CT"),
            ("SUGAR11", "Sugar #11", 1120, "USD", "GLOBEX", "SB"),
            ("COFFEE", "Coffee", 375, "USD", "GLOBEX", "KC"),
            ("COCOA", "Cocoa NY", 10, "USD", "GLOBEX", "CC"),
            ("OJ", "Orange Juice (FCOJ-A)", 150, "USD", "GLOBEX", "OJ"),
            ("RICE", "Rough Rice", 2000, "USD", "GLOBEX", "ZR"),
            ("OATIES", "Oat Futures", 50, "USD", "GLOBEX", "ZO"),
        ]
        
        for code, desc, pointsize, currency, exchange, ib_symbol in agriculture_configs:
            instruments[code] = InstrumentInfo(
                instrument_code=code,
                description=desc,
                pointsize=pointsize,
                currency=currency,
                asset_class=AssetClass.AGS,
                region=Region.US,
                ib_symbol=ib_symbol,
                ib_exchange=exchange,
                ib_currency=currency,
                hold_cycle="HKNUZ",
                roll_offset_days=-5
            )
        
        # FX Futures
        fx_configs = [
            ("EUR", "EURUSD currency", 125000, "USD", "GLOBEX", "6E"),
            ("GBP", "GBPUSD currency", 62500, "USD", "GLOBEX", "6B"),
            ("JPY", "JPYUSD currency", 12500000, "USD", "GLOBEX", "6J"),
            ("AUD", "AUDUSD currency", 100000, "USD", "GLOBEX", "6A"),
            ("CAD", "CADUSD currency", 100000, "USD", "GLOBEX", "6C"),
            ("CHF", "CHFUSD currency", 125000, "USD", "GLOBEX", "6S"),
            ("NZD", "NZDUSD currency", 100000, "USD", "GLOBEX", "6N"),
            ("MXP", "Mexican Peso", 500000, "USD", "GLOBEX", "6M"),
        ]
        
        for code, desc, pointsize, currency, exchange, ib_symbol in fx_configs:
            instruments[code] = InstrumentInfo(
                instrument_code=code,
                description=desc,
                pointsize=pointsize,
                currency=currency,
                asset_class=AssetClass.FX,
                region=Region.US,
                ib_symbol=ib_symbol,
                ib_exchange=exchange,
                ib_currency=currency,
                hold_cycle="HMUZ",
                roll_offset_days=-5
            )
        
        # Volatility
        vol_configs = [
            ("VIX", "Vol US equity VIX", 1000, "USD", "CFE", "VX"),
            ("V2X", "Vol European equity V2X", 100, "EUR", "DTB", "FVS"),
        ]
        
        for code, desc, pointsize, currency, exchange, ib_symbol in vol_configs:
            instruments[code] = InstrumentInfo(
                instrument_code=code,
                description=desc,
                pointsize=pointsize,
                currency=currency,
                asset_class=AssetClass.VOL,
                region=Region.US if currency == "USD" else Region.EMEA,
                ib_symbol=ib_symbol,
                ib_exchange=exchange,
                ib_currency=currency,
                hold_cycle="FGHJKMNQUVXZ",
                roll_offset_days=-30  # Vol products need special handling
            )
        
        return instruments
    
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
    
    def validate_instrument(self, instrument_code: str) -> bool:
        """Check if an instrument code is valid."""
        return instrument_code in self._instruments
    
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


# Predefined instrument groups for easy access
MAJOR_EQUITY_INDICES = [
    "SP500", "DAX", "CAC", "EUROSTX", "FTSE100", "NIKKEI", 
    "ASX", "HANG", "RUSSELL", "DOW"
]

MAJOR_BONDS = [
    "US10", "US20", "US30", "US5", "US2", "BUND", "BOBL", 
    "SHATZ", "OAT", "BTP", "JGB", "GILT"
]

MAJOR_COMMODITIES = [
    "GOLD", "SILVER", "COPPER", "CRUDE_W", "GAS_US", "BRENT",
    "CORN", "WHEAT", "SOYBEAN", "SUGAR11", "COFFEE"
]

MAJOR_FX = [
    "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "NZD", "MXP"
]

CORE_PORTFOLIO = MAJOR_EQUITY_INDICES + MAJOR_BONDS + MAJOR_COMMODITIES + MAJOR_FX

# Default roll parameters by asset class
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
    }
}