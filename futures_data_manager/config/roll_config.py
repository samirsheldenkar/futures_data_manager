"""
Roll configuration module for futures contracts.
Reads roll parameters from CSV files for maintainability and accuracy.
"""

import csv
from pathlib import Path
from typing import Dict, Any, Optional, List
from instruments import AssetClass


class RollConfigManager:
    """
    Manages roll configuration for futures contracts.
    Reads roll parameters from CSV files and provides fallback defaults.
    """
    
    def __init__(self, config_dir: Optional[str] = None):
        """
        Initialize roll configuration manager.
        
        Args:
            config_dir: Directory containing configuration CSV files.
                       Defaults to the directory containing this file.
        """
        if config_dir is None:
            config_dir = Path(__file__).parent
        
        self.config_dir = Path(config_dir)
        self._roll_configs = {}
        self._default_params = {}
        
        self._load_roll_config()
        self._setup_default_parameters()
    
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
                    
                    # Parse roll parameters
                    roll_config = {
                        'hold_cycle': row['HoldRollCycle'],
                        'priced_cycle': row['PricedRollCycle'],
                        'roll_offset_days': int(row['RollOffsetDays']),
                        'expiry_offset': int(row['ExpiryOffset']),
                        'carry_offset': int(row['CarryOffset'])
                    }
                    
                    # Validate parameters before storing
                    if self._validate_roll_parameters(roll_config):
                        self._roll_configs[instrument_code] = roll_config
                    else:
                        print(f"Warning: Invalid roll parameters for {instrument_code}: {roll_config}")
                        
                except (ValueError, KeyError) as e:
                    print(f"Warning: Error parsing roll config for {row.get('Instrument', 'Unknown')}: {e}")
                    continue
    
    def _setup_default_parameters(self):
        """Setup default roll parameters by asset class as fallback."""
        self._default_params = {
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
            },
            AssetClass.SECTOR: {
                "hold_cycle": "HMUZ",
                "priced_cycle": "HMUZ",
                "roll_offset_days": -5,
                "expiry_offset": 0,
                "carry_offset": -1
            },
            AssetClass.HOUSING: {
                "hold_cycle": "GKQX",  # Quarterly cycles for housing
                "priced_cycle": "GKQX",
                "roll_offset_days": -5,
                "expiry_offset": 0,
                "carry_offset": -1
            },
            AssetClass.SINGLE_STOCK: {
                "hold_cycle": "HMUZ",
                "priced_cycle": "HMUZ",
                "roll_offset_days": -5,
                "expiry_offset": 0,
                "carry_offset": -1
            },
            AssetClass.WEATHER: {
                "hold_cycle": "FGHJVXZ",  # Seasonal cycles
                "priced_cycle": "FGHJVXZ",
                "roll_offset_days": -5,
                "expiry_offset": 0,
                "carry_offset": -1
            },
            AssetClass.OTHER: {
                "hold_cycle": "HMUZ",
                "priced_cycle": "HMUZ",
                "roll_offset_days": -5,
                "expiry_offset": 0,
                "carry_offset": -1
            },
            AssetClass.COMMODITY_INDEX: {
                "hold_cycle": "HMUZ",
                "priced_cycle": "HMUZ",
                "roll_offset_days": -5,
                "expiry_offset": 0,
                "carry_offset": -1
            }
        }
    
    def get_roll_config(self, instrument_code: str) -> Optional[Dict[str, Any]]:
        """
        Get roll configuration for a specific instrument.
        
        Args:
            instrument_code: Instrument code
            
        Returns:
            Roll configuration dictionary or None if not found
        """
        return self._roll_configs.get(instrument_code)
    
    def get_default_roll_parameters(self, asset_class: AssetClass) -> Dict[str, Any]:
        """
        Get default roll parameters for an asset class.
        
        Args:
            asset_class: Asset class enum
            
        Returns:
            Dictionary of default roll parameters
        """
        return self._default_params.get(asset_class, self._default_params[AssetClass.EQUITY]).copy()
    
    def get_instrument_roll_parameters(self, instrument_code: str, asset_class: AssetClass) -> Dict[str, Any]:
        """
        Get roll parameters for a specific instrument.
        First tries to get from CSV, falls back to asset class defaults.
        
        Args:
            instrument_code: Instrument code
            asset_class: Asset class of the instrument
            
        Returns:
            Dictionary of roll parameters
        """
        # First try to get from CSV configuration
        csv_config = self.get_roll_config(instrument_code)
        if csv_config:
            return csv_config.copy()
        
        # Fall back to asset class defaults
        return self.get_default_roll_parameters(asset_class)
    
    def get_all_roll_configs(self) -> Dict[str, Dict[str, Any]]:
        """Get all roll configurations from CSV."""
        return self._roll_configs.copy()
    
    def get_instruments_by_roll_pattern(self, hold_cycle: str = None, 
                                      priced_cycle: str = None,
                                      roll_offset_days: int = None) -> List[str]:
        """
        Find instruments matching specific roll patterns.
        
        Args:
            hold_cycle: Hold cycle pattern to match
            priced_cycle: Priced cycle pattern to match
            roll_offset_days: Roll offset days to match
            
        Returns:
            List of instrument codes matching the criteria
        """
        matches = []
        
        for instrument_code, config in self._roll_configs.items():
            if hold_cycle and config['hold_cycle'] != hold_cycle:
                continue
            if priced_cycle and config['priced_cycle'] != priced_cycle:
                continue
            if roll_offset_days is not None and config['roll_offset_days'] != roll_offset_days:
                continue
            
            matches.append(instrument_code)
        
        return matches
    
    def get_instruments_by_roll_timing(self, min_offset: int = None, 
                                     max_offset: int = None) -> List[str]:
        """
        Find instruments within a roll offset range.
        
        Args:
            min_offset: Minimum roll offset (more negative = earlier roll)
            max_offset: Maximum roll offset (less negative = later roll)
            
        Returns:
            List of instrument codes within the range
        """
        matches = []
        
        for instrument_code, config in self._roll_configs.items():
            offset = config['roll_offset_days']
            
            if min_offset is not None and offset < min_offset:
                continue
            if max_offset is not None and offset > max_offset:
                continue
            
            matches.append(instrument_code)
        
        return matches
    
    def get_roll_statistics(self) -> Dict[str, Any]:
        """Get statistics about roll configurations."""
        if not self._roll_configs:
            return {}
        
        offsets = [config['roll_offset_days'] for config in self._roll_configs.values()]
        hold_cycles = [config['hold_cycle'] for config in self._roll_configs.values()]
        priced_cycles = [config['priced_cycle'] for config in self._roll_configs.values()]
        
        return {
            'total_instruments': len(self._roll_configs),
            'roll_offset_stats': {
                'min': min(offsets),
                'max': max(offsets),
                'mean': sum(offsets) / len(offsets),
                'most_common': max(set(offsets), key=offsets.count)
            },
            'unique_hold_cycles': len(set(hold_cycles)),
            'unique_priced_cycles': len(set(priced_cycles)),
            'common_hold_cycles': self._get_most_common(hold_cycles, 5),
            'common_priced_cycles': self._get_most_common(priced_cycles, 5)
        }
    
    def _get_most_common(self, items: List[str], n: int) -> List[tuple]:
        """Get the n most common items with their counts."""
        from collections import Counter
        counter = Counter(items)
        return counter.most_common(n)
    
    def validate_roll_parameters(self, params: Dict[str, Any]) -> bool:
        """
        Validate roll parameters.
        
        Args:
            params: Roll parameters dictionary
            
        Returns:
            True if valid
        """
        return self._validate_roll_parameters(params)
    
    def _validate_roll_parameters(self, params: Dict[str, Any]) -> bool:
        """Internal validation method."""
        required_keys = ["hold_cycle", "priced_cycle", "roll_offset_days", "expiry_offset", "carry_offset"]
        
        # Check all required keys present
        if not all(key in params for key in required_keys):
            return False
        
        # Validate cycle strings
        valid_months = "FGHJKMNQUVXZ"
        for cycle_key in ["hold_cycle", "priced_cycle"]:
            cycle = params[cycle_key]
            if not isinstance(cycle, str) or not cycle:
                return False
            for month in cycle:
                if month not in valid_months:
                    return False
        
        # Validate offsets
        if not isinstance(params["roll_offset_days"], int):
            return False
        
        if params["roll_offset_days"] > 0:
            return False  # Should be negative (roll before expiry)
        
        if abs(params["roll_offset_days"]) > 2000:
            return False  # Reasonable limit (increased for STIR instruments)
        
        if not isinstance(params["carry_offset"], int):
            return False
        
        if abs(params["carry_offset"]) > 12:
            return False  # Within a year
        
        return True
    
    def get_all_default_parameters(self) -> Dict[AssetClass, Dict[str, Any]]:
        """Get all default roll parameters by asset class."""
        return self._default_params.copy()


# Special roll parameters for specific market conditions
SPECIAL_ROLL_PARAMETERS = {
    "BACKWARDATION": {
        # When market is in backwardation, may want to roll later
        "roll_offset_days_adjustment": +2
    },
    "CONTANGO": {
        # When market is in contango, may want to roll earlier  
        "roll_offset_days_adjustment": -2
    },
    "LOW_VOLUME": {
        # When volume is low, roll earlier to avoid liquidity issues
        "roll_offset_days_adjustment": -5
    },
    "HIGH_VOLATILITY": {
        # During high volatility, may want to roll earlier
        "roll_offset_days_adjustment": -3
    }
}


def apply_market_condition_adjustments(
    base_params: Dict[str, Any],
    market_conditions: list
) -> Dict[str, Any]:
    """
    Apply market condition adjustments to roll parameters.
    
    Args:
        base_params: Base roll parameters
        market_conditions: List of market conditions to apply
        
    Returns:
        Adjusted roll parameters
    """
    adjusted_params = base_params.copy()
    
    total_adjustment = 0
    for condition in market_conditions:
        if condition in SPECIAL_ROLL_PARAMETERS:
            adjustment = SPECIAL_ROLL_PARAMETERS[condition].get("roll_offset_days_adjustment", 0)
            total_adjustment += adjustment
    
    if total_adjustment != 0:
        adjusted_params["roll_offset_days"] += total_adjustment
        
        # Ensure we don't go beyond reasonable limits
        if adjusted_params["roll_offset_days"] > -1:
            adjusted_params["roll_offset_days"] = -1
        elif adjusted_params["roll_offset_days"] < -90:
            adjusted_params["roll_offset_days"] = -90
    
    return adjusted_params


# Backward compatibility functions
def get_default_roll_parameters(asset_class: AssetClass) -> Dict[str, Any]:
    """Backward compatibility function."""
    manager = RollConfigManager()
    return manager.get_default_roll_parameters(asset_class)


def get_instrument_roll_parameters(instrument_code: str, asset_class: AssetClass) -> Dict[str, Any]:
    """Backward compatibility function."""
    manager = RollConfigManager()
    return manager.get_instrument_roll_parameters(instrument_code, asset_class)


def validate_roll_parameters(params: Dict[str, Any]) -> bool:
    """Backward compatibility function."""
    manager = RollConfigManager()
    return manager.validate_roll_parameters(params)


def get_all_default_parameters() -> Dict[AssetClass, Dict[str, Any]]:
    """Backward compatibility function."""
    manager = RollConfigManager()
    return manager.get_all_default_parameters()


# Create a default instance for easy access
DEFAULT_ROLL_MANAGER = RollConfigManager()