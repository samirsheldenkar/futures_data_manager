"""
Default roll configuration parameters for different asset classes.
"""

from typing import Dict, Any
from .instruments import AssetClass


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


def get_instrument_roll_parameters(instrument_code: str, asset_class: AssetClass) -> Dict[str, Any]:
    """
    Get roll parameters for a specific instrument with custom overrides.
    
    Args:
        instrument_code: Instrument code
        asset_class: Asset class of the instrument
        
    Returns:
        Dictionary of roll parameters
    """
    # Start with default parameters for asset class
    params = get_default_roll_parameters(asset_class).copy()
    
    # Instrument-specific overrides
    instrument_overrides = {
        # STIR instruments need special handling
        "EDOLLAR": {
            "roll_offset_days": -1000,  # Roll way before expiry
            "hold_cycle": "HMUZ",
            "priced_cycle": "FGHJKMNQUVXZ"
        },
        
        # Volatility instruments
        "VIX": {
            "roll_offset_days": -30,  # Roll 30 days before expiry
            "hold_cycle": "FGHJKMNQUVXZ",
            "priced_cycle": "FGHJKMNQUVXZ"
        },
        "V2X": {
            "roll_offset_days": -30,
            "hold_cycle": "FGHJKMNQUVXZ", 
            "priced_cycle": "FGHJKMNQUVXZ"
        },
        
        # Energy contracts with special roll timing
        "CRUDE_W": {
            "roll_offset_days": -3  # Roll just before expiry due to physical delivery
        },
        "GAS_US": {
            "roll_offset_days": -3
        },
        "BRENT": {
            "roll_offset_days": -3
        },
        
        # Agricultural contracts
        "CORN": {
            "hold_cycle": "HKNUZ",  # Only harvest months for holding
            "priced_cycle": "FGHJKMNQUVXZ"  # All months for pricing
        },
        "WHEAT": {
            "hold_cycle": "HKNUZ",
            "priced_cycle": "FGHJKMNQUVXZ"
        },
        "SOYBEAN": {
            "hold_cycle": "FHKNQX",  # Soybean-specific cycle
            "priced_cycle": "FGHJKMNQUVXZ"
        },
        
        # Bond futures with quarterly rolls
        "US10": {
            "hold_cycle": "HMUZ",
            "roll_offset_days": -7  # Roll a week before
        },
        "BUND": {
            "hold_cycle": "HMUZ", 
            "roll_offset_days": -7
        },
        
        # Metal contracts
        "GOLD": {
            "hold_cycle": "GJMQVZ",  # Even months
            "roll_offset_days": -5
        },
        "SILVER": {
            "hold_cycle": "HKNUZ",  # March, May, July, September, December
            "roll_offset_days": -5
        }
    }
    
    # Apply instrument-specific overrides
    if instrument_code in instrument_overrides:
        params.update(instrument_overrides[instrument_code])
    
    return params


def validate_roll_parameters(params: Dict[str, Any]) -> bool:
    """
    Validate roll parameters.
    
    Args:
        params: Roll parameters dictionary
        
    Returns:
        True if valid
    """
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
    
    if abs(params["roll_offset_days"]) > 1000:
        return False  # Reasonable limit
    
    if not isinstance(params["carry_offset"], int):
        return False
    
    if abs(params["carry_offset"]) > 12:
        return False  # Within a year
    
    return True


def get_all_default_parameters() -> Dict[AssetClass, Dict[str, Any]]:
    """Get all default roll parameters by asset class."""
    return DEFAULT_ROLL_PARAMETERS.copy()


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