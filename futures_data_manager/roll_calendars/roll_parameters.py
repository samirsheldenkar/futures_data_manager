"""
Roll parameters configuration for futures contracts.
Defines roll cycles, offsets, and other rolling parameters.
"""

from typing import Dict, Any, Optional
from dataclasses import dataclass
from futures_data_manager.config.instruments import AssetClass


@dataclass
class RollParameters:
    """Container for roll parameters."""
    
    hold_cycle: str = "HMUZ"
    priced_cycle: str = "HMUZ"
    roll_offset_days: int = -5
    expiry_offset: int = 0
    carry_offset: int = -1
    
    def __post_init__(self):
        """Validate parameters after initialization."""
        self.validate()
    
    def validate(self) -> None:
        """Validate roll parameters."""
        # Validate cycle strings
        valid_months = "FGHJKMNQUVXZ"
        
        for cycle_name, cycle in [("hold_cycle", self.hold_cycle), ("priced_cycle", self.priced_cycle)]:
            if not cycle:
                raise ValueError(f"{cycle_name} cannot be empty")
            
            for month in cycle:
                if month not in valid_months:
                    raise ValueError(f"Invalid month code '{month}' in {cycle_name}")
        
        # Validate offsets
        if self.roll_offset_days > 0:
            raise ValueError("roll_offset_days should be negative (roll before expiry)")
        
        if abs(self.roll_offset_days) > 365:
            raise ValueError("roll_offset_days magnitude should be less than 365")
        
        if abs(self.carry_offset) > 12:
            raise ValueError("carry_offset should be between -12 and 12")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "hold_cycle": self.hold_cycle,
            "priced_cycle": self.priced_cycle,
            "roll_offset_days": self.roll_offset_days,
            "expiry_offset": self.expiry_offset,
            "carry_offset": self.carry_offset
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RollParameters":
        """Create from dictionary."""
        return cls(
            hold_cycle=data.get("hold_cycle", "HMUZ"),
            priced_cycle=data.get("priced_cycle", "HMUZ"),
            roll_offset_days=data.get("roll_offset_days", -5),
            expiry_offset=data.get("expiry_offset", 0),
            carry_offset=data.get("carry_offset", -1)
        )


class DefaultRollParameters:
    """Default roll parameters by asset class."""
    
    PARAMETERS = {
        AssetClass.EQUITY: RollParameters(
            hold_cycle="HMUZ",
            priced_cycle="HMUZ", 
            roll_offset_days=-5,
            expiry_offset=0,
            carry_offset=-1
        ),
        AssetClass.BOND: RollParameters(
            hold_cycle="HMUZ",
            priced_cycle="HMUZ",
            roll_offset_days=-5,
            expiry_offset=0,
            carry_offset=-1
        ),
        AssetClass.FX: RollParameters(
            hold_cycle="HMUZ",
            priced_cycle="HMUZ",
            roll_offset_days=-5,
            expiry_offset=0,
            carry_offset=-1
        ),
        AssetClass.METALS: RollParameters(
            hold_cycle="GJMQVZ",
            priced_cycle="GJMQVZ",
            roll_offset_days=-5,
            expiry_offset=0,
            carry_offset=-1
        ),
        AssetClass.OILGAS: RollParameters(
            hold_cycle="FGHJKMNQUVXZ",
            priced_cycle="FGHJKMNQUVXZ",
            roll_offset_days=-3,
            expiry_offset=0,
            carry_offset=-1
        ),
        AssetClass.AGS: RollParameters(
            hold_cycle="HKNUZ",
            priced_cycle="FGHJKMNQUVXZ",
            roll_offset_days=-5,
            expiry_offset=0,
            carry_offset=-1
        ),
        AssetClass.VOL: RollParameters(
            hold_cycle="FGHJKMNQUVXZ",
            priced_cycle="FGHJKMNQUVXZ",
            roll_offset_days=-30,
            expiry_offset=0,
            carry_offset=-1
        ),
        AssetClass.STIR: RollParameters(
            hold_cycle="HMUZ",
            priced_cycle="FGHJKMNQUVXZ",
            roll_offset_days=-1000,  # Special handling for STIR
            expiry_offset=0,
            carry_offset=-1
        )
    }
    
    @classmethod
    def get_parameters(cls, asset_class: AssetClass) -> RollParameters:
        """Get default parameters for an asset class."""
        return cls.PARAMETERS.get(asset_class, cls.PARAMETERS[AssetClass.EQUITY])
    
    @classmethod
    def get_all_parameters(cls) -> Dict[AssetClass, RollParameters]:
        """Get all default parameters."""
        return cls.PARAMETERS.copy()


def get_roll_parameters_for_instrument(instrument_code: str, asset_class: AssetClass) -> RollParameters:
    """
    Get roll parameters for a specific instrument, with customizations for special cases.
    
    Args:
        instrument_code: Instrument identifier
        asset_class: Asset class of the instrument
        
    Returns:
        RollParameters for the instrument
    """
    # Start with default parameters for asset class
    params = DefaultRollParameters.get_parameters(asset_class)
    
    # Special cases and customizations
    customizations = {
        "EDOLLAR": RollParameters(
            hold_cycle="HMUZ",
            priced_cycle="FGHJKMNQUVXZ",
            roll_offset_days=-1000,  # Very early roll for STIR
            expiry_offset=0,
            carry_offset=-1
        ),
        "VIX": RollParameters(
            hold_cycle="FGHJKMNQUVXZ",
            priced_cycle="FGHJKMNQUVXZ", 
            roll_offset_days=-30,  # Roll 30 days before expiry
            expiry_offset=0,
            carry_offset=-1
        ),
        "V2X": RollParameters(
            hold_cycle="FGHJKMNQUVXZ",
            priced_cycle="FGHJKMNQUVXZ",
            roll_offset_days=-30,
            expiry_offset=0,
            carry_offset=-1
        ),
        # Add more customizations as needed
    }
    
    if instrument_code in customizations:
        return customizations[instrument_code]
    
    return params


def validate_roll_calendar_consistency(
    roll_parameters: RollParameters,
    contract_months: list
) -> bool:
    """
    Validate that roll parameters are consistent with available contract months.
    
    Args:
        roll_parameters: Roll parameters to validate
        contract_months: Available contract months
        
    Returns:
        True if consistent, False otherwise
    """
    # Check that hold cycle months are available in contracts
    month_codes = {"F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
                   "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12}
    
    hold_months = [month_codes[code] for code in roll_parameters.hold_cycle if code in month_codes]
    
    # Extract months from contract identifiers
    available_months = set()
    for contract in contract_months:
        try:
            if len(contract) >= 6:
                month = int(contract[4:6])
                available_months.add(month)
        except (ValueError, IndexError):
            continue
    
    # Check if all hold cycle months are available
    missing_months = set(hold_months) - available_months
    
    if missing_months:
        return False
    
    return True