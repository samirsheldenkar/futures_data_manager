"""
Multiple prices processor for creating current/forward/carry price series.
"""

from typing import Dict, Optional, Any
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from loguru import logger


class MultiplePricesProcessor:
    """
    Creates multiple price series containing:
    - PRICE: Current contract price (the contract we're holding)
    - FORWARD: Forward contract price (the next contract we'll roll to)  
    - CARRY: Carry contract price (used for carry trading signals)
    - Contract identifiers for each price series
    """
    
    def __init__(self):
        """Initialize the multiple prices processor."""
        pass
    
    def create_from_contract_prices(
        self,
        contract_prices: Dict[str, pd.DataFrame],
        roll_calendar: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Create multiple prices from individual contract prices and roll calendar.
        
        Args:
            contract_prices: Dictionary mapping contract_id -> price DataFrame
            roll_calendar: Roll calendar with roll dates and contract specifications
            
        Returns:
            DataFrame with multiple price series
        """
        if not contract_prices or roll_calendar.empty:
            logger.warning("Empty contract prices or roll calendar")
            return pd.DataFrame()
        
        try:
            # Get date range for multiple prices
            all_dates = set()
            for prices in contract_prices.values():
                if not prices.empty:
                    all_dates.update(prices.index)
            
            if not all_dates:
                logger.warning("No price data available")
                return pd.DataFrame()
            
            # Create date range
            start_date = min(all_dates)
            end_date = max(all_dates)
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            
            # Initialize result DataFrame
            multiple_prices = pd.DataFrame(index=date_range)
            multiple_prices["PRICE"] = np.nan
            multiple_prices["FORWARD"] = np.nan
            multiple_prices["CARRY"] = np.nan
            multiple_prices["PRICE_CONTRACT"] = ""
            multiple_prices["FORWARD_CONTRACT"] = ""
            multiple_prices["CARRY_CONTRACT"] = ""
            
            # Sort roll calendar
            roll_calendar = roll_calendar.sort_index()
            roll_dates = list(roll_calendar.index) + [end_date + timedelta(days=1)]
            
            # Process each roll period
            for i in range(len(roll_dates) - 1):
                period_start = roll_dates[i] if i == 0 else roll_dates[i]
                period_end = roll_dates[i + 1]
                
                # Get contracts for this period
                if i < len(roll_calendar):
                    row = roll_calendar.iloc[i]
                    current_contract = row["current_contract"]
                    forward_contract = row["next_contract"]
                    carry_contract = row.get("carry_contract", forward_contract)
                else:
                    # Last period - use previous period's forward contract as current
                    if i > 0:
                        prev_row = roll_calendar.iloc[i-1]
                        current_contract = prev_row["next_contract"]
                        forward_contract = current_contract  # No forward after last roll
                        carry_contract = prev_row.get("carry_contract", current_contract)
                    else:
                        continue
                
                logger.debug(f"Period {period_start} to {period_end}: {current_contract} -> {forward_contract}")
                
                # Fill prices for this period
                period_mask = (multiple_prices.index >= period_start) & (multiple_prices.index < period_end)
                
                # Fill current contract prices
                if current_contract in contract_prices:
                    current_prices = contract_prices[current_contract]
                    self._fill_prices(
                        multiple_prices, period_mask, current_prices, 
                        "PRICE", current_contract, "PRICE_CONTRACT"
                    )
                
                # Fill forward contract prices
                if forward_contract in contract_prices and forward_contract != current_contract:
                    forward_prices = contract_prices[forward_contract]
                    self._fill_prices(
                        multiple_prices, period_mask, forward_prices,
                        "FORWARD", forward_contract, "FORWARD_CONTRACT"
                    )
                else:
                    # Use current contract as forward if no separate forward contract
                    multiple_prices.loc[period_mask, "FORWARD"] = multiple_prices.loc[period_mask, "PRICE"]
                    multiple_prices.loc[period_mask, "FORWARD_CONTRACT"] = current_contract
                
                # Fill carry contract prices
                if carry_contract in contract_prices:
                    carry_prices = contract_prices[carry_contract]
                    self._fill_prices(
                        multiple_prices, period_mask, carry_prices,
                        "CARRY", carry_contract, "CARRY_CONTRACT"
                    )
                else:
                    # Fallback to current contract
                    multiple_prices.loc[period_mask, "CARRY"] = multiple_prices.loc[period_mask, "PRICE"]
                    multiple_prices.loc[period_mask, "CARRY_CONTRACT"] = current_contract
            
            # Remove rows with no price data
            multiple_prices = multiple_prices.dropna(subset=["PRICE"])
            
            # Forward fill missing forward and carry prices
            multiple_prices["FORWARD"] = multiple_prices["FORWARD"].fillna(method="ffill")
            multiple_prices["CARRY"] = multiple_prices["CARRY"].fillna(method="ffill")
            
            # Forward fill contract identifiers
            multiple_prices["PRICE_CONTRACT"] = multiple_prices["PRICE_CONTRACT"].replace("", np.nan).fillna(method="ffill")
            multiple_prices["FORWARD_CONTRACT"] = multiple_prices["FORWARD_CONTRACT"].replace("", np.nan).fillna(method="ffill")
            multiple_prices["CARRY_CONTRACT"] = multiple_prices["CARRY_CONTRACT"].replace("", np.nan).fillna(method="ffill")
            
            logger.success(f"Created multiple prices with {len(multiple_prices)} rows")
            return multiple_prices
            
        except Exception as e:
            logger.error(f"Error creating multiple prices: {e}")
            return pd.DataFrame()
    
    def _fill_prices(
        self,
        multiple_prices: pd.DataFrame,
        mask: pd.Series,
        source_prices: pd.DataFrame,
        price_col: str,
        contract_id: str,
        contract_col: str
    ) -> None:
        """Fill prices for a specific period and contract."""
        if source_prices.empty:
            return
        
        # Get overlapping dates
        period_dates = multiple_prices.index[mask]
        
        for date in period_dates:
            # Look for exact date match first
            if date in source_prices.index:
                price = source_prices.loc[date, "CLOSE"] if "CLOSE" in source_prices.columns else source_prices.iloc[source_prices.index.get_loc(date), 0]
                multiple_prices.loc[date, price_col] = price
                multiple_prices.loc[date, contract_col] = contract_id
            else:
                # Look for nearest date
                nearest_date = self._find_nearest_date(date, source_prices.index)
                if nearest_date is not None:
                    price = source_prices.loc[nearest_date, "CLOSE"] if "CLOSE" in source_prices.columns else source_prices.iloc[source_prices.index.get_loc(nearest_date), 0]
                    multiple_prices.loc[date, price_col] = price
                    multiple_prices.loc[date, contract_col] = contract_id
    
    def _find_nearest_date(
        self,
        target_date: pd.Timestamp,
        available_dates: pd.DatetimeIndex,
        max_days: int = 7
    ) -> Optional[pd.Timestamp]:
        """Find the nearest available date within max_days."""
        if available_dates.empty:
            return None
        
        # Calculate time differences
        time_diffs = abs(available_dates - target_date)
        min_diff_idx = time_diffs.argmin()
        nearest_date = available_dates[min_diff_idx]
        
        # Check if within acceptable range
        if time_diffs[min_diff_idx].days <= max_days:
            return nearest_date
        
        return None
    
    def update_multiple_prices(
        self,
        existing_multiple_prices: pd.DataFrame,
        new_contract_prices: Dict[str, pd.DataFrame],
        roll_calendar: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Update existing multiple prices with new contract price data.
        
        Args:
            existing_multiple_prices: Existing multiple prices DataFrame
            new_contract_prices: New contract price data
            roll_calendar: Updated roll calendar
            
        Returns:
            Updated multiple prices DataFrame
        """
        if existing_multiple_prices.empty:
            return self.create_from_contract_prices(new_contract_prices, roll_calendar)
        
        try:
            # Determine update range
            last_existing_date = existing_multiple_prices.index[-1]
            
            # Get all contract prices (existing + new)
            all_contract_prices = new_contract_prices.copy()
            
            # Create updated multiple prices
            updated_multiple_prices = self.create_from_contract_prices(
                all_contract_prices, roll_calendar
            )
            
            if updated_multiple_prices.empty:
                logger.warning("Failed to create updated multiple prices")
                return existing_multiple_prices
            
            # Splice new data onto existing
            cutoff_date = last_existing_date
            existing_part = existing_multiple_prices.loc[:cutoff_date]
            new_part = updated_multiple_prices.loc[cutoff_date:]
            
            # Remove overlap
            if not new_part.empty and cutoff_date in new_part.index:
                new_part = new_part.loc[new_part.index > cutoff_date]
            
            # Combine
            if not new_part.empty:
                combined = pd.concat([existing_part, new_part])
                combined = combined.sort_index()
                logger.info(f"Updated multiple prices: {len(existing_part)} existing + {len(new_part)} new = {len(combined)} total")
                return combined
            else:
                return existing_multiple_prices
                
        except Exception as e:
            logger.error(f"Error updating multiple prices: {e}")
            return existing_multiple_prices
    
    def validate_multiple_prices(self, multiple_prices: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate multiple prices data quality.
        
        Returns:
            Dictionary with validation results
        """
        validation = {
            "is_valid": True,
            "issues": [],
            "warnings": [],
            "stats": {}
        }
        
        if multiple_prices.empty:
            validation["is_valid"] = False
            validation["issues"].append("Multiple prices DataFrame is empty")
            return validation
        
        # Check required columns
        required_cols = ["PRICE", "FORWARD", "CARRY", "PRICE_CONTRACT", "FORWARD_CONTRACT", "CARRY_CONTRACT"]
        missing_cols = [col for col in required_cols if col not in multiple_prices.columns]
        
        if missing_cols:
            validation["is_valid"] = False
            validation["issues"].append(f"Missing columns: {missing_cols}")
        
        # Check for missing price data
        price_cols = ["PRICE", "FORWARD", "CARRY"]
        for col in price_cols:
            if col in multiple_prices.columns:
                missing_count = multiple_prices[col].isna().sum()
                if missing_count > 0:
                    pct_missing = (missing_count / len(multiple_prices)) * 100
                    if pct_missing > 10:  # More than 10% missing
                        validation["warnings"].append(f"{col} has {pct_missing:.1f}% missing values")
        
        # Check contract continuity
        for contract_col in ["PRICE_CONTRACT", "FORWARD_CONTRACT", "CARRY_CONTRACT"]:
            if contract_col in multiple_prices.columns:
                changes = multiple_prices[contract_col].ne(multiple_prices[contract_col].shift())
                change_count = changes.sum()
                validation["stats"][f"{contract_col}_changes"] = change_count
        
        # Basic statistics
        validation["stats"]["total_rows"] = len(multiple_prices)
        validation["stats"]["date_range"] = (
            multiple_prices.index.min(),
            multiple_prices.index.max()
        )
        
        return validation