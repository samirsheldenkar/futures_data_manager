"""
Adjusted prices processor for creating back-adjusted continuous price series.
"""

from typing import Dict, Optional, Any
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from loguru import logger


class AdjustedPricesProcessor:
    """
    Creates back-adjusted continuous price series from multiple prices.
    Uses Panama method (gap-adjusted) or other stitching methods.
    """
    
    def __init__(self):
        """Initialize the adjusted prices processor."""
        pass
    
    def create_from_multiple_prices(
        self,
        multiple_prices: pd.DataFrame,
        method: str = "panama",
        price_column: str = "PRICE"
    ) -> pd.DataFrame:
        """
        Create back-adjusted continuous price series from multiple prices.
        
        Args:
            multiple_prices: Multiple prices DataFrame
            method: Stitching method ("panama", "ratio", "difference")
            price_column: Column to use for price data
            
        Returns:
            DataFrame with continuous adjusted price series
        """
        if multiple_prices.empty:
            logger.warning("Empty multiple prices DataFrame")
            return pd.DataFrame()
        
        try:
            if method == "panama":
                return self._panama_stitch(multiple_prices, price_column)
            elif method == "ratio":
                return self._ratio_stitch(multiple_prices, price_column)
            elif method == "difference":
                return self._difference_stitch(multiple_prices, price_column)
            else:
                logger.error(f"Unknown stitching method: {method}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error creating adjusted prices: {e}")
            return pd.DataFrame()
    
    def _panama_stitch(
        self,
        multiple_prices: pd.DataFrame,
        price_column: str
    ) -> pd.DataFrame:
        """
        Panama (gap-adjusted) stitching method.
        Removes price gaps at roll dates by adjusting historical prices.
        """
        if price_column not in multiple_prices.columns:
            logger.error(f"Price column {price_column} not found")
            return pd.DataFrame()
        
        # Get prices and contract identifiers
        prices = multiple_prices[price_column].copy()
        contracts = multiple_prices[f"{price_column}_CONTRACT"].copy()
        
        # Find roll dates (where contract changes)
        roll_points = contracts.ne(contracts.shift()).fillna(False)
        roll_dates = multiple_prices.index[roll_points]
        
        if len(roll_dates) <= 1:
            # No rolls, return original prices
            adjusted_prices = pd.DataFrame({"PRICE": prices})
            logger.info("No roll adjustments needed")
            return adjusted_prices
        
        # Start with original prices
        adjusted_prices = prices.copy()
        
        # Work backwards from the end, adjusting each roll
        roll_dates_sorted = sorted(roll_dates, reverse=True)
        
        for roll_date in roll_dates_sorted:
            if roll_date == roll_dates_sorted[-1]:  # Skip first roll (earliest date)
                continue
            
            # Get prices just before and after roll
            roll_idx = multiple_prices.index.get_loc(roll_date)
            
            if roll_idx > 0:
                pre_roll_price = adjusted_prices.iloc[roll_idx - 1]
                post_roll_price = adjusted_prices.iloc[roll_idx]
                
                if pd.notna(pre_roll_price) and pd.notna(post_roll_price) and post_roll_price != 0:
                    # Calculate adjustment factor
                    adjustment_factor = pre_roll_price - post_roll_price
                    
                    # Apply adjustment to all prices from roll date onwards
                    adjusted_prices.iloc[roll_idx:] += adjustment_factor
                    
                    logger.debug(f"Panama adjustment at {roll_date}: {adjustment_factor:.4f}")
        
        # Create result DataFrame
        result = pd.DataFrame({"PRICE": adjusted_prices})
        result = result.dropna()
        
        logger.success(f"Created Panama-adjusted prices with {len(result)} points")
        return result
    
    def _ratio_stitch(
        self,
        multiple_prices: pd.DataFrame,
        price_column: str
    ) -> pd.DataFrame:
        """
        Ratio-based stitching method.
        Adjusts historical prices by ratio at each roll.
        """
        prices = multiple_prices[price_column].copy()
        contracts = multiple_prices[f"{price_column}_CONTRACT"].copy()
        
        # Find roll points
        roll_points = contracts.ne(contracts.shift()).fillna(False)
        roll_dates = multiple_prices.index[roll_points]
        
        if len(roll_dates) <= 1:
            return pd.DataFrame({"PRICE": prices})
        
        adjusted_prices = prices.copy()
        roll_dates_sorted = sorted(roll_dates, reverse=True)
        
        for roll_date in roll_dates_sorted:
            if roll_date == roll_dates_sorted[-1]:
                continue
            
            roll_idx = multiple_prices.index.get_loc(roll_date)
            
            if roll_idx > 0:
                pre_roll_price = adjusted_prices.iloc[roll_idx - 1]
                post_roll_price = adjusted_prices.iloc[roll_idx]
                
                if pd.notna(pre_roll_price) and pd.notna(post_roll_price) and post_roll_price != 0:
                    ratio = pre_roll_price / post_roll_price
                    adjusted_prices.iloc[roll_idx:] *= ratio
                    logger.debug(f"Ratio adjustment at {roll_date}: {ratio:.6f}")
        
        result = pd.DataFrame({"PRICE": adjusted_prices})
        result = result.dropna()
        return result
    
    def _difference_stitch(
        self,
        multiple_prices: pd.DataFrame,
        price_column: str
    ) -> pd.DataFrame:
        """
        Difference-based stitching method.
        Same as Panama method (additive adjustment).
        """
        return self._panama_stitch(multiple_prices, price_column)
    
    def update_adjusted_prices(
        self,
        existing_adjusted_prices: pd.DataFrame,
        updated_multiple_prices: pd.DataFrame,
        method: str = "panama"
    ) -> pd.DataFrame:
        """
        Update existing adjusted prices with new multiple prices data.
        
        Args:
            existing_adjusted_prices: Existing adjusted prices
            updated_multiple_prices: Updated multiple prices
            method: Stitching method
            
        Returns:
            Updated adjusted prices
        """
        if existing_adjusted_prices.empty:
            return self.create_from_multiple_prices(updated_multiple_prices, method)
        
        if updated_multiple_prices.empty:
            return existing_adjusted_prices
        
        try:
            # Find overlap point
            last_existing_date = existing_adjusted_prices.index[-1]
            
            # Check if we have new data
            new_data_mask = updated_multiple_prices.index > last_existing_date
            if not new_data_mask.any():
                logger.info("No new data to add to adjusted prices")
                return existing_adjusted_prices
            
            # Create full adjusted series
            full_adjusted = self.create_from_multiple_prices(updated_multiple_prices, method)
            
            if full_adjusted.empty:
                logger.warning("Failed to create updated adjusted prices")
                return existing_adjusted_prices
            
            # Splice: keep existing data up to cutoff, add new data after
            cutoff_date = last_existing_date
            existing_part = existing_adjusted_prices.loc[:cutoff_date]
            
            # Get new part and adjust to match existing level
            new_part = full_adjusted.loc[full_adjusted.index > cutoff_date]
            
            if not new_part.empty and not existing_part.empty:
                # Adjust level of new data to match existing
                last_existing_price = existing_part["PRICE"].iloc[-1]
                first_new_price = new_part["PRICE"].iloc[0]
                
                if pd.notna(last_existing_price) and pd.notna(first_new_price) and first_new_price != 0:
                    level_adjustment = last_existing_price - first_new_price
                    new_part["PRICE"] += level_adjustment
                    logger.debug(f"Level adjustment for new data: {level_adjustment:.4f}")
                
                # Combine
                combined = pd.concat([existing_part, new_part])
                combined = combined.sort_index()
                
                logger.info(f"Updated adjusted prices: {len(existing_part)} existing + {len(new_part)} new")
                return combined
            else:
                return existing_adjusted_prices
                
        except Exception as e:
            logger.error(f"Error updating adjusted prices: {e}")
            return existing_adjusted_prices
    
    def validate_adjusted_prices(self, adjusted_prices: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate adjusted prices data quality.
        
        Args:
            adjusted_prices: Adjusted prices DataFrame
            
        Returns:
            Dictionary with validation results
        """
        validation = {
            "is_valid": True,
            "issues": [],
            "warnings": [],
            "stats": {}
        }
        
        if adjusted_prices.empty:
            validation["is_valid"] = False
            validation["issues"].append("Adjusted prices DataFrame is empty")
            return validation
        
        # Check required columns
        if "PRICE" not in adjusted_prices.columns:
            validation["is_valid"] = False
            validation["issues"].append("Missing PRICE column")
            return validation
        
        # Check for missing price data
        missing_count = adjusted_prices["PRICE"].isna().sum()
        if missing_count > 0:
            pct_missing = (missing_count / len(adjusted_prices)) * 100
            if pct_missing > 5:  # More than 5% missing
                validation["warnings"].append(f"PRICE has {pct_missing:.1f}% missing values")
        
        # Check for negative prices
        negative_count = (adjusted_prices["PRICE"] < 0).sum()
        if negative_count > 0:
            validation["warnings"].append(f"Found {negative_count} negative price values")
        
        # Check for price continuity
        price_changes = adjusted_prices["PRICE"].pct_change().abs()
        large_changes = (price_changes > 0.2).sum()  # More than 20% daily change
        if large_changes > 0:
            validation["warnings"].append(f"Found {large_changes} large daily price changes (>20%)")
        
        # Basic statistics
        validation["stats"]["total_rows"] = len(adjusted_prices)
        validation["stats"]["date_range"] = (
            adjusted_prices.index.min(),
            adjusted_prices.index.max()
        )
        validation["stats"]["price_range"] = (
            adjusted_prices["PRICE"].min(),
            adjusted_prices["PRICE"].max()
        )
        validation["stats"]["mean_price"] = adjusted_prices["PRICE"].mean()
        validation["stats"]["std_price"] = adjusted_prices["PRICE"].std()
        
        return validation
    
    def calculate_returns(self, adjusted_prices: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate returns from adjusted prices.
        
        Args:
            adjusted_prices: Adjusted prices DataFrame
            
        Returns:
            DataFrame with returns
        """
        if adjusted_prices.empty or "PRICE" not in adjusted_prices.columns:
            return pd.DataFrame()
        
        returns_df = pd.DataFrame(index=adjusted_prices.index)
        
        # Simple returns
        returns_df["RETURNS"] = adjusted_prices["PRICE"].pct_change()
        
        # Log returns
        returns_df["LOG_RETURNS"] = np.log(adjusted_prices["PRICE"] / adjusted_prices["PRICE"].shift(1))
        
        # Cumulative returns
        returns_df["CUM_RETURNS"] = (1 + returns_df["RETURNS"]).cumprod() - 1
        
        # Remove first row (NaN)
        returns_df = returns_df.dropna()
        
        return returns_df