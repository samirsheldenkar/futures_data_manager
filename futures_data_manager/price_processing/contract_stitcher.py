"""
Contract price stitching methods for creating continuous price series.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import pandas as pd
import numpy as np
from loguru import logger


class ContractStitcher:
    """
    Handles stitching individual contract prices into continuous series.
    Provides various methods for joining contracts at roll dates.
    """
    
    def __init__(self):
        """Initialize the contract stitcher."""
        pass
    
    def stitch_contracts(
        self,
        contract_prices: Dict[str, pd.DataFrame],
        roll_dates: pd.DataFrame,
        method: str = "panama",
        price_column: str = "CLOSE"
    ) -> pd.DataFrame:
        """
        Stitch multiple contracts into a continuous price series.
        
        Args:
            contract_prices: Dictionary mapping contract_id -> price DataFrame
            roll_dates: DataFrame with roll dates and contract mappings
            method: Stitching method ("panama", "ratio", "forward_fill")
            price_column: Column to use for prices
            
        Returns:
            Continuous price series DataFrame
        """
        if not contract_prices or roll_dates.empty:
            logger.warning("Empty contract prices or roll dates")
            return pd.DataFrame()
        
        try:
            if method == "panama":
                return self._panama_stitch_contracts(contract_prices, roll_dates, price_column)
            elif method == "ratio":
                return self._ratio_stitch_contracts(contract_prices, roll_dates, price_column)
            elif method == "forward_fill":
                return self._forward_fill_stitch(contract_prices, roll_dates, price_column)
            else:
                logger.error(f"Unknown stitching method: {method}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error stitching contracts: {e}")
            return pd.DataFrame()
    
    def _panama_stitch_contracts(
        self,
        contract_prices: Dict[str, pd.DataFrame],
        roll_dates: pd.DataFrame,
        price_column: str
    ) -> pd.DataFrame:
        """
        Panama method: Back-adjust historical prices to remove gaps at roll dates.
        """
        # Create initial continuous series by concatenating contracts
        continuous_series = self._create_base_series(contract_prices, roll_dates, price_column)
        
        if continuous_series.empty:
            return pd.DataFrame()
        
        # Find roll points where gaps occur
        roll_points = []
        for roll_date in roll_dates.index:
            if roll_date in continuous_series.index:
                roll_points.append(roll_date)
        
        # Work backwards, adjusting each segment
        adjusted_prices = continuous_series[price_column].copy()
        
        for i, roll_date in enumerate(reversed(roll_points[1:])):  # Skip last roll
            # Get prices just before and after roll
            roll_idx = continuous_series.index.get_loc(roll_date)
            
            if roll_idx > 0:
                pre_roll_price = adjusted_prices.iloc[roll_idx - 1]
                post_roll_price = adjusted_prices.iloc[roll_idx]
                
                if pd.notna(pre_roll_price) and pd.notna(post_roll_price) and post_roll_price != 0:
                    # Calculate gap
                    gap = pre_roll_price - post_roll_price
                    
                    # Apply adjustment to all prices from this roll onwards
                    adjusted_prices.iloc[roll_idx:] += gap
                    
                    logger.debug(f"Panama gap adjustment at {roll_date}: {gap:.4f}")
        
        result = pd.DataFrame({price_column: adjusted_prices})
        result = result.dropna()
        
        logger.info(f"Panama stitched {len(contract_prices)} contracts into {len(result)} continuous prices")
        return result
    
    def _ratio_stitch_contracts(
        self,
        contract_prices: Dict[str, pd.DataFrame],
        roll_dates: pd.DataFrame,
        price_column: str
    ) -> pd.DataFrame:
        """
        Ratio method: Back-adjust historical prices using multiplicative factors.
        """
        # Create initial continuous series
        continuous_series = self._create_base_series(contract_prices, roll_dates, price_column)
        
        if continuous_series.empty:
            return pd.DataFrame()
        
        # Find roll points
        roll_points = []
        for roll_date in roll_dates.index:
            if roll_date in continuous_series.index:
                roll_points.append(roll_date)
        
        # Work backwards, applying ratio adjustments
        adjusted_prices = continuous_series[price_column].copy()
        
        for roll_date in reversed(roll_points[1:]):  # Skip last roll
            roll_idx = continuous_series.index.get_loc(roll_date)
            
            if roll_idx > 0:
                pre_roll_price = adjusted_prices.iloc[roll_idx - 1]
                post_roll_price = adjusted_prices.iloc[roll_idx]
                
                if pd.notna(pre_roll_price) and pd.notna(post_roll_price) and post_roll_price != 0:
                    # Calculate ratio
                    ratio = pre_roll_price / post_roll_price
                    
                    # Apply ratio to all prices from this roll onwards
                    adjusted_prices.iloc[roll_idx:] *= ratio
                    
                    logger.debug(f"Ratio adjustment at {roll_date}: {ratio:.6f}")
        
        result = pd.DataFrame({price_column: adjusted_prices})
        result = result.dropna()
        
        logger.info(f"Ratio stitched {len(contract_prices)} contracts into {len(result)} continuous prices")
        return result
    
    def _forward_fill_stitch(
        self,
        contract_prices: Dict[str, pd.DataFrame],
        roll_dates: pd.DataFrame,
        price_column: str
    ) -> pd.DataFrame:
        """
        Forward fill method: Simply concatenate contracts without adjustment.
        This preserves actual price levels but creates gaps.
        """
        continuous_series = self._create_base_series(contract_prices, roll_dates, price_column)
        
        if not continuous_series.empty:
            logger.info(f"Forward fill stitched {len(contract_prices)} contracts into {len(continuous_series)} prices")
        
        return continuous_series
    
    def _create_base_series(
        self,
        contract_prices: Dict[str, pd.DataFrame],
        roll_dates: pd.DataFrame,
        price_column: str
    ) -> pd.DataFrame:
        """
        Create base continuous series by selecting appropriate contract for each date.
        """
        all_dates = set()
        for prices in contract_prices.values():
            all_dates.update(prices.index)
        
        if not all_dates:
            return pd.DataFrame()
        
        # Create date range
        start_date = min(all_dates)
        end_date = max(all_dates)
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # Initialize series
        continuous_prices = pd.DataFrame(index=date_range)
        continuous_prices[price_column] = np.nan
        continuous_prices["contract"] = ""
        
        # Sort roll dates
        sorted_rolls = roll_dates.sort_index()
        roll_dates_list = list(sorted_rolls.index) + [end_date + pd.Timedelta(days=1)]
        
        # Fill prices for each period
        for i in range(len(roll_dates_list) - 1):
            period_start = start_date if i == 0 else roll_dates_list[i]
            period_end = roll_dates_list[i + 1]
            
            # Get current contract for this period
            if i < len(sorted_rolls):
                current_contract = sorted_rolls.iloc[i]["current_contract"]
            else:
                # After last roll, use the next contract
                current_contract = sorted_rolls.iloc[-1]["next_contract"]
            
            if current_contract in contract_prices:
                contract_data = contract_prices[current_contract]
                
                # Fill prices for this period
                period_mask = (continuous_prices.index >= period_start) & (continuous_prices.index < period_end)
                period_dates = continuous_prices.index[period_mask]
                
                for date in period_dates:
                    if date in contract_data.index:
                        continuous_prices.loc[date, price_column] = contract_data.loc[date, price_column]
                        continuous_prices.loc[date, "contract"] = current_contract
        
        # Remove rows with no data
        continuous_prices = continuous_prices.dropna(subset=[price_column])
        
        return continuous_prices
    
    def validate_stitching_quality(
        self,
        original_contracts: Dict[str, pd.DataFrame],
        stitched_series: pd.DataFrame,
        roll_dates: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Validate the quality of contract stitching.
        
        Args:
            original_contracts: Original contract price data
            stitched_series: Stitched continuous series
            roll_dates: Roll dates used for stitching
            
        Returns:
            Dictionary with validation results
        """
        validation = {
            "is_valid": True,
            "issues": [],
            "warnings": [],
            "stats": {}
        }
        
        if stitched_series.empty:
            validation["is_valid"] = False
            validation["issues"].append("Stitched series is empty")
            return validation
        
        # Check data coverage
        original_dates = set()
        for contract_data in original_contracts.values():
            original_dates.update(contract_data.index)
        
        stitched_dates = set(stitched_series.index)
        
        coverage_ratio = len(stitched_dates) / len(original_dates) if original_dates else 0
        validation["stats"]["coverage_ratio"] = coverage_ratio
        
        if coverage_ratio < 0.8:
            validation["warnings"].append(f"Low data coverage: {coverage_ratio:.1%}")
        
        # Check for gaps around roll dates
        gap_count = 0
        for roll_date in roll_dates.index:
            if roll_date not in stitched_series.index:
                gap_count += 1
        
        validation["stats"]["roll_date_gaps"] = gap_count
        if gap_count > 0:
            validation["warnings"].append(f"Missing data at {gap_count} roll dates")
        
        # Check for extreme price changes
        if len(stitched_series) > 1:
            price_col = stitched_series.columns[0]
            returns = stitched_series[price_col].pct_change().abs()
            extreme_changes = (returns > 0.2).sum()  # >20% daily changes
            
            validation["stats"]["extreme_changes"] = extreme_changes
            if extreme_changes > len(roll_dates.index):
                validation["warnings"].append(f"High number of extreme price changes: {extreme_changes}")
        
        # Basic statistics
        validation["stats"]["total_rows"] = len(stitched_series)
        validation["stats"]["date_range"] = (
            stitched_series.index.min(),
            stitched_series.index.max()
        ) if not stitched_series.empty else None
        
        return validation
    
    def analyze_roll_quality(
        self,
        contract_prices: Dict[str, pd.DataFrame],
        roll_dates: pd.DataFrame,
        price_column: str = "CLOSE"
    ) -> pd.DataFrame:
        """
        Analyze the quality of each roll transition.
        
        Args:
            contract_prices: Contract price data
            roll_dates: Roll dates DataFrame
            price_column: Price column to analyze
            
        Returns:
            DataFrame with roll quality metrics
        """
        roll_analysis = []
        
        for roll_date, row in roll_dates.iterrows():
            current_contract = row["current_contract"]
            next_contract = row["next_contract"]
            
            analysis = {
                "roll_date": roll_date,
                "current_contract": current_contract,
                "next_contract": next_contract,
                "current_price": np.nan,
                "next_price": np.nan,
                "price_gap": np.nan,
                "gap_percentage": np.nan,
                "volume_ratio": np.nan
            }
            
            try:
                # Get prices around roll date
                if current_contract in contract_prices and next_contract in contract_prices:
                    current_data = contract_prices[current_contract]
                    next_data = contract_prices[next_contract]
                    
                    # Find prices on or near roll date
                    current_price = self._get_price_near_date(current_data, roll_date, price_column)
                    next_price = self._get_price_near_date(next_data, roll_date, price_column)
                    
                    if pd.notna(current_price) and pd.notna(next_price):
                        analysis["current_price"] = current_price
                        analysis["next_price"] = next_price
                        analysis["price_gap"] = next_price - current_price
                        analysis["gap_percentage"] = (next_price - current_price) / current_price * 100
                        
                        # Volume analysis if available
                        if "VOLUME" in current_data.columns and "VOLUME" in next_data.columns:
                            current_volume = self._get_price_near_date(current_data, roll_date, "VOLUME")
                            next_volume = self._get_price_near_date(next_data, roll_date, "VOLUME")
                            
                            if pd.notna(current_volume) and pd.notna(next_volume) and current_volume > 0:
                                analysis["volume_ratio"] = next_volume / current_volume
                
            except Exception as e:
                logger.warning(f"Error analyzing roll on {roll_date}: {e}")
            
            roll_analysis.append(analysis)
        
        return pd.DataFrame(roll_analysis)
    
    def _get_price_near_date(
        self,
        price_data: pd.DataFrame,
        target_date: pd.Timestamp,
        column: str,
        max_days: int = 3
    ) -> float:
        """Get price near a target date within max_days."""
        if target_date in price_data.index:
            return price_data.loc[target_date, column]
        
        # Look for nearest date within range
        date_diffs = abs(price_data.index - target_date)
        min_diff_idx = date_diffs.argmin()
        
        if date_diffs[min_diff_idx].days <= max_days:
            nearest_date = price_data.index[min_diff_idx]
            return price_data.loc[nearest_date, column]
        
        return np.nan