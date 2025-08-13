"""
Roll calendar generator for futures contracts.
Determines when to roll from one futures contract to the next based on roll parameters.
"""

from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from loguru import logger

from ..utils.date_utils import get_business_days_between, get_expiry_date


class RollCalendarGenerator:
    """
    Generates roll calendars for futures contracts based on roll parameters and price data.
    
    A roll calendar determines when to switch from one futures contract to the next,
    ensuring continuous price series and optimal liquidity.
    """
    
    def __init__(self):
        """Initialize the roll calendar generator."""
        self.month_codes = {
            1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
            7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
        }
        
        self.month_numbers = {v: k for k, v in self.month_codes.items()}
    
    def generate_from_prices(
        self,
        instrument_code: str,
        contract_prices: Dict[str, pd.DataFrame],
        roll_parameters: Dict[str, Any]
    ) -> pd.DataFrame:
        """
        Generate a roll calendar from actual contract prices and roll parameters.
        
        Args:
            instrument_code: Instrument identifier
            contract_prices: Dictionary mapping contract_id -> price DataFrame
            roll_parameters: Roll configuration parameters
            
        Returns:
            DataFrame with columns: current_contract, next_contract, carry_contract
        """
        logger.info(f"Generating roll calendar for {instrument_code}")
        
        if not contract_prices:
            logger.warning(f"No contract prices provided for {instrument_code}")
            return pd.DataFrame()
        
        try:
            # Extract roll parameters
            hold_cycle = roll_parameters.get("hold_cycle", "HMUZ")
            priced_cycle = roll_parameters.get("priced_cycle", hold_cycle)
            roll_offset_days = roll_parameters.get("roll_offset_days", -5)
            carry_offset = roll_parameters.get("carry_offset", -1)
            expiry_offset = roll_parameters.get("expiry_offset", 0)
            
            # Get sorted list of available contracts
            available_contracts = self._get_sorted_contracts(contract_prices.keys())
            
            # Filter contracts by hold cycle
            hold_contracts = self._filter_contracts_by_cycle(available_contracts, hold_cycle)
            
            if len(hold_contracts) < 2:
                logger.warning(f"Insufficient contracts for {instrument_code}: {len(hold_contracts)}")
                return pd.DataFrame()
            
            # Generate approximate roll dates
            approximate_rolls = self._generate_approximate_roll_dates(
                hold_contracts, roll_offset_days, expiry_offset
            )
            
            # Adjust roll dates to actual trading days with price data
            adjusted_rolls = self._adjust_roll_dates_to_prices(
                approximate_rolls, contract_prices, hold_contracts
            )
            
            # Generate carry contracts
            roll_calendar = self._add_carry_contracts(
                adjusted_rolls, hold_contracts, priced_cycle, carry_offset
            )
            
            # Validate the roll calendar
            roll_calendar = self._validate_roll_calendar(
                roll_calendar, contract_prices, instrument_code
            )
            
            logger.success(f"Generated {len(roll_calendar)} roll dates for {instrument_code}")
            return roll_calendar
            
        except Exception as e:
            logger.error(f"Error generating roll calendar for {instrument_code}: {e}")
            return pd.DataFrame()
    
    def _get_sorted_contracts(self, contract_ids: List[str]) -> List[str]:
        """Sort contract IDs chronologically."""
        # Convert contract IDs to dates for sorting
        contract_dates = []
        for contract_id in contract_ids:
            try:
                if len(contract_id) >= 6:
                    year = int(contract_id[:4])
                    month = int(contract_id[4:6])
                    contract_dates.append((contract_id, datetime(year, month, 1)))
            except ValueError:
                logger.warning(f"Invalid contract ID format: {contract_id}")
                continue
        
        # Sort by date
        contract_dates.sort(key=lambda x: x[1])
        return [contract_id for contract_id, _ in contract_dates]
    
    def _filter_contracts_by_cycle(
        self,
        contracts: List[str],
        cycle: str
    ) -> List[str]:
        """Filter contracts to only those in the specified cycle."""
        filtered = []
        
        for contract_id in contracts:
            try:
                month = int(contract_id[4:6])
                month_code = self.month_codes.get(month)
                
                if month_code and month_code in cycle:
                    filtered.append(contract_id)
                    
            except (ValueError, IndexError):
                continue
        
        return filtered
    
    def _generate_approximate_roll_dates(
        self,
        contracts: List[str],
        roll_offset_days: int,
        expiry_offset: int
    ) -> List[Tuple[str, str, datetime]]:
        """
        Generate approximate roll dates based on contract expiry and roll offset.
        
        Returns:
            List of tuples: (current_contract, next_contract, roll_date)
        """
        approximate_rolls = []
        
        for i in range(len(contracts) - 1):
            current_contract = contracts[i]
            next_contract = contracts[i + 1]
            
            try:
                # Calculate expiry date
                year = int(current_contract[:4])
                month = int(current_contract[4:6])
                
                # Base expiry date (start of month + expiry offset)
                expiry_date = datetime(year, month, 1) + timedelta(days=expiry_offset)
                
                # Roll date (expiry date + roll offset)
                roll_date = expiry_date + timedelta(days=roll_offset_days)
                
                approximate_rolls.append((current_contract, next_contract, roll_date))
                
            except ValueError as e:
                logger.warning(f"Error calculating roll date for {current_contract}: {e}")
                continue
        
        return approximate_rolls
    
    def _adjust_roll_dates_to_prices(
        self,
        approximate_rolls: List[Tuple[str, str, datetime]],
        contract_prices: Dict[str, pd.DataFrame],
        hold_contracts: List[str]
    ) -> List[Tuple[str, str, datetime]]:
        """
        Adjust roll dates to actual trading days where we have prices for both contracts.
        """
        adjusted_rolls = []
        
        for current_contract, next_contract, approx_roll_date in approximate_rolls:
            # Get price data for both contracts
            current_prices = contract_prices.get(current_contract)
            next_prices = contract_prices.get(next_contract)
            
            if current_prices is None or next_prices is None:
                logger.warning(f"Missing price data for roll {current_contract} -> {next_contract}")
                continue
            
            if current_prices.empty or next_prices.empty:
                logger.warning(f"Empty price data for roll {current_contract} -> {next_contract}")
                continue
            
            # Find overlapping dates
            current_dates = set(current_prices.index.date)
            next_dates = set(next_prices.index.date)
            overlapping_dates = current_dates.intersection(next_dates)
            
            if not overlapping_dates:
                logger.warning(f"No overlapping dates for roll {current_contract} -> {next_contract}")
                continue
            
            # Find the best roll date near the approximate date
            target_date = approx_roll_date.date()
            available_dates = sorted(overlapping_dates)
            
            # Find closest available date
            best_date = self._find_closest_date(target_date, available_dates)
            
            if best_date:
                roll_datetime = datetime.combine(best_date, datetime.min.time())
                adjusted_rolls.append((current_contract, next_contract, roll_datetime))
                logger.debug(f"Roll {current_contract} -> {next_contract} on {best_date}")
            else:
                logger.warning(f"No suitable roll date found for {current_contract} -> {next_contract}")
        
        return adjusted_rolls
    
    def _find_closest_date(
        self,
        target_date: datetime.date,
        available_dates: List[datetime.date],
        max_days_diff: int = 30
    ) -> Optional[datetime.date]:
        """Find the closest available date to the target date."""
        if not available_dates:
            return None
        
        # Calculate differences
        date_diffs = [
            (abs((date - target_date).days), date)
            for date in available_dates
        ]
        
        # Sort by difference
        date_diffs.sort()
        
        # Return closest date if within acceptable range
        days_diff, closest_date = date_diffs[0]
        if days_diff <= max_days_diff:
            return closest_date
        
        return None
    
    def _add_carry_contracts(
        self,
        rolls: List[Tuple[str, str, datetime]],
        hold_contracts: List[str],
        priced_cycle: str,
        carry_offset: int
    ) -> pd.DataFrame:
        """
        Add carry contracts to the roll calendar.
        
        Carry contracts are used for calculating carry trading signals.
        """
        roll_data = []
        
        for current_contract, next_contract, roll_date in rolls:
            # Determine carry contract based on carry offset
            carry_contract = self._get_carry_contract(
                current_contract, priced_cycle, carry_offset
            )
            
            roll_data.append({
                "roll_date": roll_date,
                "current_contract": current_contract,
                "next_contract": next_contract,
                "carry_contract": carry_contract or next_contract  # Fallback to next contract
            })
        
        if not roll_data:
            return pd.DataFrame()
        
        # Create DataFrame
        roll_calendar = pd.DataFrame(roll_data)
        roll_calendar = roll_calendar.set_index("roll_date")
        roll_calendar = roll_calendar.sort_index()
        
        return roll_calendar
    
    def _get_carry_contract(
        self,
        current_contract: str,
        priced_cycle: str,
        carry_offset: int
    ) -> Optional[str]:
        """
        Determine the carry contract based on the current contract and carry offset.
        
        Args:
            current_contract: Current contract ID (YYYYMM format)
            priced_cycle: Priced cycle (e.g., "HMUZ")
            carry_offset: Offset for carry contract (-1 = previous, +1 = next)
            
        Returns:
            Carry contract ID or None
        """
        try:
            year = int(current_contract[:4])
            month = int(current_contract[4:6])
            current_month_code = self.month_codes.get(month)
            
            if not current_month_code or current_month_code not in priced_cycle:
                return None
            
            # Find position in cycle
            cycle_months = list(priced_cycle)
            try:
                current_pos = cycle_months.index(current_month_code)
            except ValueError:
                return None
            
            # Calculate carry position
            carry_pos = current_pos + carry_offset
            
            # Handle year wrap-around
            carry_year = year
            if carry_pos < 0:
                carry_pos = len(cycle_months) + carry_pos
                carry_year -= 1
            elif carry_pos >= len(cycle_months):
                carry_pos = carry_pos % len(cycle_months)
                carry_year += 1
            
            # Get carry month
            carry_month_code = cycle_months[carry_pos]
            carry_month = self.month_numbers[carry_month_code]
            
            return f"{carry_year}{carry_month:02d}00"
            
        except (ValueError, IndexError, KeyError) as e:
            logger.warning(f"Error calculating carry contract for {current_contract}: {e}")
            return None
    
    def _validate_roll_calendar(
        self,
        roll_calendar: pd.DataFrame,
        contract_prices: Dict[str, pd.DataFrame],
        instrument_code: str
    ) -> pd.DataFrame:
        """
        Validate the roll calendar and remove invalid entries.
        """
        if roll_calendar.empty:
            return roll_calendar
        
        valid_rows = []
        
        for idx, row in roll_calendar.iterrows():
            current_contract = row["current_contract"]
            next_contract = row["next_contract"]
            roll_date = idx
            
            # Check that we have price data for both contracts on the roll date
            current_prices = contract_prices.get(current_contract)
            next_prices = contract_prices.get(next_contract)
            
            if current_prices is None or next_prices is None:
                logger.warning(f"Missing price data for roll on {roll_date}")
                continue
            
            # Check that both contracts have data on the roll date
            roll_date_only = roll_date.date() if hasattr(roll_date, 'date') else roll_date
            
            current_has_data = any(
                date.date() == roll_date_only for date in current_prices.index
            )
            next_has_data = any(
                date.date() == roll_date_only for date in next_prices.index
            )
            
            if not (current_has_data and next_has_data):
                logger.warning(f"Missing price data on roll date {roll_date} for {instrument_code}")
                continue
            
            valid_rows.append(row)
        
        if not valid_rows:
            logger.warning(f"No valid roll dates found for {instrument_code}")
            return pd.DataFrame()
        
        # Reconstruct DataFrame with valid rows
        valid_calendar = pd.DataFrame(valid_rows)
        valid_calendar.index = [row.name for row in valid_rows]
        
        # Check for monotonicity
        if not valid_calendar.index.is_monotonic_increasing:
            logger.warning(f"Roll calendar for {instrument_code} is not monotonic, sorting...")
            valid_calendar = valid_calendar.sort_index()
        
        logger.info(f"Validated roll calendar for {instrument_code}: {len(valid_calendar)} valid rolls")
        return valid_calendar
    
    def generate_from_existing_calendar(
        self,
        existing_calendar: pd.DataFrame,
        extend_periods: int = 4
    ) -> pd.DataFrame:
        """
        Extend an existing roll calendar by inferring the pattern.
        
        Args:
            existing_calendar: Existing roll calendar DataFrame
            extend_periods: Number of additional roll periods to generate
            
        Returns:
            Extended roll calendar
        """
        if existing_calendar.empty or len(existing_calendar) < 2:
            logger.warning("Insufficient data to extend roll calendar")
            return existing_calendar
        
        try:
            # Analyze existing pattern
            roll_dates = existing_calendar.index
            contracts = existing_calendar["current_contract"].tolist()
            
            # Calculate typical roll interval
            intervals = []
            for i in range(1, len(roll_dates)):
                interval = (roll_dates[i] - roll_dates[i-1]).days
                intervals.append(interval)
            
            avg_interval = int(np.median(intervals)) if intervals else 90  # Default 3 months
            
            # Extend the calendar
            extended_rows = []
            last_date = roll_dates[-1]
            last_current = existing_calendar["current_contract"].iloc[-1]
            last_next = existing_calendar["next_contract"].iloc[-1]
            
            for i in range(extend_periods):
                # Next roll date
                next_roll_date = last_date + timedelta(days=avg_interval * (i + 1))
                
                # Next contracts (simple increment)
                current_contract = last_next  # Previous 'next' becomes current
                next_contract = self._increment_contract(current_contract)
                carry_contract = existing_calendar["carry_contract"].iloc[-1]  # Keep same pattern
                
                extended_rows.append({
                    "current_contract": current_contract,
                    "next_contract": next_contract,
                    "carry_contract": carry_contract
                })
                
                last_next = next_contract
            
            # Create extended DataFrame
            extended_dates = [
                last_date + timedelta(days=avg_interval * (i + 1))
                for i in range(extend_periods)
            ]
            
            extended_df = pd.DataFrame(extended_rows, index=extended_dates)
            
            # Combine with existing
            combined_calendar = pd.concat([existing_calendar, extended_df])
            combined_calendar = combined_calendar.sort_index()
            
            logger.info(f"Extended roll calendar by {extend_periods} periods")
            return combined_calendar
            
        except Exception as e:
            logger.error(f"Error extending roll calendar: {e}")
            return existing_calendar
    
    def _increment_contract(self, contract_id: str) -> str:
        """Increment a contract ID to the next period."""
        try:
            year = int(contract_id[:4])
            month = int(contract_id[4:6])
            
            # Increment month
            month += 3  # Assume quarterly
            if month > 12:
                month -= 12
                year += 1
            
            return f"{year}{month:02d}00"
            
        except (ValueError, IndexError):
            return contract_id
    
    def validate_calendar_against_prices(
        self,
        roll_calendar: pd.DataFrame,
        contract_prices: Dict[str, pd.DataFrame]
    ) -> Tuple[bool, List[str]]:
        """
        Validate a roll calendar against available price data.
        
        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        issues = []
        
        if roll_calendar.empty:
            issues.append("Roll calendar is empty")
            return False, issues
        
        # Check monotonicity
        if not roll_calendar.index.is_monotonic_increasing:
            issues.append("Roll dates are not in chronological order")
        
        # Check each roll
        for roll_date, row in roll_calendar.iterrows():
            current_contract = row["current_contract"]
            next_contract = row["next_contract"]
            
            # Check contract data exists
            if current_contract not in contract_prices:
                issues.append(f"Missing price data for current contract {current_contract}")
                continue
            
            if next_contract not in contract_prices:
                issues.append(f"Missing price data for next contract {next_contract}")
                continue
            
            # Check data on roll date
            current_data = contract_prices[current_contract]
            next_data = contract_prices[next_contract]
            
            roll_date_only = roll_date.date() if hasattr(roll_date, 'date') else roll_date
            
            current_has_data = any(d.date() == roll_date_only for d in current_data.index)
            next_has_data = any(d.date() == roll_date_only for d in next_data.index)
            
            if not current_has_data:
                issues.append(f"No price data for {current_contract} on roll date {roll_date}")
            
            if not next_has_data:
                issues.append(f"No price data for {next_contract} on roll date {roll_date}")
        
        is_valid = len(issues) == 0
        return is_valid, issues