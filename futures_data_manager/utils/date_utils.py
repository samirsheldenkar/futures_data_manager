"""
Date utility functions for futures data processing.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional
import numpy as np


def get_business_days_between(start_date: datetime, end_date: datetime) -> int:
    """Get number of business days between two dates."""
    return len(pd.bdate_range(start=start_date, end=end_date)) - 1


def get_expiry_date(year: int, month: int, expiry_offset: int = 0) -> datetime:
    """
    Calculate futures contract expiry date.
    
    Args:
        year: Contract year
        month: Contract month
        expiry_offset: Days offset from start of month
        
    Returns:
        Contract expiry date
    """
    base_date = datetime(year, month, 1)
    return base_date + timedelta(days=expiry_offset)


def is_business_day(date: datetime) -> bool:
    """Check if a date is a business day."""
    return date.weekday() < 5


def next_business_day(date: datetime) -> datetime:
    """Get the next business day."""
    next_day = date + timedelta(days=1)
    while not is_business_day(next_day):
        next_day += timedelta(days=1)
    return next_day


def previous_business_day(date: datetime) -> datetime:
    """Get the previous business day."""
    prev_day = date - timedelta(days=1)
    while not is_business_day(prev_day):
        prev_day -= timedelta(days=1)
    return prev_day


def get_quarter_months(year: int) -> List[int]:
    """Get quarterly months (March, June, September, December)."""
    return [3, 6, 9, 12]


def get_imm_dates(year: int) -> List[datetime]:
    """Get IMM dates (3rd Friday) for quarterly months."""
    imm_dates = []
    for month in [3, 6, 9, 12]:
        # Find 3rd Friday
        first_day = datetime(year, month, 1)
        first_friday = first_day + timedelta(days=(4 - first_day.weekday()) % 7)
        third_friday = first_friday + timedelta(days=14)
        imm_dates.append(third_friday)
    return imm_dates


def parse_contract_month(contract_id: str) -> Optional[datetime]:
    """
    Parse contract ID to get contract month.
    
    Args:
        contract_id: Contract identifier (e.g., "20240315")
        
    Returns:
        Contract month as datetime or None if invalid
    """
    try:
        if len(contract_id) >= 6:
            year = int(contract_id[:4])
            month = int(contract_id[4:6])
            return datetime(year, month, 1)
    except (ValueError, IndexError):
        pass
    return None


def format_contract_month(date: datetime) -> str:
    """Format datetime as contract month string (YYYYMM00)."""
    return f"{date.year}{date.month:02d}00"


def get_roll_schedule(
    start_year: int,
    end_year: int,
    cycle: str = "HMUZ"
) -> List[str]:
    """
    Generate roll schedule for a given cycle.
    
    Args:
        start_year: Starting year
        end_year: Ending year  
        cycle: Roll cycle (e.g., "HMUZ" for quarterly)
        
    Returns:
        List of contract IDs
    """
    month_map = {
        'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
        'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
    }
    
    contracts = []
    for year in range(start_year, end_year + 1):
        for month_code in cycle:
            if month_code in month_map:
                month = month_map[month_code]
                contracts.append(f"{year}{month:02d}00")
    
    return sorted(contracts)


def get_nth_business_day(year: int, month: int, n: int) -> datetime:
    """
    Get the nth business day of a month.
    
    Args:
        year: Year
        month: Month
        n: Which business day (1 = first, -1 = last)
        
    Returns:
        The nth business day
    """
    if n > 0:
        # nth business day from start of month
        first_day = datetime(year, month, 1)
        business_days = pd.bdate_range(start=first_day, periods=n, freq='B')
        return business_days[-1].to_pydatetime()
    else:
        # nth business day from end of month
        if month == 12:
            last_day = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            last_day = datetime(year, month + 1, 1) - timedelta(days=1)
        
        business_days = pd.bdate_range(end=last_day, periods=abs(n), freq='B')
        return business_days[0].to_pydatetime()


def get_third_friday(year: int, month: int) -> datetime:
    """
    Get the third Friday of a month (IMM date).
    
    Args:
        year: Year
        month: Month
        
    Returns:
        Third Friday datetime
    """
    first_day = datetime(year, month, 1)
    # Find first Friday
    days_to_friday = (4 - first_day.weekday()) % 7
    first_friday = first_day + timedelta(days=days_to_friday)
    # Third Friday is 2 weeks later
    third_friday = first_friday + timedelta(days=14)
    return third_friday


def days_until_expiry(
    contract_id: str,
    current_date: Optional[datetime] = None,
    expiry_offset: int = 0
) -> Optional[int]:
    """
    Calculate days until contract expiry.
    
    Args:
        contract_id: Contract identifier (YYYYMM format)
        current_date: Current date (defaults to today)
        expiry_offset: Days offset from start of month for expiry
        
    Returns:
        Days until expiry or None if invalid contract ID
    """
    if current_date is None:
        current_date = datetime.now()
    
    contract_date = parse_contract_month(contract_id)
    if contract_date is None:
        return None
    
    expiry_date = contract_date + timedelta(days=expiry_offset)
    return (expiry_date - current_date).days


def is_contract_expired(
    contract_id: str,
    current_date: Optional[datetime] = None,
    expiry_offset: int = 0
) -> bool:
    """
    Check if a contract has expired.
    
    Args:
        contract_id: Contract identifier
        current_date: Current date
        expiry_offset: Days offset for expiry calculation
        
    Returns:
        True if contract has expired
    """
    days_left = days_until_expiry(contract_id, current_date, expiry_offset)
    return days_left is not None and days_left < 0


def get_active_contracts(
    contract_list: List[str],
    current_date: Optional[datetime] = None,
    min_days_to_expiry: int = 30
) -> List[str]:
    """
    Filter contracts to only active ones with sufficient time to expiry.
    
    Args:
        contract_list: List of contract identifiers
        current_date: Current date
        min_days_to_expiry: Minimum days required until expiry
        
    Returns:
        List of active contracts
    """
    if current_date is None:
        current_date = datetime.now()
    
    active = []
    for contract_id in contract_list:
        days_left = days_until_expiry(contract_id, current_date)
        if days_left is not None and days_left >= min_days_to_expiry:
            active.append(contract_id)
    
    return active


def generate_contract_series(
    start_date: datetime,
    end_date: datetime,
    cycle: str = "HMUZ",
    offset_months: int = 0
) -> List[str]:
    """
    Generate a series of contract identifiers for a date range.
    
    Args:
        start_date: Start date
        end_date: End date
        cycle: Contract cycle (e.g., "HMUZ")
        offset_months: Months to offset each contract
        
    Returns:
        List of contract identifiers covering the date range
    """
    month_map = {
        'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
        'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
    }
    
    contracts = []
    current_date = start_date
    
    while current_date <= end_date:
        # Find next contract month in cycle
        year = current_date.year
        month = current_date.month
        
        # Find next cycle month
        cycle_months = [month_map[code] for code in cycle if code in month_map]
        cycle_months.sort()
        
        next_month = None
        for cycle_month in cycle_months:
            if cycle_month >= month:
                next_month = cycle_month
                break
        
        if next_month is None:
            # Move to next year
            next_month = cycle_months[0]
            year += 1
        
        # Apply offset
        contract_month = next_month + offset_months
        contract_year = year
        
        while contract_month > 12:
            contract_month -= 12
            contract_year += 1
        
        while contract_month <= 0:
            contract_month += 12
            contract_year -= 1
        
        contract_id = f"{contract_year}{contract_month:02d}00"
        contracts.append(contract_id)
        
        # Move to next contract period
        current_date = datetime(year, next_month, 1) + timedelta(days=32)
        current_date = current_date.replace(day=1)  # First day of next month
    
    return contracts


def validate_contract_id(contract_id: str) -> bool:
    """
    Validate a contract identifier format.
    
    Args:
        contract_id: Contract identifier to validate
        
    Returns:
        True if valid format
    """
    try:
        if len(contract_id) not in [6, 8]:  # YYYYMM or YYYYMM00
            return False
        
        year = int(contract_id[:4])
        month = int(contract_id[4:6])
        
        # Basic validation
        if year < 1900 or year > 2100:
            return False
        
        if month < 1 or month > 12:
            return False
        
        return True
        
    except (ValueError, IndexError):
        return False