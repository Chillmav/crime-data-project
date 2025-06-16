import pandas as pd

def is_us_weekend_or_holiday(date_str):

    date = pd.to_datetime(date_str)
    
    # Check if it's a weekend (Saturday=5, Sunday=6)
    if date.weekday() >= 5:
        return True

    year = date.year

    # Fixed-date holidays
    fixed_holidays = pd.to_datetime([
        f'{year}-01-01',  # New Year's Day
        f'{year}-07-04',  # Independence Day
        f'{year}-11-11',  # Veterans Day
        f'{year}-12-25',  # Christmas Day
    ])

    # Adjust for holidays that fall on a weekend
    observed = fixed_holidays.copy()
    observed = observed.where(fixed_holidays.weekday != 5, fixed_holidays - pd.Timedelta(days=1))  # Saturday → Friday
    observed = observed.where(fixed_holidays.weekday != 6, fixed_holidays + pd.Timedelta(days=1))  # Sunday → Monday
    fixed_and_observed = pd.concat([fixed_holidays.to_series(), observed.to_series()]).drop_duplicates()

    # Variable holidays (e.g., MLK Day, Presidents' Day, etc.)
    def nth_weekday(year, month, weekday, n):
        """Get the nth weekday (0=Mon,...6=Sun) of a given month and year. Negative n counts from end."""
        first = pd.Timestamp(year=year, month=month, day=1)
        if n > 0:
            offset = (weekday - first.weekday() + 7) % 7
            return first + pd.Timedelta(days=offset + 7*(n-1))
        else:
            last = (first + pd.offsets.MonthEnd(1)).normalize()
            offset = (last.weekday() - weekday + 7) % 7
            return last - pd.Timedelta(days=offset + 7*(-n-1))

    variable_holidays = pd.to_datetime([
        nth_weekday(year, 1, 0, 3),   # Martin Luther King Jr. Day (3rd Monday in January)
        nth_weekday(year, 2, 0, 3),   # Presidents' Day (3rd Monday in February)
        nth_weekday(year, 5, 0, -1),  # Memorial Day (last Monday in May)
        nth_weekday(year, 9, 0, 1),   # Labor Day (1st Monday in September)
        nth_weekday(year, 10, 0, 2),  # Columbus Day (2nd Monday in October)
        nth_weekday(year, 11, 3, 4),  # Thanksgiving Day (4th Thursday in November)
    ]).to_series()

    all_holidays = pd.concat([fixed_and_observed, variable_holidays]).drop_duplicates()

    return date.normalize() in all_holidays.dt.normalize().values
