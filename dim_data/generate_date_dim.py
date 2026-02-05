import pandas as pd
from datetime import datetime, timedelta

# FINAL date range (LOCKED)
start_date = datetime(2026, 1, 1)
end_date = datetime(2026, 12, 31)

dates = []
current_date = start_date

while current_date <= end_date:
    date_id = int(current_date.strftime('%Y%m%d'))
    day = current_date.day
    week = int(current_date.strftime('%U'))  # week of year
    month = current_date.month
    year = current_date.year

    # Quarter calculation
    if month in [1, 2, 3]:
        quarter = "Q1"
    elif month in [4, 5, 6]:
        quarter = "Q2"
    elif month in [7, 8, 9]:
        quarter = "Q3"
    else:
        quarter = "Q4"

    is_weekend = "Yes" if current_date.weekday() >= 5 else "No"

    dates.append([
        date_id,
        current_date.date(),
        day,
        week,
        month,
        quarter,
        year,
        is_weekend
    ])

    current_date += timedelta(days=1)

# Create DataFrame
dim_date = pd.DataFrame(
    dates,
    columns=[
        "date_id",
        "date",
        "day",
        "week",
        "month",
        "quarter",
        "year",
        "is_weekend"
    ]
)

# Save to CSV (or convert to Parquet later)
dim_date.to_csv("dim_date.csv", index=False)

print(dim_date.head())
print(dim_date.tail())
