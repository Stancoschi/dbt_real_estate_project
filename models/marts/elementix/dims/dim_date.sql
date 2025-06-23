-- This model generates a standard date dimension table.
-- It creates a continuous sequence of dates and enriches them with various
-- date-part attributes (e.g., year, month, day of week) to support time-based analysis.

{{ config(materialized='table') }}
-- Explicitly materializing as a table is a best practice for dimension models,
-- even if it's the default, to ensure performance and clarity.

-- The date_spine CTE generates a sequence of dates.
-- The range can be adjusted by changing the start date and the ROWCOUNT.
WITH date_spine AS (
    SELECT 
        -- Generate dates starting from January 1, 2000
        DATEADD(DAY, seq4(), '2000-01-01')::DATE AS date_day
    FROM 
        -- Generate enough rows for 30 years of data.
        TABLE(GENERATOR(ROWCOUNT => (365*30)))
)

SELECT
    date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month_of_year,
    EXTRACT(DAY FROM date_day) AS day_of_month,
    EXTRACT(QUARTER FROM date_day) AS quarter_of_year,
    
    TO_CHAR(date_day, 'Month') AS month_name,
    TO_CHAR(date_day, 'Mon') AS month_name_short,
    
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week, -- Snowflake: 0 = Sunday, 6 = Saturday
    TO_CHAR(date_day, 'Day') AS day_name,
    TO_CHAR(date_day, 'Dy') AS day_name_short,
    
    EXTRACT(WEEKISO FROM date_day) AS week_of_year_iso,
    EXTRACT(DAYOFYEAR FROM date_day) AS day_of_year,
    
    DATE_TRUNC('MONTH', date_day)::DATE AS first_day_of_month,
    LAST_DAY(date_day, 'MONTH')::DATE AS last_day_of_month,
    DATE_TRUNC('QUARTER', date_day)::DATE AS first_day_of_quarter,
    LAST_DAY(date_day, 'QUARTER')::DATE AS last_day_of_quarter,
    DATE_TRUNC('YEAR', date_day)::DATE AS first_day_of_year,
    LAST_DAY(date_day, 'YEAR')::DATE AS last_day_of_year,

    (day_of_week IN (0, 6)) AS is_weekend -- Boolean flag for weekend days
    
FROM date_spine
-- Filter to ensure the dimension does not extend beyond the current date.
WHERE date_day <= CURRENT_DATE()
ORDER BY date_day