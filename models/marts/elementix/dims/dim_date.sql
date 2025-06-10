-- models/marts/elementix/dims/dim_date.sql
{{
  config(
    materialized = 'table'
  )
}}
-- Explicităm ca tabel, deși ar trebui să fie deja default pentru marts conform dbt_project.yml

-- Generează o secvență de date. Ajustează intervalul după nevoie.
WITH date_spine AS (
    SELECT 
        DATEADD(DAY, seq4(), '2000-01-01')::DATE AS date_day -- Începe de la 1 Ian 2000
    FROM 
        TABLE(GENERATOR(ROWCOUNT => (365*30))) -- Generează (365*30) rânduri
        -- SEQ4() aici este doar un placeholder pentru a genera o secvență de la 0
        -- valoarea efectivă a secvenței este dată de iterarea generatorului.
)
SELECT
    date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month_of_year,
    EXTRACT(DAY FROM date_day) AS day_of_month,
    EXTRACT(QUARTER FROM date_day) AS quarter_of_year,
    
    TO_CHAR(date_day, 'Month') AS month_name,
    TO_CHAR(date_day, 'Mon') AS month_name_short,
    
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week, -- 0 (Duminică) - 6 (Sâmbătă) în Snowflake
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

    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (0, 6) THEN TRUE -- Duminică (0) sau Sâmbătă (6)
        ELSE FALSE 
    END AS is_weekend
    
FROM date_spine
WHERE date_day <= CURRENT_DATE() -- Opțional: Limitează la data curentă sau o dată viitoare rezonabilă
ORDER BY date_day