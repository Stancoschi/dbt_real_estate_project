-- This model creates the primary fact table for mortgage activity.
-- It aggregates key metrics like mortgage count and total amount on a monthly basis,
-- grouped by geography, lender type, and key loan characteristics.
-- It is materialized as an incremental table for efficient daily updates.

{{ config(
    materialized='incremental',
    unique_key=['recording_month', 'property_state_code', 'property_county', 'lender_type', 'is_construction_loan', 'is_heloc', 'is_owner_occupied', 'is_variable_rate', 'is_second_mortgage'],
    incremental_strategy='merge'
) }}

WITH mortgages_enriched AS (
    -- Select from the central enriched model, applying basic filters for data quality.
    SELECT 
        *
    FROM {{ ref('int_elementix_mortgages_enriched') }}
    WHERE recording_date IS NOT NULL 
      AND mortgage_amount IS NOT NULL AND mortgage_amount > 0 
      AND borrower_state_code IS NOT NULL

    {% if is_incremental() %}
    -- This block executes only on incremental runs. It filters the source data to process
    -- only new or recently arrived records, making the daily run much faster.
    -- We use a 3-day lookback window to catch any late-arriving data.
    AND recording_date > (SELECT DATEADD(day, -3, MAX(max_recording_date)) FROM {{ this }})
    {% endif %}
),

aggregated_data AS (
    -- Group the data by dimensional attributes to calculate monthly metrics.
    SELECT
        -- Dimensional Keys
        me.recording_month, 
        me.borrower_state_code AS property_state_code, 
        me.borrower_county AS property_county,
        me.lender_type,
        
        -- Key Loan Flags for segmentation
        me.is_construction_loan,
        me.is_heloc,
        me.is_owner_occupied,
        me.is_variable_rate,
        me.is_second_mortgage,
        
        -- Aggregated Metrics
        COUNT(DISTINCT me.mortgage_id) AS num_mortgages,
        SUM(me.mortgage_amount) AS total_mortgage_amount,
        AVG(me.mortgage_amount) AS avg_mortgage_amount,
        MEDIAN(me.mortgage_amount) AS median_mortgage_amount,
        AVG(me.loan_term_months) AS avg_loan_term_months,
        
        -- Metrics for specific loan types
        SUM(CASE WHEN me.is_construction_loan THEN me.mortgage_amount ELSE 0 END) AS total_construction_loan_amount,
        COUNT(DISTINCT CASE WHEN me.is_construction_loan THEN me.mortgage_id ELSE NULL END) AS num_construction_loans,
        SUM(CASE WHEN me.is_heloc THEN me.mortgage_amount ELSE 0 END) AS total_heloc_amount,
        COUNT(DISTINCT CASE WHEN me.is_heloc THEN me.mortgage_id ELSE NULL END) AS num_heloc_loans,
        SUM(CASE WHEN me.is_owner_occupied THEN me.mortgage_amount ELSE 0 END) AS total_owner_occupied_amount,
        COUNT(DISTINCT CASE WHEN me.is_owner_occupied THEN me.mortgage_id ELSE NULL END) AS num_owner_occupied_loans,
        
        -- This column is crucial for the incremental logic. It stores the latest recording date
        -- in this batch, which will be used in the next run to determine the starting point for new data.
        MAX(me.recording_date) as max_recording_date

    FROM mortgages_enriched me
    GROUP BY
        me.recording_month,
        me.borrower_state_code,
        me.borrower_county,
        me.lender_type,
        me.is_construction_loan,
        me.is_heloc,
        me.is_owner_occupied,
        me.is_variable_rate,
        me.is_second_mortgage
)

SELECT * FROM aggregated_data