
{{ config(materialized='incremental', unique_key=['recording_month', 'property_state_code', 'property_county', 'lender_type', 'is_construction_loan', 'is_heloc', 'is_owner_occupied', 'is_variable_rate', 'is_second_mortgage'], incremental_strategy='merge') }}

WITH mortgages_enriched AS (
    SELECT *
    FROM {{ ref('int_elementix_mortgages_enriched') }}
    WHERE recording_date IS NOT NULL 
      AND mortgage_amount IS NOT NULL AND mortgage_amount > 0 
      AND borrower_state_code IS NOT NULL

    {% if is_incremental() %}
    -- If we are running incrementally, we only want to include new records
    -- that have been recorded after the last date in our existing table.
    AND recording_date > (SELECT MAX(recording_date) FROM {{ this }}) 
    -- `{{ this }}` refers to the current model being built, which is the target table.
    {% endif %}
),

dim_date AS (
    SELECT date_day FROM {{ ref('dim_date') }}
)

SELECT
    -- Primary and Foreign Keys
    me.recording_month, 
    me.borrower_state_code AS property_state_code, 
    me.borrower_county AS property_county,
    me.lender_type,
    me.is_construction_loan,
    me.is_heloc,
    me.is_owner_occupied,
    me.is_variable_rate,
    me.is_second_mortgage,
    
    -- Aggregate Metrics
    COUNT(DISTINCT me.mortgage_id) AS num_mortgages,
    SUM(me.mortgage_amount) AS total_mortgage_amount,
    AVG(me.mortgage_amount) AS avg_mortgage_amount,
    MEDIAN(me.mortgage_amount) AS median_mortgage_amount,
    AVG(me.loan_term_months) AS avg_loan_term_months,
    SUM(CASE WHEN me.is_construction_loan THEN me.mortgage_amount ELSE 0 END) AS total_construction_loan_amount,
    COUNT(DISTINCT CASE WHEN me.is_construction_loan THEN me.mortgage_id ELSE NULL END) AS num_construction_loans,
    SUM(CASE WHEN me.is_heloc THEN me.mortgage_amount ELSE 0 END) AS total_heloc_amount,
    COUNT(DISTINCT CASE WHEN me.is_heloc THEN me.mortgage_id ELSE NULL END) AS num_heloc_loans,
    SUM(CASE WHEN me.is_owner_occupied THEN me.mortgage_amount ELSE 0 END) AS total_owner_occupied_amount,
    COUNT(DISTINCT CASE WHEN me.is_owner_occupied THEN me.mortgage_id ELSE NULL END) AS num_owner_occupied_loans,
    
    -- Adding the date dimension for time-based analysis
    MAX(me.recording_date) as recording_date

FROM mortgages_enriched me
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9