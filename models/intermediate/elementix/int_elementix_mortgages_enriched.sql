-- This model serves as the central enriched source for all mortgage transactions.
-- It joins the core mortgage data with details about the lender and the primary property/borrower address.
-- This creates a wide, denormalized table that is easy to query for building final analytics models.

WITH stg_mortgages AS (
    -- Import all columns from the staging mortgage model.
    SELECT *
    FROM {{ ref('stg_elementix_mortgages') }}
),

stg_lenders AS (
    -- Import key lender attributes.
    SELECT
        lender_id,
        lender_name,
        lender_type
    FROM {{ ref('stg_elementix_lenders') }}
),

stg_borrower_addresses AS (
    -- Use the generic address staging model but alias columns to reflect their role as the "borrower's primary address".
    -- This CTE prepares address data specifically for the join with mortgages.
    SELECT
        address_id,
        full_address AS borrower_full_address,
        city AS borrower_city,
        county_name AS borrower_county,
        state_code AS borrower_state_code,
        zip_code AS borrower_zip_code,
        latitude AS borrower_latitude,
        longitude AS borrower_longitude
    FROM {{ ref('stg_elementix_addresses') }}
)

SELECT
    -- Core mortgage details from stg_mortgages
    sm.mortgage_id,
    sm.document_id,
    sm.issuance_date,
    sm.recording_date,
    sm.maturity_date,
    sm.mortgage_amount,
    sm.loan_term_months,
    
    -- Geographical details of the mortgage recording
    sm.county_id AS mortgage_county_id,
    sm.county_name AS mortgage_county_name,
    sm.county_state_code AS mortgage_county_state_code,
    sm.region_id AS mortgage_region_id,

    -- Boolean flags describing the loan type
    sm.is_amendment,
    sm.is_blanket_loan,
    sm.is_condo_loan,
    sm.is_construction_loan,
    sm.is_correction,
    sm.is_heloc,
    sm.is_interest_only,
    sm.is_leasehold,
    sm.is_open_end,
    sm.is_owner_occupied,
    sm.has_prepayment_penalty,
    sm.is_reverse_mortgage,
    sm.is_second_mortgage,
    sm.is_subordinate,
    sm.is_variable_rate,
    sm.satisfaction_id,

    -- Enriched lender details from the join
    sl.lender_id,
    sl.lender_name,
    sl.lender_type,

    -- Enriched primary borrower/property address details from the join
    sba.address_id AS borrower_address_id,
    sba.borrower_full_address,
    sba.borrower_city,
    sba.borrower_county,
    sba.borrower_state_code,
    sba.borrower_zip_code,
    sba.borrower_latitude,
    sba.borrower_longitude,

    -- Pre-calculated date parts for easier aggregation in downstream models
    DATE_TRUNC('MONTH', sm.recording_date)::DATE AS recording_month,
    DATE_TRUNC('YEAR', sm.recording_date)::DATE AS recording_year,
    
    -- Derived metrics from array columns for quick analysis
    ARRAY_SIZE(sm.property_address_ids_array) AS num_properties_in_loan,
    sm.borrower_names_array[0]::VARCHAR AS primary_borrower_name -- Get the first borrower from the array, can be NULL.

FROM stg_mortgages sm
-- Join to bring in lender information.
LEFT JOIN stg_lenders sl
    ON sm.lender_id = sl.lender_id
-- Join to bring in the primary address details. This assumes `borrower_address_id`
-- is the key for the main property being mortgaged or the borrower's primary residence.
LEFT JOIN stg_borrower_addresses sba
    ON sm.borrower_address_id = sba.address_id