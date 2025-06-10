-- models/intermediate/elementix/int_elementix_mortgages_enriched.sql
WITH stg_mortgages AS (
    SELECT *
    FROM {{ ref('stg_elementix_mortgages') }}
),

stg_lenders AS (
    SELECT
        lender_id,
        lender_name,
        lender_type
    FROM {{ ref('stg_elementix_lenders') }}
),

stg_borrower_addresses AS (
    -- Folosim stg_elementix_addresses pentru adresa principală a împrumutatului
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
    -- Din stg_mortgages (informații principale despre ipotecă)
    sm.mortgage_id,
    sm.document_id,
    sm.issuance_date,
    sm.recording_date,
    sm.maturity_date,
    sm.mortgage_amount,
    sm.loan_term_months,
    
    sm.county_id AS mortgage_county_id,
    sm.county_name AS mortgage_county_name,
    sm.county_state_code AS mortgage_county_state_code,
    sm.region_id AS mortgage_region_id,

    -- Flag-uri
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

    -- Detalii despre creditor (din stg_lenders)
    sl.lender_id,
    sl.lender_name,
    sl.lender_type,

    -- Detalii despre adresa principală a împrumutatului/proprietății (din stg_borrower_addresses)
    -- Acest JOIN se face pe borrower_address_id din stg_mortgages
    sba.address_id AS borrower_address_id,
    sba.borrower_full_address,
    sba.borrower_city,
    sba.borrower_county,
    sba.borrower_state_code,
    sba.borrower_zip_code,
    sba.borrower_latitude,
    sba.borrower_longitude,

    -- Date trunchiate pentru agregări ușoare în marts
    DATE_TRUNC('MONTH', sm.recording_date)::DATE AS recording_month,
    DATE_TRUNC('YEAR', sm.recording_date)::DATE AS recording_year,
    
    -- Numărul de proprietăți asociate (din array-ul original)
    ARRAY_SIZE(sm.property_address_ids_array) AS num_property_addresses_in_loan,
    -- Primul nume de împrumutat (dacă există în array)
    sm.borrower_names_array[0]::VARCHAR AS primary_borrower_name -- Index 0, poate fi NULL

FROM stg_mortgages sm
LEFT JOIN stg_lenders sl
    ON sm.lender_id = sl.lender_id
LEFT JOIN stg_borrower_addresses sba
    ON sm.borrower_address_id = sba.address_id 
    -- Presupunem că sm.borrower_address_id este cheia pentru adresa principală
    -- a proprietății ipotecate sau a împrumutatului.

-- WHERE sm.mortgage_id = 'some_test_id' -- Pentru depanare rapidă pe un singur ID