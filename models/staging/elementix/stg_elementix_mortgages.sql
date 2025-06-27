-- This staging model cleans and prepares the core mortgage transaction data.
-- It applies type casting for dates and numbers, renames columns for clarity,
-- and handles basic data cleaning, serving as the foundation for the entire pipeline.

WITH source_data AS (
    SELECT * FROM {{ source('elementix_deed_mortgage', 'MORTGAGES') }}
)

SELECT
    -- Primary and Foreign Keys
    ID AS mortgage_id,
    DOCUMENT_ID AS document_id,
    COUNTY_DOCUMENT_ID AS county_document_id,
    NULLIF(TRIM(lender_id), '') AS lender_id, -- Foreign key to stg_lenders
    LENDER_ADDRESS_ID AS lender_address_id,
    LENDER_ALIAS_ID AS lender_alias_id,
    BORROWER_ADDRESS_ID AS borrower_address_id, -- Foreign key to stg_addresses
    SATISFACTION_ID AS satisfaction_id,
    LENDER_NAME AS lender_name_raw, -- Keep the raw lender name for potential quality checks

    -- Dates - Cast to DATE type for proper date functions and to prevent errors.
    TRY_CAST(ISSUANCE_DATE AS DATE) AS issuance_date,
    TRY_CAST(RECORDING_DATE AS DATE) AS recording_date,
    TRY_CAST(MATURITY_DATE AS DATE) AS maturity_date,

    -- Numeric Values - Cast to NUMBER to ensure correct calculations.
    TRY_CAST(MORTGAGE_AMOUNT AS NUMBER) AS mortgage_amount,
    TRY_CAST(LOAN_TERM_MONTHS AS NUMBER) AS loan_term_months,

    -- Denormalized Geographic Information
    COUNTY_ID AS county_id,
    COUNTY_NAME AS county_name,
    COUNTY_STATE AS county_state_code,
    REGION_ID AS region_id,

    -- Boolean Flags - Explicitly cast to BOOLEAN for consistent filtering.
    IS_AMENDMENT::BOOLEAN AS is_amendment,
    IS_BLANKET::BOOLEAN AS is_blanket_loan,
    IS_CONDO::BOOLEAN AS is_condo_loan,
    IS_CONSTRUCTION::BOOLEAN AS is_construction_loan,
    IS_CORRECTION::BOOLEAN AS is_correction,
    IS_HOME_EQUITY_LINE_OF_CREDIT::BOOLEAN AS is_heloc,
    IS_INTEREST_ONLY::BOOLEAN AS is_interest_only,
    IS_LEASEHOLD::BOOLEAN AS is_leasehold,
    IS_OPEN_END::BOOLEAN AS is_open_end,
    IS_OWNER_OCCUPIED::BOOLEAN AS is_owner_occupied,
    IS_PREPAYMENT_PENALTY::BOOLEAN AS has_prepayment_penalty,
    IS_REVERSE_MORTGAGE::BOOLEAN AS is_reverse_mortgage,
    IS_SECOND_MORTGAGE::BOOLEAN AS is_second_mortgage,
    IS_SUBORDINATE::BOOLEAN AS is_subordinate,
    IS_VARIABLE_RATE::BOOLEAN AS is_variable_rate,
    
    -- Array Columns - Passed through as-is. They will be unnested in intermediate models.
    ADDRESSES AS property_raw_addresses_array,
    ADDRESSES_IDS AS property_address_ids_array,
    BORROWER_NAMES AS borrower_names_array

FROM source_data
-- Apply critical filters at the staging level to ensure a baseline of data quality.
WHERE
    -- A mortgage record is meaningless without its primary key.
    ID IS NOT NULL 
    -- The recording date is essential for almost all time-based analysis.
    AND TRY_CAST(RECORDING_DATE AS DATE) IS NOT NULL
    -- A mortgage must be associated with a lender.
    AND NULLIF(TRIM(lender_id), '') IS NOT NULL