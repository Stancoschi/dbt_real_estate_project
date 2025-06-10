-- models/staging/elementix/stg_elementix_mortgages.sql
WITH source_data AS (
    SELECT * FROM {{ source('elementix_deed_mortgage', 'MORTGAGES') }}
)
SELECT
    -- Chei și Identificatori
    ID AS mortgage_id,
    DOCUMENT_ID AS document_id,
    COUNTY_DOCUMENT_ID AS county_document_id,
    NULLIF(TRIM(lender_id), '') AS lender_id, -- Cheie străină către stg_elementix_lenders
    LENDER_NAME AS lender_name_raw, -- Numele creditorului, poate fi util pentru verificări
    LENDER_ADDRESS_ID AS lender_address_id, -- Cheie străină
    LENDER_ALIAS_ID AS lender_alias_id,
    BORROWER_ADDRESS_ID AS borrower_address_id, -- Cheie străină către stg_elementix_addresses

    -- Date Cheie (folosim TRY_CAST pentru robustețe)
    TRY_CAST(ISSUANCE_DATE AS DATE) AS issuance_date,
    TRY_CAST(RECORDING_DATE AS DATE) AS recording_date,
    TRY_CAST(MATURITY_DATE AS DATE) AS maturity_date,

    -- Valori Numerice (folosim TRY_CAST pentru robustețe)
    TRY_CAST(MORTGAGE_AMOUNT AS NUMBER) AS mortgage_amount,
    TRY_CAST(LOAN_TERM_MONTHS AS NUMBER) AS loan_term_months,

    -- Informații Geografice (denormalizate aici, dar utile)
    COUNTY_ID AS county_id,
    COUNTY_NAME AS county_name,
    COUNTY_STATE AS county_state_code,
    REGION_ID AS region_id,

    -- Flag-uri Booleene (Snowflake tratează TRUE/FALSE corect, dar un CAST explicit poate fi adăugat dacă sursa e VARCHAR)
    -- Dacă sursa are 'Y'/'N' sau '1'/'0', va trebui să le convertești. Ex: IFF(IS_CONDO = 'Y', TRUE, FALSE)
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
    
    -- Coloane ARRAY (le păstrăm așa cum sunt în staging, le vom aplatiza în modelele intermediare)
    ADDRESSES AS property_raw_addresses_array, -- Conține adrese textuale?
    ADDRESSES_IDS AS property_address_ids_array, -- Conține ID-uri de adrese?
    BORROWER_NAMES AS borrower_names_array,

    -- Legat de Satisfacție/Închidere
    SATISFACTION_ID AS satisfaction_id

    -- BORROWER_ADDRESS, LENDER_ADDRESS (textuale, pot fi utile pt QC sau dacă ID-urile lipsesc)
    -- TRIM(BORROWER_ADDRESS) AS borrower_address_text,
    -- TRIM(LENDER_ADDRESS) AS lender_address_text
FROM source_data
WHERE ID IS NOT NULL 
  AND TRY_CAST(RECORDING_DATE AS DATE) IS NOT NULL -- Un filtru important pentru analize temporale
  AND mortgage_id  is not NULL -- Asigură-te că avem un ID valid pentru ipotecă
  AND lender_id IS NOT NULL AND TRIM(lender_id) <> ''