-- This staging model prepares data for signers on mortgage documents.
-- It cleans and renames columns and, importantly, filters out signers
-- associated with mortgages that have already been filtered out in `stg_elementix_mortgages`.
-- This ensures data consistency and improves performance in downstream models.

WITH source_data AS (
    -- Select all columns from the raw signers table.
    SELECT * FROM {{ source('elementix_deed_mortgage', 'SIGNERS') }}
),

valid_mortgages AS (
    -- Create a distinct list of mortgage IDs that are considered valid
    -- based on the filters applied in the stg_elementix_mortgages model.
    SELECT DISTINCT mortgage_id
    FROM {{ ref('stg_elementix_mortgages') }}
)

SELECT
    -- Identifiers
    s.ID AS signer_record_id, -- Primary key for the signer record itself.
    s.DEED_ID AS deed_id,
    s.MORTGAGE_ID AS mortgage_id,
    s.SATISFACTION_ID AS satisfaction_id,
    
    -- Signer Name Details
    s.NAME AS signer_name_raw,
    s.NORMALIZED_FIRST_NAME AS signer_first_name_normalized,
    s.NORMALIZED_MIDDLE_NAME AS signer_middle_name_normalized,
    s.NORMALIZED_LAST_NAME AS signer_last_name_normalized,
    s.NORMALIZED_FULL_NAME AS signer_full_name_normalized,
    s.ORIGINAL_CLEANED_NAME AS original_cleaned_name,
    s.TITLE AS signer_title,
    
    -- Notary Details associated with the signer
    s.NOTARY_ID AS notary_id,
    s.NOTARY_NAME AS notary_name,
    s.NOTARY_STATE AS notary_state_code,
    
    -- Array column to be unnested in an intermediate model
    s.SIGNING_ON_BEHALF_OF AS signing_on_behalf_of_array

FROM source_data s
-- Use an INNER JOIN to act as a filter. This keeps only the signer records
-- that are linked to a mortgage present in our clean `stg_elementix_mortgages` view.
INNER JOIN valid_mortgages m
    ON s.MORTGAGE_ID = m.mortgage_id
-- An additional filter to ensure the signer record itself has a primary key.
WHERE s.ID IS NOT NULL