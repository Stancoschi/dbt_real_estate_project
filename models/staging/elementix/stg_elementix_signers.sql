WITH
source_data AS (
    SELECT * FROM {{ source('elementix_deed_mortgage', 'SIGNERS') }}
),
valid_mortgages AS (
    SELECT DISTINCT mortgage_id FROM {{ ref('stg_elementix_mortgages') }}
)
SELECT
    s.ID AS signer_record_id,
    s.DEED_ID AS deed_id,
    s.MORTGAGE_ID AS mortgage_id,
    
    s.NAME AS signer_name_raw,
    s.NORMALIZED_FIRST_NAME AS signer_first_name_normalized,
    s.NORMALIZED_MIDDLE_NAME AS signer_middle_name_normalized,
    s.NORMALIZED_LAST_NAME AS signer_last_name_normalized,
    s.NORMALIZED_FULL_NAME AS signer_full_name_normalized,
    s.ORIGINAL_CLEANED_NAME AS original_cleaned_name,
    
    s.TITLE AS signer_title,
    
    s.NOTARY_ID AS notary_id,
    s.NOTARY_NAME AS notary_name,
    s.NOTARY_STATE AS notary_state_code,
    
    s.SIGNING_ON_BEHALF_OF AS signing_on_behalf_of_array,
    s.SATISFACTION_ID AS satisfaction_id
FROM source_data s
JOIN valid_mortgages m ON s.MORTGAGE_ID = m.mortgage_id
WHERE s.ID IS NOT NULL
