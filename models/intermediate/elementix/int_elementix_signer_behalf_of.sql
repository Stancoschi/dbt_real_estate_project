-- models/intermediate/elementix/int_elementix_signer_behalf_of.sql
WITH stg_signers AS (
    SELECT
        signer_record_id,
        mortgage_id,
        signing_on_behalf_of_array -- Coloana ARRAY
    FROM {{ ref('stg_elementix_signers') }}
)
-- Aplatizează array-ul
SELECT
    s.signer_record_id,
    s.mortgage_id,
    f.value::VARCHAR AS signing_on_behalf_of_entity,
    f.index AS behalf_of_index
FROM stg_signers s,
LATERAL FLATTEN(input => s.signing_on_behalf_of_array) f
WHERE f.value IS NOT NULL AND TRIM(f.value::VARCHAR) <> ''