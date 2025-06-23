-- This model unnests the `signing_on_behalf_of_array` from the signers data.
-- Each row in the output represents a single entity that a person was signing on behalf of,
-- creating a clear link between a signer record, a mortgage, and the represented entity.

WITH stg_signers AS (
    -- Select only the necessary columns for this transformation.
    SELECT
        signer_record_id,
        mortgage_id,
        signing_on_behalf_of_array
    FROM {{ ref('stg_elementix_signers') }}
)

-- Use LATERAL FLATTEN to transform the array of entities into a flat, one-to-many structure.
SELECT
    s.signer_record_id,
    s.mortgage_id,
    f.value::VARCHAR AS signing_on_behalf_of_entity, -- The name of the entity being represented.
    f.index AS behalf_of_index                      -- The 0-based index from the original array.
FROM stg_signers s,
LATERAL FLATTEN(input => s.signing_on_behalf_of_array) f
WHERE f.value IS NOT NULL AND TRIM(f.value::VARCHAR) <> '' -- Filter out any null or empty strings.