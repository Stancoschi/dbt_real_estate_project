-- This model unnests the borrower_names_array from each mortgage record.
-- The purpose is to create a clean, one-to-many mapping between a single mortgage
-- and all of its associated borrowers, making borrower-level analysis possible.

WITH stg_mortgages AS (
    -- Select only the necessary columns from the staging model to keep the CTE light.
    SELECT
        mortgage_id,
        borrower_names_array
    FROM {{ ref('stg_elementix_mortgages') }}
)

-- Unnest the borrower_names_array using LATERAL FLATTEN.
-- For each mortgage, this creates a new row for every name in the array.
SELECT
    m.mortgage_id,
    f.value::VARCHAR AS borrower_name, -- Extracts the name from the flattened structure.
    f.index AS borrower_index         -- The 0-based index of the name in the original array, useful for identifying the "primary" borrower.
FROM stg_mortgages m,
LATERAL FLATTEN(input => m.borrower_names_array) f
WHERE f.value IS NOT NULL AND TRIM(f.value::VARCHAR) <> '' -- Filter out any null or empty strings from the array.