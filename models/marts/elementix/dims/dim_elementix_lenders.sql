-- This model creates the lender dimension table.
-- It provides a clean, unique record for each lending institution,
-- which can be used to enrich fact tables with lender-specific attributes.

WITH stg_lenders AS (
    -- Select all relevant columns from the staging model for lenders.
    SELECT
        lender_id,
        lender_name,
        lender_type,
        lender_address_full,
        lender_domain_name
    FROM {{ ref('stg_elementix_lenders') }}
)

SELECT
    lender_id,
    lender_name,
    -- Impute null lender types as 'Unknown' to ensure data consistency.
    COALESCE(lender_type, 'Unknown') AS lender_type,
    lender_address_full,
    lender_domain_name
    -- NOTE: This is a Type 1 Slowly Changing Dimension (SCD), where existing
    -- records are overwritten with new information. For historical tracking (SCD Type 2),
    -- additional logic for versioning records would be required.
FROM stg_lenders
WHERE lender_id IS NOT NULL -- Ensure that every record in the dimension has a valid primary key.