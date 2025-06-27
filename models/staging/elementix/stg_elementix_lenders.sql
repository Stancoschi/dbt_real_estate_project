-- This staging model cleans and prepares the raw lender data.
-- It focuses on renaming columns for clarity and performing light data cleaning.

WITH source_data AS (
    -- Select directly from the source table for lenders.
    SELECT * FROM {{ source('elementix_deed_mortgage', 'LENDERS') }}
)

SELECT
    -- Keys and Identifiers
    ID AS lender_id, -- This is the primary key for lenders.
    
    -- Lender Attributes
    NAME AS lender_name,
    LENDER_TYPE AS lender_type,
    
    -- Trim whitespace from the address field to ensure consistency.
    -- Further parsing of this address could happen in a downstream model if needed.
    TRIM(ADDRESS) AS lender_address_full,
    
    DOMAIN_NAME AS lender_domain_name,
    DESCRIPTION AS lender_description

FROM source_data
-- Filter out records where the primary key is null, as they cannot be reliably joined.
WHERE ID IS NOT NULL