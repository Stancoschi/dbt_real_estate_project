-- This model unnests the property_address_ids_array from each mortgage.
-- The purpose is to create a link table that maps each mortgage to every single
-- property it covers, which is crucial for analyzing blanket loans or property portfolios.

WITH stg_mortgages AS (
    -- Select only the mortgage ID and the array of address IDs to be unnested.
    SELECT
        mortgage_id,
        property_address_ids_array
    FROM {{ ref('stg_elementix_mortgages') }}
),

stg_addresses AS (
    -- Select all descriptive attributes for each address.
    SELECT
        address_id,
        full_address,
        city AS property_city,
        county_name AS property_county,
        state_code AS property_state_code,
        zip_code AS property_zip_code,
        latitude AS property_latitude,
        longitude AS property_longitude
    FROM {{ ref('stg_elementix_addresses') }}
),

-- Use LATERAL FLATTEN to transform the nested array of address IDs into a flat,
-- tabular structure. This creates a distinct row for each address associated with a mortgage.
flattened_property_addresses AS (
    SELECT
        m.mortgage_id,
        f.value::VARCHAR AS property_address_id
    FROM stg_mortgages m,
    LATERAL FLATTEN(input => m.property_address_ids_array) f
    WHERE f.value IS NOT NULL -- Ensure we don't process any null IDs that might be in the array.
)

-- Join the flattened address IDs with the full address details to enrich the data.
SELECT DISTINCT
    fpa.mortgage_id,
    fpa.property_address_id,
    sa.full_address AS property_full_address,
    sa.property_city,
    sa.property_county,
    sa.property_state_code,
    sa.property_zip_code,
    sa.property_latitude,
    sa.property_longitude
FROM flattened_property_addresses fpa
LEFT JOIN stg_addresses sa
    ON fpa.property_address_id = sa.address_id