-- This model creates the property dimension table.
-- Each record represents a unique physical property address from the dataset,
-- providing detailed geographical and descriptive attributes.

WITH stg_addresses AS (
    -- Select all address attributes from the staging layer.
    -- The primary key `address_id` is aliased to `property_id` for clarity
    -- within the context of a property-centric dimension.
    SELECT
        address_id AS property_id,
        full_address,
        short_address,
        street_number,
        street_name,
        unit_number,
        city,
        county_name,
        state_code,
        zip_code,
        latitude,
        longitude,
        region_id
    FROM {{ ref('stg_elementix_addresses') }}
)

SELECT
    property_id,
    full_address,
    short_address,
    street_number,
    street_name,
    unit_number,
    city,
    county_name,
    state_code,
    zip_code,
    latitude,
    longitude,
    region_id
FROM stg_addresses
WHERE property_id IS NOT NULL
-- NOTE: A `SELECT DISTINCT` is not used here because the `address_id`
-- from the staging layer is already tested for uniqueness. This assumes
-- clean source data and improves performance by avoiding an unnecessary operation.