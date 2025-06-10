-- models/marts/elementix/dims/dim_elementix_properties.sql
WITH stg_addresses AS (
    SELECT
        address_id AS property_id, -- Redenumim pentru claritate în contextul dimensiunii
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
WHERE property_id IS NOT NULL -- Asigură unicitatea și validitatea
-- Deoarece stg_elementix_addresses.address_id este deja testat ca unic și not_null,
-- un SELECT DISTINCT nu ar trebui să fie necesar aici dacă sursa e curată.