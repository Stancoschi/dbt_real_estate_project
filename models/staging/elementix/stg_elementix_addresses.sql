-- This staging model cleans and standardizes the raw address data.
-- It performs basic transformations like column renaming, type casting,
-- and simple data cleaning to prepare the data for downstream models.

WITH source_data AS (
    -- Select directly from the source table defined in `sources.yml`.
    SELECT * FROM {{ source('elementix_deed_mortgage', 'ADDRESSES') }}
)

SELECT
    -- Keys and Identifiers
    ID AS address_id, -- Rename for clarity and consistency. This is the primary key.
    REGION_ID AS region_id,

    -- Address Details
    ADDRESS_FULL AS full_address,
    ADDRESS_SHORT AS short_address,
    STREET_NUMBER AS street_number,
    STREET_NAME_PRE_DIRECTIONAL AS street_name_pre_directional,
    STREET_NAME AS street_name,
    STREET_NAME_POST_TYPE AS street_name_post_type,
    STREET_NAME_POST_DIRECTIONAL AS street_name_post_directional,
    UNIT AS unit_number,
    CITY AS city,
    COUNTY AS county_name,
    STATE AS state_code,

    -- Clean the zip_code field by standardizing empty strings to NULL.
    NULLIF(TRIM(zip_code), '') AS zip_code,

    -- Geographic Coordinates
    -- No explicit casting is needed here if the source types are already numeric.
    -- If they were text, TRY_CAST(latitude AS NUMBER(10, 7)) would be appropriate.
    LATITUDE AS latitude,
    LONGITUDE AS longitude

FROM source_data
-- Basic filtering at the staging level to remove records with invalid zip codes.
-- This improves data quality for all downstream models.
WHERE zip_code IS NOT NULL
  AND LENGTH(TRIM(zip_code)) BETWEEN 5 AND 10 -- Allows for 5-digit and ZIP+4 formats.
  AND address_id IS NOT NULL -- Ensure primary key exists.