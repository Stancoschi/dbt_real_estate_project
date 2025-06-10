-- models/intermediate/elementix/int_elementix_mortgage_property_addresses.sql
WITH stg_mortgages AS (
    SELECT
        mortgage_id,
        property_address_ids_array -- Coloana ARRAY cu ID-urile adreselor
    FROM {{ ref('stg_elementix_mortgages') }}
),

stg_addresses AS (
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

-- Aplatizează array-ul de ID-uri de adrese din ipoteci
-- Funcția FLATTEN din Snowflake este folosită aici.
-- Pentru fiecare mortgage_id, va crea un rând pentru fiecare element din property_address_ids_array.
flattened_property_addresses AS (
    SELECT
        m.mortgage_id,
        f.value::VARCHAR AS property_address_id -- Extragem valoarea din array (ID-ul adresei)
                                                -- și o castăm la VARCHAR (sau tipul ID-ului adresei)
    FROM stg_mortgages m,
    LATERAL FLATTEN(input => m.property_address_ids_array) f
    WHERE f.value IS NOT NULL -- Ignorăm ID-urile nule din array, dacă există
)
-- Unim cu detaliile adresei
SELECT
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