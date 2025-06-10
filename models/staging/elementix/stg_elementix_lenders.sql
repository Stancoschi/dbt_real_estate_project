-- models/staging/elementix/stg_elementix_lenders.sql
WITH source_data AS (
    SELECT * FROM {{ source('elementix_deed_mortgage', 'LENDERS') }}
)
SELECT
    COALESCE(ID,'UNKNOWN') AS lender_id,          -- Presupunem cheie primară
    NAME AS lender_name,
    LENDER_TYPE AS lender_type,
    TRIM(ADDRESS) AS lender_address_full, -- TRIM pentru a elimina spații accidentale
                                        -- Această adresă poate necesita parsare/join ulterior
    DOMAIN_NAME AS lender_domain_name,
    DESCRIPTION AS lender_description
    -- Adaugă alte coloane relevante
FROM source_data
WHERE ID IS NOT NULL -- Un filtru de bază, dacă ID-ul e cheie