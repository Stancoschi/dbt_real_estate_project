-- models/marts/elementix/dims/dim_elementix_lenders.sql
WITH stg_lenders AS (
    SELECT
        lender_id,
        lender_name,
        lender_type,
        lender_address_full, -- Din stg_elementix_lenders
        lender_domain_name
    FROM {{ ref('stg_elementix_lenders') }}
)
SELECT
    lender_id,
    lender_name,
    COALESCE(lender_type, 'Unknown') AS lender_type, -- Exemplu de imputare simplă
    lender_address_full,
    lender_domain_name
    -- Poți adăuga aici coloane SCD Type 2 (slowly changing dimensions) dacă e necesar
    -- sau agregate precum prima/ultima dată a unei ipoteci, dar asta ar necesita join-uri.
    -- Pentru început, o păstrăm simplă.
FROM stg_lenders
WHERE lender_id IS NOT NULL -- Asigură-te că ID-ul este prezent