-- models/intermediate/elementix/int_elementix_mortgage_borrowers.sql
WITH stg_mortgages AS (
    SELECT
        mortgage_id,
        borrower_names_array -- Coloana ARRAY cu numele împrumutaților
    FROM {{ ref('stg_elementix_mortgages') }}
)
-- Aplatizează array-ul de nume de împrumutați
SELECT
    m.mortgage_id,
    f.value::VARCHAR AS borrower_name, -- Extragem fiecare nume din array
    f.index AS borrower_index         -- Indexul elementului în array (0, 1, 2...)
                                      -- poate fi util pentru a identifica "primul" împrumutat
FROM stg_mortgages m,
LATERAL FLATTEN(input => m.borrower_names_array) f
WHERE f.value IS NOT NULL AND TRIM(f.value::VARCHAR) <> '' -- Ignorăm numele goale