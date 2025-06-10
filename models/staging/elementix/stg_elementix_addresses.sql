-- models/staging/elementix/stg_elementix_addresses.sql
WITH source_data AS (
    -- Referențiem sursa 'ADDRESSES' din 'elementix_deed_mortgage'
    -- așa cum am definit-o în sources_elementix.yml
    SELECT * FROM {{ source('elementix_deed_mortgage', 'ADDRESSES') }}
)
SELECT
    -- Chei și Identificatori
    ID AS address_id,         -- Redenumire și presupunem că este cheia primară
    REGION_ID AS region_id,

    -- Detalii Adresă
    ADDRESS_FULL AS full_address,
    ADDRESS_SHORT AS short_address,
    STREET_NUMBER AS street_number,
    STREET_NAME_PRE_DIRECTIONAL AS street_name_pre_directional,
    STREET_NAME AS street_name,
    STREET_NAME_POST_TYPE AS street_name_post_type,
    STREET_NAME_POST_DIRECTIONAL AS street_name_post_directional,
    UNIT AS unit_number, -- Redenumire pentru claritate
    CITY AS city,
    COUNTY AS county_name,
    STATE AS state_code,       -- Presupunem că e codul statului (ex: 'CA', 'NY')
    CASE 
        WHEN zip_code IS NULL OR TRIM(zip_code) = '' THEN NULL
        ELSE zip_code
    END AS zip_code,

    -- Coordonate Geografice (important să verificăm tipul de date sursă)
    -- Dacă sunt VARCHAR, vom folosi TRY_CAST la NUMBER. Dacă sunt deja NUMBER, putem omite CAST-ul.
    LATITUDE AS latitude,   -- Ajustează precizia/scala dacă e nevoie
    LONGITUDE AS longitude  -- Ajustează precizia/scala dacă e nevoie

    -- Adaugă aici orice alte coloane din ADDRESSES pe care le consideri utile
    -- Ex: _LOADED_AT AS loaded_at -- Dacă sursa are o astfel de coloană
FROM source_data
WHERE zip_code IS NOT NULL AND LENGTH(TRIM(zip_code)) BETWEEN 5 AND 10 -- Asigură-te că ZIP_CODE nu este NULL
-- Poți adăuga un WHERE aici dacă vrei să filtrezi înregistrările la nivel de staging
-- Ex: WHERE ID IS NOT NULL