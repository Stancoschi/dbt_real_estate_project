-- models/staging/elementix/stg_elementix_persons.sql
WITH source_data AS (
    SELECT * FROM {{ source('elementix_deed_mortgage', 'PERSONS') }}
)
SELECT
    ID AS person_id,
    BEST_PERSON_ALIAS_ID AS best_person_alias_id, -- Poate fi un ID către o entitate "curățată"
    
    NAME AS person_name_raw,
    NORMALIZED_FIRST_NAME AS person_first_name_normalized,
    NORMALIZED_MIDDLE_NAME AS person_middle_name_normalized,
    NORMALIZED_LAST_NAME AS person_last_name_normalized,
    
    COMPANY_NAME AS person_company_name,
    COMPANY_ROLE AS person_company_role,
    COMPANY_TYPE AS person_company_type,
    COMPANY_ADDRESS AS person_company_address,
    COMPANY_DOMAIN_NAME AS person_company_domain_name,
    COMPANY_INDUSTRY AS person_company_industry,
    DESCRIPTION_AT_COMPANY AS person_description_at_company,
    
    REGION_ID AS region_id
FROM source_data
WHERE ID IS NOT NULL