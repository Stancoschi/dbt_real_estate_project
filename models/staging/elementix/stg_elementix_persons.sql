-- This staging model cleans and prepares the raw data for persons or entities
-- involved in transactions. It standardizes column names for clarity and consistency.

WITH source_data AS (
    -- Select directly from the source table for persons.
    SELECT * FROM {{ source('elementix_deed_mortgage', 'PERSONS') }}
)

SELECT
    -- Identifiers
    ID AS person_id, -- The primary key for the person/entity record.
    BEST_PERSON_ALIAS_ID AS best_person_alias_id, -- An ID that may link to a "master" or cleansed entity record.
    
    -- Person Name Details
    NAME AS person_name_raw,
    NORMALIZED_FIRST_NAME AS person_first_name_normalized,
    NORMALIZED_MIDDLE_NAME AS person_middle_name_normalized,
    NORMALIZED_LAST_NAME AS person_last_name_normalized,
    
    -- Associated Company Details
    COMPANY_NAME AS person_company_name,
    COMPANY_ROLE AS person_company_role,
    COMPANY_TYPE AS person_company_type,
    COMPANY_ADDRESS AS person_company_address,
    COMPANY_DOMAIN_NAME AS person_company_domain_name,
    COMPANY_INDUSTRY AS person_company_industry,
    DESCRIPTION_AT_COMPANY AS person_description_at_company,
    
    -- Geographic Identifier
    REGION_ID AS region_id

FROM source_data
-- Filter out records that are missing a primary key.
WHERE ID IS NOT NULL