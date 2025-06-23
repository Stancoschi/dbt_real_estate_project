-- This singular test checks for duplicate records in the mortgage-to-property link model.
-- Each combination of mortgage_id and property_address_id should be unique.
-- If this query returns any rows, the test will fail.

SELECT
    mortgage_id,
    property_address_id,
    COUNT(*) as record_count
FROM {{ ref('int_elementix_mortgage_property_addresses') }}
GROUP BY
    mortgage_id,
    property_address_id
HAVING record_count > 1