-- models/marts/elementix/facts/fct_elementix_monthly_mortgage_activity.sql
WITH mortgages_enriched AS (
    SELECT *
    FROM {{ ref('int_elementix_mortgages_enriched') }}
    WHERE recording_date IS NOT NULL -- Asigură-te că avem o dată pentru agregare
      AND mortgage_amount IS NOT NULL AND mortgage_amount > 0 -- Considerăm doar ipoteci valide
      AND borrower_state_code IS NOT NULL -- Important pentru agregare geografică
),

dim_date AS (
    SELECT date_day FROM {{ ref('dim_date') }}
)

SELECT
    -- Chei de dimensiune / Grupări
    me.recording_month, -- Deja calculat în int_elementix_mortgages_enriched
    
    -- Chei către dimensiuni (opțional, dar bună practică dacă le folosești activ în BI)
    -- me.lender_id, -- Poate fi păstrat, sau doar lender_type
    -- me.borrower_address_id AS property_id, -- Cheie către dim_properties (adresa principală a împrumutatului)

    -- Atribute pentru grupare direct în tabelul de fapte (denormalizare)
    me.borrower_state_code AS property_state_code, 
    me.borrower_county AS property_county,
    -- me.borrower_zip_code AS property_zip_code, -- Poate fi prea granular pentru un mart lunar general
    me.lender_type,

    -- Flag-uri pentru tipuri de împrumut (pentru a putea filtra/segmenta pe ele)
    me.is_construction_loan,
    me.is_heloc,
    me.is_owner_occupied,
    me.is_variable_rate,
    me.is_second_mortgage,
    
    -- Metrici Agregate
    COUNT(DISTINCT me.mortgage_id) AS num_mortgages,
    SUM(me.mortgage_amount) AS total_mortgage_amount,
    AVG(me.mortgage_amount) AS avg_mortgage_amount,
    MEDIAN(me.mortgage_amount) AS median_mortgage_amount, -- Snowflake suportă MEDIAN
    
    AVG(me.loan_term_months) AS avg_loan_term_months,
    
    -- Metrici pentru tipuri specifice de împrumut
    SUM(CASE WHEN me.is_construction_loan THEN me.mortgage_amount ELSE 0 END) AS total_construction_loan_amount,
    COUNT(DISTINCT CASE WHEN me.is_construction_loan THEN me.mortgage_id ELSE NULL END) AS num_construction_loans,

    SUM(CASE WHEN me.is_heloc THEN me.mortgage_amount ELSE 0 END) AS total_heloc_amount,
    COUNT(DISTINCT CASE WHEN me.is_heloc THEN me.mortgage_id ELSE NULL END) AS num_heloc_loans,

    SUM(CASE WHEN me.is_owner_occupied THEN me.mortgage_amount ELSE 0 END) AS total_owner_occupied_amount,
    COUNT(DISTINCT CASE WHEN me.is_owner_occupied THEN me.mortgage_id ELSE NULL END) AS num_owner_occupied_loans

FROM mortgages_enriched me
-- Join opțional cu dim_date dacă ai nevoie de atribute din dim_date care nu sunt deja pe me.recording_month
-- LEFT JOIN dim_date dd ON me.recording_month = dd.first_day_of_month 

GROUP BY
    me.recording_month,
    me.borrower_state_code,
    me.borrower_county,
    -- me.borrower_zip_code,
    me.lender_type,
    me.is_construction_loan,
    me.is_heloc,
    me.is_owner_occupied,
    me.is_variable_rate,
    me.is_second_mortgage
    -- Dacă ai adăugat lender_id și property_id, grupează și pe ele.
ORDER BY
    me.recording_month DESC,
    me.borrower_state_code,
    me.borrower_county