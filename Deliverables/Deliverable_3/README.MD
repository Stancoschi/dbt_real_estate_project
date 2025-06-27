# dbt Real Estate Mortgage Analysis Project

This dbt project performs data transformation on the "Elementix Deed and Mortgage Intelligence" dataset from the Snowflake Marketplace. It builds a complete data pipeline from raw source data to an analytics-ready star schema.

## Project Structure

The project follows the dbt best practice of layering data transformations:

-   `models/staging`: This layer contains one view for each source table. Its purpose is to perform light cleaning:
    -   Renaming columns for clarity.
    -   Casting data to the correct types.
    -   Basic null handling.
    -   *No joins are performed here.*

-   `models/intermediate`: This layer is for complex transformations and business logic. These models are `ephemeral` (they exist only as CTEs and are not materialized in the database).
    -   **`int_elementix_mortgages_enriched`**: Joins mortgages with lender and property address details.
    -   **`int_..._borrowers`, `int_..._property_addresses`**: These models use `LATERAL FLATTEN` to unnest array columns from the source data, creating a one-to-many relationship (e.g., one mortgage to many borrowers).

-   `models/marts`: This is the final presentation layer, creating tables ready for BI analysis.
    -   **`fct_elementix_monthly_mortgage_activity`**: An `incremental` fact table that aggregates key metrics like mortgage count and total amount. Its incremental design allows for efficient daily updates.
    -   **`dim_*` tables**: Dimension tables (`dim_lenders`, `dim_properties`, `dim_date`) that provide descriptive context for the facts.

## Testing Approach

Data quality is enforced through a comprehensive testing strategy:

-   **Generic Tests**: `unique` and `not_null` tests are applied to all primary key columns in staging and mart models.
-   **Relationship Tests**: Referential integrity between tables (e.g., between `stg_mortgages` and `stg_lenders`) is validated using `relationships` tests.
-   **Custom Logic Tests**: The `dbt-expectations` and `dbt-utils` packages are used to enforce more complex business rules, such as:
    -   `expect_column_value_lengths_to_be_between` for state codes and zip codes.
    -   `accepted_range` to ensure mortgage amounts are non-negative.