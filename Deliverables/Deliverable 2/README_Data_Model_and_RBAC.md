# Data Model and RBAC Design

This document outlines the logical data model and Role-Based Access Control (RBAC) strategy for the dbt real estate project.

## 1. Schema Structure

The data model is organized into a layered architecture using two primary schemas in Snowflake:

-   **`RAW_STAGING`**: This schema houses the first layer of transformations. Models in this layer are configured as `views`. Their purpose is to:
    -   Select data from the raw source tables in the `DEED_AND_MORTGAGE_INTELLIGENCE` database.
    -   Perform basic cleaning, such as renaming columns for clarity (e.g., `ID` -> `mortgage_id`), casting data types (`TRY_CAST`), and handling basic nulls.
    -   No business logic or joins are applied at this stage.

-   **`ANALYTICS`**: This is the presentation schema, containing data that is fully transformed, aggregated, and ready for business intelligence and reporting. Models in this layer are configured as `tables` or `incremental tables` for performance. It follows a Star Schema design:
    -   **Fact Tables (`fct_...`)**: Contain aggregated numerical measures (e.g., `total_mortgage_amount`, `num_mortgages`) grouped by dimensional attributes. `fct_elementix_monthly_mortgage_activity` is an incremental table to efficiently process daily data updates.
    -   **Dimension Tables (`dim_...`)**: Contain descriptive attributes that provide context to the fact tables (e.g., `dim_lenders`, `dim_properties`, `dim_date`).

## 2. Table Purposes

### Fact Tables
-   `fct_elementix_monthly_mortgage_activity`: Aggregates mortgage metrics monthly by geography (state, county) and lender type. This is the primary table for BI dashboards.

### Dimension Tables
-   `dim_lenders`: A dimension containing details about each unique lending institution.
-   `dim_properties`: A dimension representing each unique property address from the dataset.
-   `dim_date`: A standard date dimension generated to facilitate time-based analysis.

## 3. Role-Based Access Control (RBAC)

Two distinct roles have been created to enforce the principle of least privilege:

| Role | Database / Schema  | Privileges Granted | Purpose |
| -| - | - | - |
| **`data_engineer`** | `DBT_REAL_ESTATE_PROJECT_DB.RAW_STAGING`      | `ALL` (CREATE/ALTER/DROP) | A developer role for building and testing new models in the staging area.|
| | `DBT_REAL_ESTATE_PROJECT_DB.ANALYTICS`  | `SELECT`(Read-only)  | To query and validate the final models without being able to change them.    |
| | `DEED_AND_MORTGAGE_INTELLIGENCE.PUBLIC` | `SELECT` (Read-only)   | To read from the raw source data.                                                  |
| **`etl_user`**  | `DBT_REAL_ESTATE_PROJECT_DB` (all schemas)    | `ALL` (CREATE/ALTER/DROP)    | A functional, non-human role used by automation tools (like GitHub Actions) to run `dbt run` and deploy the entire project. |
| | `DEED_AND_MORTGAGE_INTELLIGENCE.PUBLIC`     | `SELECT` (Read-only)  | To read from the raw source data. |

This design separates development permissions (`data_engineer`) from automated deployment permissions (`etl_user`), ensuring a secure and controlled data environment.