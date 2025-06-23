# Real Estate Mortgage Analytics Pipeline

This project is a complete data engineering solution built on Snowflake, designed to process and analyze real estate mortgage data. It demonstrates a full data lifecycle, from raw data ingestion and transformation to automated deployment and business intelligence reporting.

The core technologies used are **Snowflake** for the data warehouse, **dbt (data build tool)** for transformations, and **GitHub Actions** for CI/CD and automation.


## 1. Dataset Selection

### Dataset: Deed and Mortgage Intelligence by Elementix
The project leverages the **Elementix Deed and Mortgage Intelligence** dataset, available on the Snowflake Marketplace.

### Rationale for Selection
This dataset was chosen for several key reasons that make it ideal for a comprehensive data engineering project:

*   **Real-World Complexity:** It mirrors the complexity of real-world data, containing millions of records across multiple related tables (`MORTGAGES`, `LENDERS`, `ADDRESSES`, etc.).
*   **Richness of Data:** It includes a wide variety of data types, from numerical and date values to semi-structured data like `ARRAYS` (e.g., lists of borrower names and property addresses). This provided an excellent opportunity to implement advanced transformation techniques like `LATERAL FLATTEN`.
*   **High Velocity:** The dataset is updated daily, making it a perfect use case for building **automated, incremental data pipelines** that are both efficient and cost-effective.
*   **Business Value:** The data allows for meaningful business analysis, such as tracking market trends, analyzing lender activity, and understanding geographic hotspots, which culminates in a valuable BI dashboard.

---

## 2. Project Architecture

The project follows a modern, layered data modeling approach to ensure scalability, maintainability, and data quality.

![Analytics Schema ERD](ERD_Diagram.png) 


### Schema Layers
The data is processed through three distinct layers within Snowflake:

1.  **Staging Layer (`RAW_STAGING` schema)**
    *   **Purpose:** To create a clean, standardized version of the raw source data.
    *   **Implementation:** Models are materialized as `views`.
    *   **Transformations:**
        *   Column renaming for clarity and consistency.
        *   Type casting (`TRY_CAST` for robustness).
        *   Basic data cleaning (e.g., trimming whitespace, standardizing nulls).
        *   No joins or complex business logic are applied here.

2.  **Intermediate Layer (`ephemeral` models)**
    *   **Purpose:** To handle complex transformations and serve as logical building blocks for the final models.
    *   **Implementation:** Models are `ephemeral`, meaning they are not physically created in the database but are injected as Common Table Expressions (CTEs) during the run.
    *   **Transformations:**
        *   Joining core business entities (e.g., mortgages with lenders).
        *   Unnesting `ARRAY` data into a tabular format using `LATERAL FLATTEN`.

3.  **Marts Layer (`ANALYTICS` schema)**
    *   **Purpose:** To provide clean, aggregated, and analytics-ready tables for BI tools and end-users.
    *   **Implementation:** Models are materialized as `tables` for performance, following a Star Schema design.
    *   **Key Models:**
        *   `fct_elementix_monthly_mortgage_activity`: An **incremental fact table** that aggregates key metrics. Its incremental design ensures that daily runs are fast and only process new data.
        *   `dim_lenders`, `dim_properties`, `dim_date`: Dimension tables that provide descriptive context to the facts.

---

## 3. Technology Stack

*   **Data Warehouse:** Snowflake
*   **Data Transformation:** dbt Core
*   **Automation & CI/CD:** GitHub Actions
*   **Data Quality:** dbt Tests (including generic, singular, and packages like `dbt-expectations`)
*   **Business Intelligence:** Snowsight Dashboards

---

## 4. Key Features Implemented

*   **Incremental Models:** The main fact table is incremental, designed to efficiently handle daily data updates without requiring full refreshes.
*   **Comprehensive Data Testing:** The pipeline includes a robust suite of tests to ensure data quality, including checks for uniqueness, non-null values, referential integrity (`relationships`), and custom business logic.
*   **CI/CD Automation:** A GitHub Actions workflow automates the entire process. It triggers on pushes to `main` and runs on a daily schedule, executing `dbt run` and `dbt test` to ensure the data warehouse is always up-to-date and validated.
*   **Secure Credential Management:** Snowflake credentials are not stored in the repository. They are managed securely using GitHub Secrets and injected into the CI/CD environment at runtime.
*   **Role-Based Access Control (RBAC):** The project defines dedicated Snowflake roles (`data_engineer`, `etl_user`) with specific privileges to enforce the principle of least privilege.

---

## 5. How to Run the Project

### Prerequisites
*   Python 3.9+
*   A dbt profile configured to connect to your Snowflake account. See `dbt_project.yml` for the required profile name.

### Setup
1.  **Clone the repository:**
    ```bash
    git clone [your-repo-url]
    cd [your-repo-name]
    ```
2.  **Install dbt and the Snowflake adapter:**
    ```bash
    pip install dbt-snowflake
    ```
3.  **Install dbt packages:**
    ```bash
    dbt deps
    ```
    This will install packages like `dbt_utils` from `packages.yml`.

### Execution
1.  **Run the models:**
    ```bash
    # For the first run, a full-refresh is needed for the incremental model
    dbt run --full-refresh 
    ```
2.  **Test the data:**
    ```bash
    dbt test
    ```
3.  **For subsequent daily runs:**
    ```bash
    dbt run
    ```
---

## 6. BI Dashboard Preview

A BI dashboard was created in Snowsight to visualize the key metrics from the `fct_elementix_monthly_mortgage_activity` table. It provides insights into market trends, geographic activity, and loan type distribution.


![Snowsight Dashboard](Snowflake_Dashboard.png)