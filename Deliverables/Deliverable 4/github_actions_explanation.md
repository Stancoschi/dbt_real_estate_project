# GitHub Actions CI/CD Workflow Explanation



**Workflow File:** `.github/workflows/dbt_deploy.yml`

## Workflow Steps

1.  **Trigger (`on`)**:
    -   The workflow is automatically triggered on any `push` or `pull_request` to the `main` branch. This ensures that any code being merged into production is automatically tested.
    -   It is also configured to run on a `schedule` (daily at 5:00 AM UTC) to process new data ingested into the source tables.

2.  **Job: `dbt_run_and_test`**:
    -   **`runs-on: ubuntu-latest`**: The job runs on a fresh, virtual Linux environment provided by GitHub.
    -   **`actions/checkout@v3`**: The first step checks out the repository's code so the workflow can access the dbt project files.
    -   **`actions/setup-python@v4`**: It installs the specified version of Python, which is a prerequisite for dbt Core.
    -   **`Install dbt and adapter`**: This step uses `pip` to install `dbt-snowflake`, the specific adapter needed to connect to Snowflake.
    -   **`Install dbt packages`**: It runs `dbt deps` to download the dependencies defined in `packages.yml` (like `dbt_utils` and `dbt_expectations`).
    -   **`Run dbt models`**: It executes `dbt run`. The connection to Snowflake is established using environment variables.
    -   **`Test dbt models`**: If `dbt run` succeeds, it executes `dbt test` to validate the data quality of the newly built models. If any test fails, the entire workflow fails, preventing bad data from being deployed.

## Secrets and Credentials

Snowflake credentials (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`) are not hardcoded. They are stored securely in **GitHub Secrets**. The workflow file references these secrets using the `${{ secrets.VARIABLE_NAME }}` syntax. The `profiles.yml` file is configured to read these credentials from environment variables, ensuring a secure and portable setup.