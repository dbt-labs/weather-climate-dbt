# Claude Instructions for Weather Climate dbt Project

## Important Guidelines

### 1. Always Validate Models After Changes
- After creating or modifying any dbt model, ALWAYS run `dbt run --select <model_name>` to validate it compiles and runs successfully
- Use `dbt show --select <model_name>` to preview the data and ensure it looks as expected
- If there are errors, fix them before moving on to the next task
- Run `dbt test --select <model_name>` after the model runs successfully

### 2. Code Quality Checks
- Run `dbt compile` to check for syntax errors across all models
- Use `dbt run` to execute models and verify they produce expected results
- Use `dbt show` to preview data and validate transformations
- Use `dbt test` to run data quality tests

### 3. Data Validation Steps
1. Run the model: `dbt run --select <model_name>`
2. Preview the data: `dbt show --select <model_name>`
3. Test the model: `dbt test --select <model_name>`

### 4. Materialization Strategy
- Keep all models as views initially
- Before changing any model to a table, check with the user first
- Consider data volume, query performance, and refresh frequency when deciding on materialization