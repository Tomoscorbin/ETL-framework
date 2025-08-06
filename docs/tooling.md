# Tooling Overview

This project combines several tools to maintain code quality, automate deployments and
process data efficiently.

## Data Processing

- **PySpark & Delta Lake** – the ETL jobs are written in PySpark and persist data in Delta tables
  for reliable, ACID-compliant storage.
- **Databricks Labs DQX** – expectation-based data quality checks stop bad data from progressing
  through the pipeline.

## Infrastructure & Deployment

- **Databricks Asset Bundles (DABs)** define clusters, jobs and other workspace assets as code.
  The `databricks.yml` bundle is validated and deployed through CI/CD.
- **GitHub Actions** run tests, linting and bundle validation on every pull request and push to
  main.

## Testing & Code Quality

- **Pytest** covers unit and integration tests under the `tests/` directory.
- **Ruff** enforces style and formatting rules and runs via the [`lint.sh`](../lint.sh) script.
- **MyPy** performs static type checking, also invoked from `lint.sh`.

## Documentation

- **Sphinx** with the MyST parser renders the Markdown files in `docs/` and builds the public
  documentation site.

