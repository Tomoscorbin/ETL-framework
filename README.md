# ETL-framework

A demo ETL framework emphasising high-standard data engineering practices: CI/CD with GitHub Actions, medallion layering, testing, data quality, documentation, and Infrastructure as Code with DABS.

# #🚦 Principles & Practices
### 1. Medallion Architecture (Bronze → Silver → Gold)
Structured layering from raw ingestion to clean and consumable datasets. Bronze stores raw, Silver applies transformations and joins, and Gold produces analytics-ready outputs. This enables traceability and maintainability.

### 2. Data Quality Framework
Managed via metadata/data_quality_checks.py and SQL checks in check_failures.sql & display_failures.sql. Alerts are automated via data_quality_alerts.yml job. Databricks' DQX checks ensure quality gates at each stage.

### 3. CI / CD with GitHub Actions & Databricks Asset Bundles
Run unit tests & linting on pull requests.
Validate Databricks Asset Bundle YAML to catch infra/configuration errors early.
Use GitHub Actions to deploy bundles to a dev environment automatically on pushes to main.
Support manual, SHA-controlled deployment to prod environments.
Treat codebase and infra definitions as Infrastructure-as-Code (IaC) using DAB YAML definitions.
Isolate environments (dev, prod) using environment-specific DAB targets and variable substitution.
Trigger post-deployment actions like running DDL operations.
Auto-build and deploy Sphinx documentation to GitHub Pages on pushes to main, ensuring public docs stay current.
Leverage Git commit SHA or semantic version tags for traceability and rollback capabilities.
Promote to prod upon successful dev deployments.

### 4. Testing & Code Quality
Unit tests & integration tests.
Linting and type checking.

### 5. Documentation
Fully Sphinx-documented (docs/) with autodoc configuration.

### 6. Infrastructure as Code (DABS)
Databricks infrastructure automation configured via databricks/variables.yml and resources.yml.

### 7. Software Engineering Standards
Consistent logging (logger.py), structured settings (settings.py), enumerated configs in enums.py. Layered abstractions: models manage table definitions and writers handle writes.
