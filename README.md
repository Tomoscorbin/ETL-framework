# ETL-framework

A demo ETL framework emphasising high-standard data engineering practices: CI/CD with GitHub Actions, medallion layering, testing, data quality, documentation, and Infrastructure as Code with DABS.

# #🚦 Principles & Practices
### 1. Medallion Architecture (Bronze → Silver → Gold)
Structured layering from raw ingestion to clean and consumable datasets. Bronze stores raw, Silver applies transformations and joins, and Gold produces analytics-ready outputs. This enables traceability and maintainability.

### 2. Data Quality
Managed via metadata/data_quality_checks.py and SQL checks in check_failures.sql & display_failures.sql. Alerts are automated via data_quality_alerts.yml job. 
Uses Databricks DQX (Data Quality eXtended) to define expectation-based rules (completeness, uniqueness, patterns, range) at both row and column levels, enabling flexible validation pipelines.
Track table health over time using Lakehouse Monitoring’s profile and drift metrics tables.
Configure Databricks SQL alerts on DQ failures.

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
Includes unit tests and integration tests, validating transforms, quality logic, and infrastructure using Pytest.
Enforces linting with Pylint and static typing with MyPy.

### 5. Documentation
Fully Sphinx-documented (docs/) with autodoc configuration.
CI pipeline auto-deploys docs to GitHub Pages, ensuring public-facing documentation reflects the codebase.

### 6. Infrastructure as Code (DABS)
Databricks infrastructure automation configured with DABs.
Uses databricks/variables.yml and resources.yml to configure DAB bundles, cluster specs, secrets, jobs, and workspace paths.
DABS integrates with GitHub Actions to validate and deploy these YAML configurations automatically.

### 7. Software Engineering Standards
Structured configuration (settings.py), enum-managed constants (enums.py), and centralized logging (logger.py) enforce maintainability and readability.
Abstractions like DeltaTable and DeltaWriter isolate complexity and support reusable logic across pipelines.


*Note: This project is an evolving demo framework. Some elements are partially implemented or stubbed for demonstration purposes.*
