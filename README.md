# ETL-framework 🏗️

A demo ETL framework emphasising high-standard data engineering practices: CI/CD with GitHub Actions, medallion layering, testing, data quality, documentation, and Infrastructure as Code with DABS.

## Principles & Practices🚦
### Medallion Architecture (Bronze → Silver → Gold) 🪙 
- Structured layering from raw ingestion to clean and consumable datasets. Bronze stores raw, Silver applies transformations and joins, and Gold produces analytics-ready outputs. This enables traceability and maintainability.

### Data Quality 🧪
- Managed via metadata/data_quality_checks.py and SQL checks in check_failures.sql & display_failures.sql. Alerts are automated via data_quality_alerts.yml job. 
- Uses Databricks DQX (Data Quality eXtended) to define expectation-based rules (completeness, uniqueness, patterns, range) at both row and column levels, enabling flexible validation pipelines.
- Track table health over time using Lakehouse Monitoring’s profile and drift metrics tables.
- Configure Databricks SQL alerts on DQ failures.

### CI / CD with GitHub Actions & Databricks Asset Bundles 🔁
- Run unit tests & linting on pull requests.
- Validate Databricks Asset Bundle YAML to catch infra/configuration errors early.
- Use GitHub Actions to deploy bundles to a dev environment automatically on pushes to main.
- Support manual, SHA-controlled deployment to prod environments.
- Treat codebase and infra definitions as Infrastructure-as-Code (IaC) using DAB YAML definitions.
- Isolate environments (dev, prod) using environment-specific DAB targets and variable substitution.
- Trigger post-deployment actions like running DDL operations.
- Auto-build and deploy Sphinx documentation to GitHub Pages on pushes to main, ensuring public docs stay current.
- Leverage Git commit SHA or semantic version tags for traceability and rollback capabilities.
- Promote to prod upon successful dev deployments.

### Testing & Code Quality 🧪
- Includes unit tests and integration tests, validating transforms, quality logic, and infrastructure using Pytest.
- Enforces linting with Ruff and static typing with MyPy.

### Documentation 📚
- Fully Sphinx-documented (docs/) with autodoc configuration.
- CI pipeline auto-deploys docs to GitHub Pages, ensuring public-facing documentation reflects the codebase.

### Infrastructure as Code (DABS) 🧱
- Databricks infrastructure automation configured with DABs.
- Uses databricks/variables.yml and resources.yml to configure DAB bundles, cluster specs, secrets, jobs, and workspace paths.
- DABS integrates with GitHub Actions to validate and deploy these YAML configurations automatically.

### Software Engineering Standards 💻
- Structured configuration (settings.py), enum-managed constants (enums.py), and centralized logging (logger.py) enforce maintainability and readability.
- Abstractions like DeltaTable and DeltaWriter isolate complexity and support reusable logic across pipelines.

### Security & Governance 🔐
- Sensitive credentials (e.g. API keys, database passwords) are never hardcoded and are securely managed via GitHub secrets. Secrets are injected at runtime using environment variables or cluster scopes.
- Tables are defined and registered within Unity Catalog, enabling centralized data governance across workspaces. This allows for fine-grained access controls, lineage tracking, and auditability.
- Permissions are set at the catalog, schema, and table level to restrict access based on least privilege principles (e.g. only Silver and Gold consumers have read access; only ETL jobs have write access to Bronze and Silver layers).
- Unity Catalog automatically tracks table lineage and access history, supporting traceability for all medallion-layered tables.




*Note: This is an ongoing demo framework. Some elements are partially implemented or stubbed for demonstration purposes.*
