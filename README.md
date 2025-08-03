# ETL-framework 🏗️

A demo ETL framework designed to showcase high-quality data engineering principles and practices. This project isn’t production-grade - it’s a reference implementation that brings together key patterns like medallion architecture, data quality enforcement, CI/CD with GitHub Actions, Infrastructure as Code (IaC) using Databricks Asset Bundles (DABs), automated documentation, and more. It's intended as a learning tool and best practice guide, demonstrating how to build reliable, maintainable, and secure data pipelines using modern tooling and engineering standards.

The pipeline is simulating a real-world online grocery analytics scenario: The business has asked for a dashboard to help category managers and supply chain analysts make better decisions around product placement, replenishment, and promotional effectiveness. They want to answer questions like:
- Which products drive the most repeat purchases and should be prioritised?
- Which promotions are actually increasing reorder rates or basket sizes?
- How do sales patterns vary across departments and days of the week?

We are using a Kaggle [Instacart Online Grocery dataset](https://www.kaggle.com/datasets/yasserh/instacart-online-grocery-basket-analysis-dataset) as a proxy for real transactional source data from an online grocer. See [Use Case](docs/use_case.md) for more info.


## Principles & Practices🚦
### Medallion Architecture (Bronze → Silver → Gold) 🪙 
- Structured layering from raw ingestion to clean and consumable datasets. Bronze stores raw, Silver applies transformations and joins, and Gold produces analytics-ready outputs. This enables traceability and maintainability.

### Data Quality 🧪
- Automated DQ framework that warns or blocks bad data from writing to tables.
- Leverages Databricks DQX to define expectation-based rules (e.g. completeness, uniqueness, patterns, ranges) at both row and column levels, enabling flexible validation pipelines.
- Table health is continuously monitored using Lakehouse Monitoring, with support for profiling and drift detection over time.
- Failures trigger Databricks SQL alerts, ensuring awareness of ata quality issues.

### Data Modeling 🧩
- Organizes the gold layer into relational star schemas using Kimball's dimensional modeling approach.
- Separates fact tables from dimension tables and promotes conformed dimensions for consistent analytics.

### CI / CD with GitHub Actions & Databricks Asset Bundles 🔁
- Run unit tests & linting on pull requests.
- Validate Databricks Asset Bundle YAML to catch infra/configuration errors early.
- Use GitHub Actions to deploy bundles to a dev environment automatically on pushes to main.
- Support manual, SHA-controlled deployment to prod environments.
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
- Databricks infrastructure is managed using DABS (Databricks Asset Bundles) for repeatable, declarative deployments.
- Configurations define clusters, secrets, jobs, and workspace assets using YAML.
- DABS integrates with GitHub Actions for automated validation and deployment as part of the CI/CD workflow.

### Software Engineering Standards 💻
- Configuration, constants, and logging are structured and centralized to promote maintainability, clarity, and consistent behavior across the codebase.
- Abstractions like DeltaTable and DeltaWriter isolate complexity and support reusable logic across pipelines.

### Security & Governance 🔐
- Sensitive credentials (e.g. API keys, database passwords) are never hardcoded and are securely managed via GitHub secrets. Secrets are injected at runtime using environment variables or cluster scopes.
- Tables are defined and registered within Unity Catalog, enabling centralized data governance across workspaces. This allows for fine-grained access controls, lineage tracking, and auditability.
- Permissions are set at the catalog, schema, and table level to restrict access based on least privilege principles (e.g. consumers only have read access to Gold; only SPNs or authorised users have write access).
- Unity Catalog automatically tracks table lineage and access history, supporting traceability for all medallion-layered tables.




*Note: This is an ongoing demo framework. Some elements-like ingestion-are partially implemented or stubbed for demonstration purposes.*
