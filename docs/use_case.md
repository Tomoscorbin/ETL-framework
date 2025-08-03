# Instacart Analytics Dashboard Use Case

## Business Context

Category managers and supply chain analysts have requested a dashboard that highlights product performance and ordering behaviour for an online grocery platform. The goals are to identify which products drive repeat purchases, evaluate promotional effectiveness and monitor how sales trends vary by department and by day of the week.

## Source Data

The demo leverages the public [Instacart Online Grocery Dataset](https://www.kaggle.com/datasets/yasserh/instacart-online-grocery-basket-analysis-dataset) as a stand‑in for real transactional data. The dataset includes:

- Customer orders with timestamps
- Product catalogue information (product, aisle and department metadata)
- Reorder indicators and sequence numbers

These files are ingested from raw CSV into the Lakehouse using the Databricks File System (DBFS) to simulate landing data from an operational store.
Because ingestion isn't a priority for this demo, the ingestion component is intentionally lightweight and not production-ready.

## Engineering Goals

This repository demonstrates how a production‑ready data pipeline could be delivered using modern engineering practices:

- **Medallion architecture** to build bronze, silver and gold layers with clear contracts between them.
- **Dimensional modeling** to shape the gold layer into a relational star schema following Kimball principles.
- **Automated data quality** checks that stop bad data from progressing through the pipeline.
- **CI/CD with Databricks Asset Bundles** so infrastructure and code are version‑controlled and deployed together.
- **Comprehensive testing and type checking** via `pytest`, `ruff` and `mypy`.
- **Automated documentation** built with Sphinx and published via GitHub Pages.

## Pipeline Overview

1. **Ingestion (Bronze):** Raw Instacart CSV files are copied to DBFS and written to Delta tables without transformation.
2. **Cleansing and Enrichment (Silver):** Records are validated, cleaned and enriched with additional attributes such as aisle and department names.
3. **Aggregation (Gold):** Business‑ready tables such as daily product sales or promotion performance are produced for direct consumption by downstream analytics tools.

## Data Model

The gold layer organises data into a relational star schema based on Kimball dimensional modelling. Fact tables capture measurable events like orders or order_items, while dimension tables provide descriptive context for products, customers and time. This structure supports conformed dimensions and efficient joins for analytics.

## Dashboard Delivery

The gold tables are intended to power a SQL‑based dashboard that exposes metrics like top‑selling products, repeat purchase rates and the effect of promotions over time. While the dashboard itself is out of scope for this repo, the data model and pipeline are structured so that the final tables could be queried by BI tools such as Power BI, Tableau or Databricks SQL.

## Extensibility

Although simplified for demonstration purposes, the framework can be extended to incorporate real‑world features such as streaming ingestion, slowly changing dimensions or additional data quality rules. It aims to provide a strong foundation for engineers tasked with delivering reliable analytics datasets for business stakeholders.

