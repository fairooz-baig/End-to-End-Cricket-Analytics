# End-to-End Cricket Analytics Pipeline

A complete **Data Engineering + Analytics** project on IPL and Cricket match data using Databricks.

I built this project to demonstrate a full data pipeline — from raw ingestion to insightful dashboards — using modern Databricks tools.
 

---

## Project Overview

This project processes historical cricket match data (2008–2020), builds a reliable data pipeline using Delta Live Tables (DLT), and delivers clean, analytics-ready data with interactive dashboards.

From messy JSON files → Bronze → Silver → Gold layer, and finally beautiful dashboards — everything is automated and production-ready.

---

##  Key Features

- Automated Data Ingestion from JSON files using Databricks Auto Loader
- Delta Live Tables Pipeline (Bronze → Silver → Gold)
- Proper schema enforcement and data quality checks
- Rich transformations (winner logic, season, toss impact, etc.)
- SCD Type 1 implementation for streaming updates
- Ready for visualization in Databricks Lakeview Dashboards
- Secure handling of API keys (using secrets)

---

## Tech Stack

- Databricks (Unity Catalog + Delta Live Tables)
- PySpark
- Delta Lake
- SQL
- Databricks Asset Bundles (for CI/CD deployment)
- Lakeview Dashboards

---

##  How to Run

1. Clone the repository
2. Update `databricks.yml` with your workspace details
3. Deploy using Databricks Asset Bundles:
   ```bash
   databricks bundle deploy --target dev

---
## What You Can Analyze

- Most successful IPL teams
- Toss impact on match results
- Venue-wise performance
- Head-to-head records
- Player of the Match trends
- Win by runs vs wickets patterns

---
## Author
Fairooz Baig ->  https://github.com/fairooz-baig
