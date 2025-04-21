### `src/marts/`

# Prefect-Driven Data Mart Curation Layer

This folder contains Prefect.io flows responsible for **building intermediate and analytical data marts**. These flows extract cleaned data from the **staging schema** and load curated records into the **`marts` schema** for reporting, dashboarding, and downstream ML consumption.

<br>

---

<br>

## Contains

### Current Demo Marts

- `mart_datto_rmm_devices.py` — Curates Datto RMM device records into a production-grade analytical table

Each mart script defines a flow that reads from PostgreSQL `staging`, transforms if needed, and writes to the `marts` schema under a controlled table structure.

<br>

---

<br>

## 📂 Purpose

The `marts` layer is responsible for:

- **Consolidating staging-level tables** into business-ready, query-optimized formats
- Ensuring **data quality, type safety, and schema integrity**
- Writing curated outputs to the **PostgreSQL `marts` schema**
- Supporting **BI tools, dashboards, and ML pipelines** with stable structures

<br>

---

<br>

## Role in the ETL Pipeline

This folder represents the **data modeling and delivery zone** of your pipeline:

- Sits **downstream of staging**, using normalized ETL output
- Loads **versioned, audit-capable tables**
