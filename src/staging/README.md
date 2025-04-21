### `staging/`

# Prefect-Driven Data Extraction Layer

This folder contains Prefect.io ETL flows responsible for **extracting source data**, transforming it to a usable schema, and loading the results into both **raw object storage** (MinIO) and **staging database schemas** (PostgreSQL).

<br>

---

<br>

## Contains

### Current Demo Flow Groups

- `api/` — Flows for third-party integrations such as Datto RMM and end-of-life data APIs
- `frontend/` — Flows for frontend-sourced data such as asset views or scraped dashboards

Each subfolder represents a **source domain** and contains the flow scripts and deployment YAMLs associated with that domain.

<br>

---

<br>

## 📂 Purpose

The `staging` folder is responsible for:

- Running **source-specific Prefect flows**
- Coordinating **modular extract/transform/load logic** per integration
- Landing structured records in the **PostgreSQL staging schema**
- Persisting raw and transformed data into **MinIO object storage** (as Parquet or JSON)

Each flow operates independently and follows a standardized layout to ensure reliability and reusability across products.

<br>

---

<br>

## Role in the ETL Pipeline

This layer serves as the **data ingestion and landing zone** for the entire pipeline:

- **Entry point** for scheduled, versioned flows
- **Isolated per integration**, with clean folder separation
- **Downstream from `utils/` and config** folders
- **Upstream from enrichment, modeling, or ML inference layers**

<br>

---

<br>

To add a new source, copy an existing folder (e.g., `api/datto_rmm/`), define the extract/transform/load logic in `src/`, and create a `stg_*.py` entrypoint script under the new `staging/<domain>/` folder.
