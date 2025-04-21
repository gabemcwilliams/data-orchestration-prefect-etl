# `staging/api/<product name>/src/` - Prefect-Driven API Orchestration

This module contains ETL integrations for third-party APIs used in Prefect.io flows. It organizes API-based extraction, transformation, and loading workflows into a clear structure by vendor/source.

---

## Contains

### Current Demo Folders
- `datto_rmm/` — End-to-end ETL pipeline for Datto RMM API data
- `end_of_life_date/` — Integration for OS end-of-life API (from endoflife.date)

---

## 📂 Structure and Responsibilities
Each vendor/source module (e.g., `datto_rmm`) contains the following standardized subfolders:

| Folder | Purpose |
|--------|---------|
| `extract/` | API clients and extractors. Encapsulate auth, pagination, and data normalization into pandas DataFrames. |
| `transform/` | Applies column logic, parsing, filtering, and structure transformations specific to each dataset. |
| `load/` | Uploads data to MinIO and/or PostgreSQL. Handles serialization and schema alignment. |
| `config/` | Contains YAML configurations per endpoint, defining secrets, timestamps, metadata, and staging destinations. |
| `utilities/` | Shared tools such as Vault integration, task preparation, and OTP/MFA handling. |

---

## Execution
Each dataset has an associated `stg_api_*.py` file in the `src` root of its module (e.g., `stg_api_datto_rmm_devices.py`). These Prefect flows:

1. Extract API data
2. Transform the result into a clean schema
3. Load into both:
   - MinIO (as Parquet or JSON)
   - PostgreSQL (as relational tables)

Deployment metadata is stored in sibling `.yaml` files for orchestration.

---

## Status
This structure is designed for:
- Secure, secrets-based ingestion
- Modular development across API endpoints
- Scalable integration with Prefect orchestration

Further vendors or sources should follow the same pattern for consistency.

---

If you're adding a new source, copy an existing module like `datto_rmm`, modify configs, and add your extract/transform/load logic accordingly.