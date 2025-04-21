# Data Orchestration with Prefect ETL

This repository contains real-world Prefect pipelines for orchestrating ETL workflows across vendor APIs, front-end
scraping interfaces, and secure data delivery targets. It includes examples from structured data sources like Datto RMM
and EndOfLife.date, as well as browser-emulated flows like Scalepad.

All workflows are modular, secure (Vault-integrated), and designed for batch or real-time orchestration across client
environments.

---

# Features

- 100+ vendor-specific ETL routes implemented across structured APIs and browser-based workflows
- Modular Prefect flows, staged by vendor and domain
- Secrets managed via Vault and TOTP support
- Front-end scraping and headless session workflows (where APIs don't exist)
- Secure loaders for PostgreSQL and MinIO
- Reusable `task_prep` utilities with YAML config loading
- End-to-end testing with `pytest`
- Auto-generated `.docx` client-facing documents for lifecycle guidance

---

# 📁 Key Folder Layout

```plaintext
.
├── docs/                     # Vision documents and Vault policy
├── src/                      # All flow logic (deploy, extract, transform, load)
│   ├── staging/              # Source-level ETL: API, frontend
│   ├── marts/                # Analytical or ML-ready data marts
│   └── utils/                # Vault, OTP, secrets, config helpers
├── tests/                    # Unit test coverage using pytest
├── Dockerfile                # Optional container build
├── requirements.txt          # Main Python requirements
└── requirements_tests.txt    # Pytest and testing tools
```

---

# Example: Windows OS Lifecycle Reporting

- Fetches release metadata from [endoflife.date](https://endoflife.date/windows)
- Parses expiring and new builds using regex and pattern filters
- Generates `.docx` advisories for end-of-life Windows versions
- Used internally and externally to plan patching or decommissioning

---

# 🧪 Running Tests

```
pytest tests/staging/api/datto_rmm/unit/test_api_datto_rmm.py
pytest tests/staging/api/end_of_life_date/unit/test_api_end_of_life_date.py
```

---

## ▶️ Deployment Process

This project uses custom utility scripts to generate and register Prefect deployment YAMLs automatically:

### 1. Deployment YAML Generator

A script scans local source directories to identify all flow modules and outputs standardized Prefect `-deploy.yaml`
files with:

- Entrypoint auto-detection
- Custom scheduling, worker pool, and timezone support
- One YAML per flow file

Example use:

```
python src/deploy_all_prefect_builds.py
```

### 2. Automated Flow Deployment

A second script scans for all generated deployment YAMLs and deploys any not yet registered in Prefect Cloud or Server.
It supports:

- Automatic detection of active vs missing deployments
- SSL bypass support for internal or dev/test environments

```
python src/deploy_all_prefect_builds_INSECURE_CONTEXT.py --insecure
```

These tools support CI/CD pipelines and scale onboarding across dozens or hundreds of flows efficiently.python
python src/deploy_all_prefect_builds.py


### 2. Automated Flow Deployment

A second script scans for all generated deployment YAMLs and deploys any not yet registered in Prefect Cloud or Server.
Supports optional SSL bypass for internal testing environments.

```
python src/deploy_all_prefect_builds.py         # Generate all -deploy.yaml files
python src/deploy_all_prefect_builds_INSECURE_CONTEXT.py --insecure  # Deploy to Prefect
```

---

## Recommended Tools

- [Prefect](https://docs.prefect.io/)
- [Vault](https://www.vaultproject.io/) for secrets
- [MinIO](https://min.io/) or S3-compatible storage
- [PostgreSQL](https://www.postgresql.org/)

---

## 👤 Maintainer

Developed and maintained by [Gabe McWilliams](https://github.com/gabemcwilliams)
