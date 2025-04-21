# Front-End Extraction Modules

This folder contains extract classes and utilities that perform **front-end scraping or browser-simulated data retrieval** in cases where no public API exists or required data is only accessible via a web interface.

These modules are designed to integrate with the broader ETL architecture alongside API-based flows, using consistent return formats and shared configuration structures.

---

## Purpose

Some vendor platforms expose critical data only through browser interfaces, often requiring login forms, multi-step authentication, or dynamically generated content. This extraction layer supports those cases by:

- Emulating browser sessions using `requests` or headless tools
- Handling multi-step login flows, including MFA where needed
- Capturing data from authenticated endpoints or embedded assets (e.g., CSV exports)
- Returning results in a uniform `{"data": ..., "result": ...}` format for downstream transformation and loading

---

## Key Characteristics

- **Session-based login**: Emulates user login with cookies, CSRF tokens, and state persistence
- **MFA integration**: Supports TOTP-based MFA and configurable auth steps
- **Headless operation**: Uses `requests` or headless automation (e.g., Selenium if needed) to avoid GUI interaction
- **Structured output**: All flows return structured data with logging context, status codes, and error traces
- **Vault integration**: Secrets are injected securely from HashiCorp Vault using pre-configured mount points and paths
- **Tightly scoped**: Each module focuses on a single data domain (e.g., assets, tickets, alerts) per platform

---

## Output Format

All extract classes in this folder return a dictionary with the following structure:

```python
{
    "data": pd.DataFrame(...),
    "result": {
        "job_title": str,
        "status_code": int,
        "message": str
    }
}
