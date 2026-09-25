# IDX Market Data Scraper

Automated daily Level-3 order book and broker summary scraper for the Indonesia Stock Exchange (IDX).

This repository runs automatically every weekday at 19:00 WIB (12:00 UTC) via GitHub Actions, scraping Level-3 queue depth and broker summary data for 332 equities, IHSG composite index, and global macro indicators. The output Parquet date partitions are synced directly to Google Drive (`idx-mbo-data`).

---

## Architecture Overview

- **Runner**: Public GitHub Actions on `ubuntu-latest` (utilizing unlimited public runner minutes).
- **Storage Target**: Google Drive remote `gdrive:idx-mbo-data/` via `rclone`.
- **Security**: Zero cleartext credentials in code; authentication managed via encrypted GitHub Secrets.

---

## Required GitHub Secrets

Configure the following secrets under **Settings > Secrets and variables > Actions**:

| Secret Name | Description |
|---|---|
| `STOCKBIT_TOKEN` | Bearer JWT authentication token for Stockbit API access |
| `RCLONE_CONFIG_DATA` | Base64-encoded `rclone.conf` configured for Google Drive remote `gdrive:` |

---

## Automated Token Synchronization

Synchronize or refresh your Stockbit authentication token and Rclone configuration automatically using `scripts/update_token.py`:

```bash
# synchronize token and rclone configuration to github actions secrets
python scripts/update_token.py --token "<your-jwt-token>" --sync-github --sync-rclone --github-repo popboat1/idx-scraper
```

---

## Pulling Data Locally

To pull scraped date partitions from Google Drive to your local workstation:

```bash
# sync historical parquet partitions from google drive to local folder
rclone copy gdrive:idx-mbo-data/ idx_data/ -P
```

---

## Local Execution

Run the batch fetcher manually on your workstation:

```bash
# install dependencies
pip install -r requirements.txt

# set authentication token
export STOCKBIT_TOKEN="Bearer <your-token>"

# run one-shot scrape for today
python scripts/batch_fetcher.py --once --concurrency 4
```
