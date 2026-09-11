# CoreDataEngineers Data Infrastructure & ETL Pipeline

This repository contains an enterprise-grade, automated Bash ETL (Extract, Transform, Load) pipeline for financial survey data, complete with error handling, atomic writes, idempotency, automated log pruning, cron scheduling, and a file organization utility.

The pipeline automates the ingestion, transformation, and structured storage of the New Zealand Annual Enterprise Survey dataset. Engineered specifically for POSIX-compliant environments, it addresses core operational requirements: processing raw external files safely over the network, standardizing schema conventions across analytical layers, and maintaining a reliable, production-ready **Gold** storage that serves downstream business intelligence and analytics applications.

## Table of Contents
- [Directory Architecture](#directory-architecture)
- [Pipeline Overview](#pipeline-overview)
  - [1. Extract Phase](#1-extract-phase-raw_dir)
  - [2. Transform Phase](#2-transform-phase-transformed_dir)
  - [3. Load Phase](#3-load-phase-gold_dir)
- [Technical Features & Reliability Guarantees](#technical-features--reliability-guarantees)
- [Installation & Setup](#installation--setup)
  - [Prerequisites](#prerequisites)
  - [Step 1: Clone Repository & Setup Permissions](#step-1-clone-repository--setup-permissions)
  - [Step 2: Environment Configuration](#step-2-environment-configuration)
- [Usage Guide](#usage-guide)
  - [Running the ETL Pipeline Manually](#running-the-etl-pipeline-manually)
- [Utility Script: File Aggregation](#utility-script-file-aggregation-move_csv_jsonsh)
  - [Running the Utility](#running-the-utility)
  - [Script Execution Logic](#script-execution-logic)
- [Cron Job Automation](#cron-job-automation)
  - [Crontab Entry Configuration](#crontab-entry-configuration)
  - [How to Install the Cron Schedule](#how-to-install-the-cron-schedule)

## Directory Architecture

```
bash-etl-pipeline/
├── scripts/
│   ├── etl_pipeline.sh      # Extract -> Transform -> Load (logic only)
│   └── move_csv_json.sh     # Utility to consolidate CSV and JSON files
├── config/
│   └── config.sh            # ALL config: paths + CSV_URL, version-controlled
├── raw/                     # Extract landing zone (git-ignored contents)
├── Transformed/             # Transform output (git-ignored contents)
├── Gold/                    # Load target - single source of truth (git-ignored contents)
├── logs/                    # Timestamped run logs
├── .gitignore
└── README.md
```

## Pipeline Overview

1. Extract Phase (`RAW_DIR`)
 
 - Source: Configured via `CSV_URL` in `config/config.sh` (NZ Stats Annual Enterprise Survey 2023 dataset).

 - Execution:

   - Uses `curl` with exponential retries (`--retry 3`), timeouts, and connection failure resilience.

   - Validates HTTP response status (`200 OK`).

   - Asserts non-zero file footprint (`-s`).

   - Performs structure verification ensuring the download is a valid comma-delimited CSV.

 - Output: Stored at `raw/annual-enterprise-survey-2023-financial-year-provisional.csv`.


2. Transform Phase (`TRANSFORMED_DIR`)

 - Execution:

   - Re-verifies existence and integrity of the raw input asset.

   - Employs `awk` for high-performance streaming schema parsing.

   - Renames `Variable_code` to `variable_code`.

   - Filters and isolates required columns: `year`, `Value`, `Units`, `variable_code`.

   - Dynamic column-index lookup handles potential upstream schema position shifts seamlessly.

 - Output: Saved to `Transformed/2023_year_finance.csv`.

3. Load Phase (`GOLD_DIR`)

 - Execution:

   - Verifies availability of the transformed data asset.

   - Implements an idempotent merge engine utilizing Unix comm set-difference operations.

   - Evaluates row uniqueness against existing records in the Gold layer.

   - Appends only newly detected unique rows while preserving existing records.

 - Output: Maintained at `Gold/2023_year_finance.csv`.


## Technical Features & Reliability Guarantees

 - Strict Execution Guardrails: Written with `set -Eeuo pipefail` and `IFS=$'\n\t'` to catch unexpected errors, unbound variables, and broken pipe commands instantly.

 - Atomic Writes: All data transformations write to isolated, masked temporary files (`.XXXXXX`) before performing standard `mv` replacements. This prevents partially written or corrupted files during unexpected interrupts.

 - Concurrency Locking: Atomic directory lock strategy (`mkdir`) prevents race conditions or overlapping cron job instances. Recovers automatically from stale locks left behind by dead processes.

 - Audit Logging & Cleanup: Automated cleanup trap (`EXIT` signal) purges transient files on script completion or failure. Log files are automatically pruned after a configurable retention period (`LOG_RETENTION_DAYS=30`).


### Installation & Setup

### Prerequisites

 - Linux OS (Ubuntu, Debian, RHEL, CentOS, or similar).

 - standard utilities: `bash` (v4+), `curl`, `awk`, `coreutils` (`comm`, `sort`, `find`, `mktemp`).

 - `git` installed for version control.

### Step 1: Clone Repository & Setup Permissions

```bash
git clone https://github.com/CoreDataEngineers/bash-etl-pipeline.git
cd coredata-etl-pipeline

# Make scripts executable
chmod +x scripts/etl_pipeline.sh scripts/move_csv_json.sh
```

### Step 2: Environment Configuration

Inspect or edit `config/config.sh` to update paths or source URLs if needed:

```bash
# Display default parameters
cat config/config.sh
```

## Usage Guide

**Running the ETL Pipeline Manually**

Run the primary ETL pipeline directly from the repository root:

```bash
./scripts/etl_pipeline.sh
```

**Sample Terminal Output:**

```Plaintext
2026-09-10 12:00:00 [INFO] Config validated - CSV_URL is set. Moving on.
2026-09-10 12:00:00 [INFO] ===== STEP 1/3: EXTRACT =====
2026-09-10 12:00:00 [INFO] Starting EXTRACTION phase from remote source...
2026-09-10 12:00:02 [INFO] HTTP status check passed (200). Moving on.
2026-09-10 12:00:02 [INFO] Non-empty check passed. Moving on.
2026-09-10 12:00:02 [INFO] CSV shape check passed (comma-delimited header found). Moving on.
2026-09-10 12:00:02 [INFO] CONFIRMED: raw file saved -> /path/to/raw/annual-enterprise-survey-2023-financial-year-provisional.csv (size: 9.4M, sha256: ...)
2026-09-10 12:00:02 [INFO] ===== STEP 2/3: TRANSFORM =====
2026-09-10 12:00:02 [INFO] Starting TRANSFORMATION phase...
2026-09-10 12:00:03 [INFO] Column mapping check passed (Year, Value, Units, Variable_code all found). Moving on.
2026-09-10 12:00:03 [INFO] CONFIRMED: transformed file saved -> /path/to/Transformed/2023_year_finance.csv (5001 lines incl. header)
2026-09-10 12:00:03 [INFO] ===== STEP 3/3: LOAD =====
2026-09-10 12:00:03 [INFO] Starting IDEMPOTENT LOAD phase...
2026-09-10 12:00:03 [INFO] Gold file did not exist yet - initialized with header + 5000 row(s)
2026-09-10 12:00:03 [INFO] CONFIRMED: Gold file present -> /path/to/Gold/2023_year_finance.csv (5001 lines incl. header)
2026-09-10 12:00:03 [INFO] ETL run complete. rows_added=5000, rows_skipped_existing=0
```

## Utility Script: File Aggregation (move_csv_json.sh)

The repository includes a standalone utility script designed to scan a source directory and collect all `.csv` and `.json` files into a dedicated `json_and_CSV` folder.

### Running the Utility

```bash
./scripts/move_csv_json.sh
```

### Script Execution Logic

 - Evaluates target directory structures and creates json_and_CSV/ if missing.

 - Performs globbing checks for .csv and .json files within the designated SRC_DIR.

 - Handles single, multiple, or missing file scenarios without throwing system syntax errors.

 - Safely moves matched files into json_and_CSV/ and reports operational summary metrics.

## Cron Job Automation

As part of requirement, the pipeline is scheduled to run daily at **12:00 AM** (Midnight).

### Crontab Entry Configuration

View the cron definition file in `cron/crontab_entry.txt`:

```Plaintext
0 0 * * * /bin/bash /path/to/scripts/etl_pipeline.sh >> /path/to/logs/cron.log 2>&1
```

### How to Install the Cron Schedule

Open your user crontab editor:

```bash
crontab -e
```

Append the following absolute path entry (replace `/path/to/` with your system's absolute root path):

```bash
0 0 * * * /bin/bash /absolute/path/to/scripts/etl_pipeline.sh >> /absolute/path/to/logs/cron.log 2>&1
```

Save and exit the editor. Verify active **cron** registration:

```bash
crontab -l
```