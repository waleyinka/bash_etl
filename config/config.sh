#!/usr/bin/env bash

###############################################################################
# config.sh
#
# For every filesystem path the pipeline touches (just for simplicity & readability)
#
# Contract: the caller must set PROJECT_ROOT before sourcing this file,
# e.g.:
#   PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
#   source "${PROJECT_ROOT}/config/config.sh"
###############################################################################


# ==============================================================================
# etl_pipeline.sh
# ==============================================================================

: "${PROJECT_ROOT:?config.sh requires PROJECT_ROOT to be set before it is sourced}"

# --- data source -------------------------------------------------------------
CSV_URL="https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2023-financial-year-provisional/Download-data/annual-enterprise-survey-2023-financial-year-provisional.csv"


# --- data layer directories -------------------------------------------------
RAW_DIR="${PROJECT_ROOT}/raw"
TRANSFORMED_DIR="${PROJECT_ROOT}/Transformed"
GOLD_DIR="${PROJECT_ROOT}/Gold"
LOG_DIR="${PROJECT_ROOT}/logs"


# --- specific files ---------------------------------------------------------
RAW_FILE="${RAW_DIR}/annual-enterprise-survey-2023-financial-year-provisional.csv"
TRANSFORMED_FILE="${TRANSFORMED_DIR}/2023_year_finance.csv"
GOLD_FILE="${GOLD_DIR}/2023_year_finance.csv"


# --- runtime -----------------------------------------------------------------
# Single-instance lock so cron can never run two overlapping copies.
LOCK_FILE="/tmp/bash_etl_pipeline.lock"

# How many days of per-run logs to keep before they're pruned automatically.
LOG_RETENTION_DAYS=30





# ==============================================================================
# move_csv_json.sh
# ==============================================================================

# --- Pipeline Folder Paths ----------------------------------
SRC_DIR="./source_folder"
DEST_DIR="./json_and_csv"