#!/bin/bash

###############################################################################
# etl_pipeline.sh
#
# Simple ETL pipeline for the NZ Stats "Annual Enterprise Survey 2023" CSV dataset.
#
# Objective:
#   EXTRACT   -> download source CSV into ./raw
#   TRANSFORM -> rename Variable_code -> variable_code,
#                select [year, Value, Units, variable_code], write to
#                ./Transformed/2023_year_finance.csv
#   LOAD      -> idempotently merge only *new* rows into
#                ./Gold/2023_year_finance.csv (safe to re-run on a schedule)
#
# All configuration (paths + CSV_URL) lives in config/config.sh, kept
# out of this file for readability.
#
# Design goals:
#   - fail fast & loud (set -Eeuo pipefail)
#   - never trust the network: retry, verify HTTP status, verify non-empty,
#     sanity-check shape of the file before using it
#   - never trust a previous step blindly: explicitly check each artifact
#     exists before the next stage consumes it
#   - atomic writes only (write to temp file, then mv into place) so a
#     crash mid-run never leaves a half-written file behind
#   - single-instance execution via flock (important once this runs on cron)
#   - idempotent load: safe to re-run daily against an unchanged or
#     appended source file without creating duplicate rows
###############################################################################

set -Eeuo pipefail
IFS=$'\n\t'

# ==============================================================================
# LOAD CONFIG
# All paths and CSV_URL come from config/config.sh - nothing here is inline.
# ==============================================================================

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# shellcheck disable=SC1091
source "${PROJECT_ROOT}/config/config.sh"

mkdir -p "$RAW_DIR" "$TRANSFORMED_DIR" "$GOLD_DIR" "$LOG_DIR"

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="${LOG_DIR}/etl_${TIMESTAMP}.log"


# ==============================================================================
# LOGGING HELPERS & LOG RETENTION
# ==============================================================================

log()  { 
    printf '%s [%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1" "$2" | tee -a "$LOG_FILE" >&2; 
}
info() { log "INFO"  "$1"; }
warn() { log "WARN"  "$1"; }
err()  { log "ERROR" "$1"; }

# Prune anything older than LOG_RETENTION_DAYS on every run rather than
pruned="$(find "$LOG_DIR" -name 'etl_*.log' -mtime "+${LOG_RETENTION_DAYS}" -print -delete | wc -l | tr -d ' ')"
[[ "$pruned" -gt 0 ]] && info "Pruned ${pruned} log file(s) older than ${LOG_RETENTION_DAYS} days"


# ==============================================================================
# ERROR HANDLING & CLEANUP
# ==============================================================================

declare -a TMP_FILES=()
cleanup() {
  local ec=$?
  local f
  for f in "${TMP_FILES[@]:-}"; do
    [[ -n "$f" && -e "$f" ]] && rm -f "$f"
  done
 
  if [[ -n "${LOCK_DIR:-}" && -d "$LOCK_DIR" ]]; then
    local lock_owner
    lock_owner="$(cat "${LOCK_DIR}/pid" 2>/dev/null || echo '')"
    [[ "$lock_owner" == "$$" ]] && rm -rf "$LOCK_DIR"
  fi
  if [[ $ec -ne 0 ]]; then
    err "Pipeline exited with status ${ec}. See ${LOG_FILE} for details."
  fi
}

trap cleanup EXIT
trap 'err "Failed at line ${LINENO}: ${BASH_COMMAND}"' ERR


# ==============================================================================
# SINGLE-INSTANCE LOCK (for cron)
# ==============================================================================

LOCK_DIR="${LOCK_FILE}.d"

if mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "$$" > "${LOCK_DIR}/pid"
  info "Acquired lock ${LOCK_DIR} (pid $$)"
else
  held_by="$(cat "${LOCK_DIR}/pid" 2>/dev/null || echo '')"
  if [[ -n "$held_by" ]] && ! kill -0 "$held_by" 2>/dev/null; then
    warn "Stale lock at ${LOCK_DIR} (pid ${held_by} is no longer running) - reclaiming"
    rm -rf "$LOCK_DIR"
    mkdir "$LOCK_DIR" && echo "$$" > "${LOCK_DIR}/pid"
    info "Acquired lock ${LOCK_DIR} (pid $$)"
  else
    err "Another instance of ${0##*/} is already running (pid ${held_by:-unknown}, lock: ${LOCK_DIR}). Exiting."
    exit 1
  fi
fi


# ==============================================================================
# VALIDATE REQUIRED CONFIG
# ==============================================================================
: "${CSV_URL:?CSV_URL is not set. Check config/config.sh.}"
info "Config validated - CSV_URL is set. Moving on."


# ==============================================================================
# EXTRACT
# ==============================================================================
info "===== STEP 1/3: EXTRACT ====="
info "Starting EXTRACTION phase from remote source..."

DOWNLOAD_TMP="$(mktemp "${RAW_DIR}/.download.XXXXXX")"
TMP_FILES+=("$DOWNLOAD_TMP")

HTTP_STATUS="$(curl \
  --silent --show-error --location --fail \
  --retry 3 --retry-delay 5 --retry-connrefused \
  --connect-timeout 15 --max-time 300 \
  --output "$DOWNLOAD_TMP" \
  --write-out '%{http_code}' \
  "$CSV_URL")" || { err "curl failed to reach CSV_URL after retries"; exit 1; }

if [[ "$HTTP_STATUS" != "200" ]]; then
  err "Unexpected HTTP status ${HTTP_STATUS} when downloading source file"
  exit 1
fi
info "HTTP status check passed (200). Moving on."

# Verify data existence and physical footprint footprint (>0 bytes)
if [[ ! -s "$DOWNLOAD_TMP" ]]; then
  err "Downloaded file is empty - refusing to proceed"
  exit 1
fi
info "Non-empty check passed. Moving on."

# Verifiy the payload is a CSV file
if ! head -n 1 "$DOWNLOAD_TMP" | grep -q ','; then
  err "Downloaded file does not look like CSV (no comma found in header row)"
  exit 1
fi
info "CSV shape check passed (comma-delimited header found). Moving on."

mv -f "$DOWNLOAD_TMP" "$RAW_FILE"

# Explicit confirmation the file is genuinely present, as required by spec.
if [[ -f "$RAW_FILE" && -s "$RAW_FILE" ]]; then
  RAW_SIZE="$(du -h "$RAW_FILE" | cut -f1)"
  RAW_SHA="$(sha256sum "$RAW_FILE" | awk '{print $1}')"
  info "CONFIRMED: raw file saved -> ${RAW_FILE} (size: ${RAW_SIZE}, sha256: ${RAW_SHA})"
else
  err "Raw file missing or empty after download - aborting"
  exit 1
fi


# ==============================================================================
# TRANSFORM
# ==============================================================================
# echo "" > $LOG_FILE
info "===== STEP 2/3: TRANSFORM ====="
info "Starting TRANSFORMATION phase..."

# Guardrail: never assume the previous step succeeded silently - re-check.
if [[ ! -f "$RAW_FILE" ]]; then
  err "Raw file not found at ${RAW_FILE} - cannot transform. Aborting."
  exit 1
fi

if [[ ! -s "$RAW_FILE" ]]; then
  err "Raw file at ${RAW_FILE} is empty - cannot transform. Aborting."
  exit 1
fi
info "Raw CSV file already exists at ${RAW_FILE} and is non-empty - moving on."

TRANSFORM_TMP="$(mktemp "${TRANSFORMED_DIR}/.transform.XXXXXX")"
TMP_FILES+=("$TRANSFORM_TMP")

if ! awk -F',' '
  NR==1 {
    for (i=1; i<=NF; i++) {
      col=$i
      gsub(/\r/,"",col)
      idx[col]=i
    }
    required["Year"]=1; required["Value"]=1; required["Units"]=1; required["Variable_code"]=1
    for (r in required) {
      if (!(r in idx)) {
        print "Missing required column: " r > "/dev/stderr"
        exit 2
      }
    }
    print "year,Value,Units,variable_code"
    next
  }
  {
    print $idx["Year"] "," $idx["Value"] "," $idx["Units"] "," $idx["Variable_code"]
  }
' "$RAW_FILE" > "$TRANSFORM_TMP"; then
  err "Transform failed - required column(s) not found in source CSV header"
  exit 1
fi
info "Column mapping check passed (Year, Value, Units, Variable_code all found). Moving on."

LINE_COUNT="$(wc -l < "$TRANSFORM_TMP" | tr -d ' ')"
if [[ "$LINE_COUNT" -le 1 ]]; then
  err "Transformed output has no data rows (only header, or empty) - aborting"
  exit 1
fi
info "Row-count check passed (${LINE_COUNT} lines incl. header). Moving on."

mv -f "$TRANSFORM_TMP" "$TRANSFORMED_FILE"

if [[ -f "$TRANSFORMED_FILE" && -s "$TRANSFORMED_FILE" ]]; then
  info "CONFIRMED: transformed file saved -> ${TRANSFORMED_FILE} (${LINE_COUNT} lines incl. header)"
else
  err "Transformed file missing or empty after transform step - aborting"
  exit 1
fi


# ==============================================================================
# LOAD (idempotent merge into Gold)
# ==============================================================================
info "===== STEP 3/3: LOAD ====="
info "Starting IDEMPOTENT LOAD phase..."

if [[ ! -f "$TRANSFORMED_FILE" ]]; then
  err "Transformed file not found at ${TRANSFORMED_FILE} - cannot load. Aborting."
  exit 1
fi
info "Transformed file already exists at ${TRANSFORMED_FILE} - moving on to load."

if [[ ! -f "$GOLD_FILE" ]]; then
  # First-ever load: Gold doesn't exist yet, so every data row is new.
  cp "$TRANSFORMED_FILE" "${GOLD_FILE}.tmp"
  mv -f "${GOLD_FILE}.tmp" "$GOLD_FILE"
  added="$(( $(wc -l < "$GOLD_FILE" | tr -d ' ') - 1 ))"
  skipped=0
  info "Gold file did not exist yet - initialized with header + ${added} row(s)"
else
  NEW_ROWS_TMP="$(mktemp "${GOLD_DIR}/.newrows.XXXXXX")"
  TMP_FILES+=("$NEW_ROWS_TMP")

  # Set-difference: data rows present in the fresh transform but not
  # already present in Gold. `comm` requires both inputs pre-sorted.
  comm -23 \
    <(tail -n +2 "$TRANSFORMED_FILE" | sort) \
    <(tail -n +2 "$GOLD_FILE" | sort) \
    > "$NEW_ROWS_TMP"

  added="$(wc -l < "$NEW_ROWS_TMP" | tr -d ' ')"
  current_rows="$(tail -n +2 "$TRANSFORMED_FILE" | wc -l | tr -d ' ')"
  skipped="$(( current_rows - added ))"

  if [[ "$added" -eq 0 ]]; then
    info "No new rows detected vs. Gold. Gold layer already up to date - nothing to do."
  else
    info "Appending ${added} new row(s) to existing Gold file (skipping ${skipped} already-loaded row(s))"
    cp "$GOLD_FILE" "${GOLD_FILE}.tmp"
    cat "$NEW_ROWS_TMP" >> "${GOLD_FILE}.tmp"
    mv -f "${GOLD_FILE}.tmp" "$GOLD_FILE"
  fi
fi

if [[ -f "$GOLD_FILE" && -s "$GOLD_FILE" ]]; then
  GOLD_LINES="$(wc -l < "$GOLD_FILE" | tr -d ' ')"
  info "CONFIRMED: Gold file present -> ${GOLD_FILE} (${GOLD_LINES} lines incl. header)"
else
  err "Gold file missing or empty after load step - aborting"
  exit 1
fi

info "ETL run complete. rows_added=${added}, rows_skipped_existing=${skipped}"
info "Log written to ${LOG_FILE}"