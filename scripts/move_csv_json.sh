#!/usr/bin/env bash

###############################################################################
# move_csv_json.sh
#
# Moves every .csv and .json file (case-insensitive extension) from a
# source folder into a destination folder (default: json_and_CSV).
# Works for one file or many; safe with filenames containing spaces.
#
# Usage:
#   ./move_csv_json.sh -s SOURCE_DIR [-d DEST_DIR]
#
# Example:
#   ./move_csv_json.sh -s ./demo_move/source_folder -d ./json_and_CSV
###############################################################################

set -Eeuo pipefail
IFS=$'\n\t'


# ==============================================================================
# LOAD CONFIG
# Resolve absolute path of the script directory cleanly
# ============================================================================== 
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# shellcheck disable=SC1091
source "${PROJECT_ROOT}/config/config.sh"

mkdir -p "$SRC_DIR" "$DEST_DIR"

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="${LOG_DIR}/etl_${TIMESTAMP}.log"



usage() {
  cat <<EOF
Usage: $(basename "$0") -s SOURCE_DIR [-d DEST_DIR]
  -s  Source directory to scan for *.csv / *.json files (required)
  -d  Destination directory (default: json_and_CSV; created if missing)
EOF
  exit 1
}

DEST_DIR="json_and_CSV"
SOURCE_DIR=""

while getopts ":s:d:h" opt; do
  case "$opt" in
    s) SOURCE_DIR="$OPTARG" ;;
    d) DEST_DIR="$OPTARG" ;;
    h) usage ;;
    *) usage ;;
  esac
done

[[ -z "$SOURCE_DIR" ]] && usage
if [[ ! -d "$SOURCE_DIR" ]]; then
  echo "ERROR: source directory '${SOURCE_DIR}' does not exist" >&2
  exit 1
fi

mkdir -p "$DEST_DIR"

log() { printf '%s [%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1" "$2"; }

moved=0

# find ... -print0 / read -d '' handles filenames with spaces or newlines
# safely - a plain `for f in $(find ...)` would break on those.
while IFS= read -r -d '' file; do
  base="$(basename "$file")"
  dest="${DEST_DIR}/${base}"

  # Never silently overwrite an existing file with the same name.
  if [[ -e "$dest" ]]; then
    dest="${DEST_DIR}/$(date +%Y%m%d%H%M%S)_${base}"
    log "WARN" "'${base}' already exists in destination - renaming to $(basename "$dest")"
  fi

  mv -- "$file" "$dest"
  log "INFO" "Moved: ${file} -> ${dest}"
  moved=$((moved + 1))
done < <(find "$SOURCE_DIR" -maxdepth 1 -type f \( -iname '*.csv' -o -iname '*.json' \) -print0)

if [[ "$moved" -eq 0 ]]; then
  log "INFO" "No .csv or .json files found in '${SOURCE_DIR}'. Nothing to move."
else
  log "INFO" "Done. Moved ${moved} file(s) into '${DEST_DIR}'."
fi







# Resolve absolute path of the script directory cleanly
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [$1] $2"
}

# Source configuration file parameters
if [ -f "$ENV_FILE" ]; then
    # shellcheck disable=SC1091
    source "$ENV_FILE"
else
    log "CRITICAL" "Aborting: Environment configuration profile (.env) missing."
    exit 1
fi

# Ensure directories exist securely before processing file streams
mkdir -p "$SRC_DIR" "$DEST_DIR"

# ==============================================================================
# 1. THE BULLETPROOF MOVE OPERATION
# ==============================================================================
log "INFO" "Scanning '$SRC_DIR' for targeting CSV and JSON file structures..."

# Guardrail: Enable nullglob so patterns that do not find a match expand to nothing
# instead of literal wildcard string loops (e.g. '*.csv')
shopt -s nullglob

# Collect matches into an array format to cleanly handle file names containing spaces
files_to_move=( "$SRC_DIR"/*.csv "$SRC_DIR"/*.json )

# Verify if array contains data objects
if [ ${#files_to_move[@]} -eq 0 ]; then
    log "INFO" "😴 Zero matching CSV or JSON records discovered in '$SRC_DIR'. Doing nothing."
    exit 0
fi

log "INFO" "Found ${#files_to_move[@]} target file(s) to process. Initiating transfer..."

# Guardrail Parameters on mv:
# --backup=numbered: If a file named 'data.csv' already exists in the target dir, 
#                    Linux automatically renames the incoming file to 'data.csv.~1~' 
#                    instead of destructively overwriting existing production records.
# -v: Verbose output allows us to pipeline exact movements directly into logs.
if mv --backup=numbered -v "${files_to_move[@]}" "$DEST_DIR/"; then
    log "INFO" "🎉 Move operation successfully verified."
else
    log "ERROR" "Critical failure occurred during file transfer sequence."
    exit 1
fi

# Disable nullglob to restore standard bash pattern matching defaults
shopt -u nullglob
