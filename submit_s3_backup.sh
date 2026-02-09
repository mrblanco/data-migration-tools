#!/bin/bash
#SBATCH --job-name=s3_backup
#SBATCH --output=logs/s3_backup_%A_%a.out
#SBATCH --error=logs/s3_backup_%A_%a.err
#SBATCH --time=48:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --partition=normal

# Activate conda environment
source ~/.bashrc
conda activate compress_files

# --- Configuration ---
# These can be overridden by environment variables
BUCKET="${BUCKET:-guttmanlab-primarydata}"
PREFIX="${PREFIX:-}"  # Optional: prefix/folder in bucket
STORAGE_CLASS="${STORAGE_CLASS:-DEEP_ARCHIVE}"
WORKERS="${WORKERS:-4}"
EXCLUDE="${EXCLUDE:-.snakemake/}"

# File containing source directories (one per line)
# Lines starting with # are comments, empty lines are ignored
SOURCES_FILE="${SOURCES_FILE:-s3_sources.txt}"

# --- Read directories from file ---
if [ ! -f "$SOURCES_FILE" ]; then
    echo "Error: Sources file not found: $SOURCES_FILE"
    echo "Create a file with one source directory per line, or set SOURCES_FILE env variable"
    exit 1
fi

# Read non-empty, non-comment lines into array
mapfile -t SOURCES < <(grep -v '^\s*#' "$SOURCES_FILE" | grep -v '^\s*$')

if [ ${#SOURCES[@]} -eq 0 ]; then
    echo "Error: No directories found in $SOURCES_FILE"
    exit 1
fi

echo "Loaded ${#SOURCES[@]} directories from $SOURCES_FILE"

# --- Job Array Logic ---
# If running as array job, use SLURM_ARRAY_TASK_ID to select source
# If running as single job, process all sources sequentially
if [ -n "$SLURM_ARRAY_TASK_ID" ]; then
    # Job array mode: process one directory per task
    IDX=$((SLURM_ARRAY_TASK_ID - 1))  # Arrays are 1-indexed by default
    if [ $IDX -ge ${#SOURCES[@]} ]; then
        echo "Error: Array index $IDX exceeds number of sources (${#SOURCES[@]})"
        exit 1
    fi
    SOURCES_TO_PROCESS=("${SOURCES[$IDX]}")
    echo "Job array task $SLURM_ARRAY_TASK_ID processing: ${SOURCES[$IDX]}"
else
    # Single job mode: process all sources
    SOURCES_TO_PROCESS=("${SOURCES[@]}")
    echo "Single job mode: processing ${#SOURCES[@]} source(s)"
fi

# Create logs directory if it doesn't exist
mkdir -p logs

# --- Process each source ---
for source in "${SOURCES_TO_PROCESS[@]}"; do
    FOLDER_NAME=$(basename "$source")
    if [ -z "$PREFIX" ]; then
        S3_PREFIX="$FOLDER_NAME"
    else
        S3_PREFIX="$PREFIX/$FOLDER_NAME"
    fi

    echo "========================================"
    echo "S3 Backup Job"
    echo "========================================"
    echo "Source: $source"
    echo "Bucket: $BUCKET"
    echo "S3 Prefix: $S3_PREFIX"
    echo "Storage Class: $STORAGE_CLASS"
    echo "Workers: $WORKERS"
    echo "Exclude: $EXCLUDE"
    echo "========================================"

    # Run the backup
    python ./s3_backup.py \
        --source "$source" \
        --bucket "$BUCKET" \
        --prefix "$S3_PREFIX" \
        --storage-class "$STORAGE_CLASS" \
        --workers "$WORKERS" \
        --exclude "$EXCLUDE" \
        --log-dir ./logs \
        --no-progress \
        --resume

    if [ $? -ne 0 ]; then
        echo "Warning: s3_backup.py exited with errors for $source"
        echo "Check logs for details. Run with --resume to retry failed uploads."
    else
        echo "Backup completed successfully for: $FOLDER_NAME"
    fi
done

echo "========================================"
echo "All backups complete."
echo "Job finished at $(date)"
echo "========================================"
