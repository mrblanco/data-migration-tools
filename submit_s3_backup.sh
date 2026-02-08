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
SOURCE_DIR="${SOURCE_DIR:-/groups/guttman/projects/chip-dip/2021_Histone-ABCAM_Merge}"
BUCKET="${BUCKET:-guttmanlab-primarydata}"
PREFIX="${PREFIX:-}"  # Optional: prefix/folder in bucket
STORAGE_CLASS="${STORAGE_CLASS:-DEEP_ARCHIVE}"
WORKERS="${WORKERS:-4}"
EXCLUDE="${EXCLUDE:-.snakemake/}"

# File containing source directories for job arrays (one per line)
SOURCES_FILE="${SOURCES_FILE:-s3_sources.txt}"

# --- Determine what to backup ---
# If running as array job and sources file exists, use that
# Otherwise, use SOURCE_DIR directly

if [ -n "$SLURM_ARRAY_TASK_ID" ] && [ -f "$SOURCES_FILE" ]; then
    # Job array mode: read directory from file
    mapfile -t SOURCES < <(grep -v '^\s*#' "$SOURCES_FILE" | grep -v '^\s*$')

    if [ ${#SOURCES[@]} -eq 0 ]; then
        echo "Error: No directories found in $SOURCES_FILE"
        exit 1
    fi

    IDX=$((SLURM_ARRAY_TASK_ID - 1))
    if [ $IDX -ge ${#SOURCES[@]} ]; then
        echo "Error: Array index $IDX exceeds number of sources (${#SOURCES[@]})"
        exit 1
    fi

    SOURCE_DIR="${SOURCES[$IDX]}"
    echo "Job array task $SLURM_ARRAY_TASK_ID processing: $SOURCE_DIR"
fi

# Extract folder name for prefix if not specified
FOLDER_NAME=$(basename "$SOURCE_DIR")
if [ -z "$PREFIX" ]; then
    S3_PREFIX="$FOLDER_NAME"
else
    S3_PREFIX="$PREFIX/$FOLDER_NAME"
fi

echo "========================================"
echo "S3 Backup Job"
echo "========================================"
echo "Source: $SOURCE_DIR"
echo "Bucket: $BUCKET"
echo "S3 Prefix: $S3_PREFIX"
echo "Storage Class: $STORAGE_CLASS"
echo "Workers: $WORKERS"
echo "Exclude: $EXCLUDE"
echo "========================================"

# Create logs directory if it doesn't exist
mkdir -p logs

# Run the backup
python ./s3_backup.py \
    --source "$SOURCE_DIR" \
    --bucket "$BUCKET" \
    --prefix "$S3_PREFIX" \
    --storage-class "$STORAGE_CLASS" \
    --workers "$WORKERS" \
    --exclude "$EXCLUDE" \
    --log-dir ./logs \
    --no-progress \
    --resume

EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    echo "Warning: s3_backup.py exited with code $EXIT_CODE"
    echo "Check logs for details. Run with --resume to retry failed uploads."
else
    echo "Backup completed successfully!"
fi

echo "========================================"
echo "Job finished at $(date)"
echo "========================================"

exit $EXIT_CODE
