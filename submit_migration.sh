#!/bin/bash
#SBATCH --job-name=data_migration
#SBATCH --output=logs/migration_%A_%a.out
#SBATCH --error=logs/migration_%A_%a.err
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --partition=normal

# Activate conda environment
source ~/.bashrc
conda activate compress_files

# --- Configuration ---
# File containing source directories (one per line)
# Lines starting with # are comments, empty lines are ignored
SOURCES_FILE="${SOURCES_FILE:-sources.txt}"
SCRATCH_BASE="/resnick/scratch/mblanco"
FINAL_BASE="/resnick/groups/guttman/projects/chip-dip"
EXCLUDE=".snakemake/"

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

# --- Process each source ---
for source in "${SOURCES_TO_PROCESS[@]}"; do
    foldername=$(basename "$source")
    destination="$SCRATCH_BASE/$foldername"
    finaldest="$FINAL_BASE/$foldername"

    echo "========================================"
    echo "Source: $source"
    echo "Scratch destination: $destination"
    echo "Final destination: $finaldest"
    echo "Exclude: $EXCLUDE"
    echo "========================================"

    # Step 1: Migrate and compress to scratch
    # --check-existing skips files that already exist in final destination
    python ./data-migration-tools/migrate_compress.py \
        --source "$source" \
        --dest "$destination" \
        --pigz \
        --pigz-threads 4 \
        --workers 4 \
        --exclude "$EXCLUDE" \
        --check-existing \
        --check-path "$finaldest" \
        --no-progress  # Disable progress bars for batch jobs

    if [ $? -ne 0 ]; then
        echo "Warning: migrate_compress.py exited with errors for $source"
        # Continue anyway - partial transfers are handled
    fi

    # Step 2: Move from scratch to final destination
    python ./data-migration-tools/move_to_final.py \
        --source "$destination" \
        --dest "$finaldest"

    if [ $? -ne 0 ]; then
        echo "Warning: move_to_final.py exited with errors for $source"
    fi

    echo "Completed: $foldername"
done

echo "All migrations complete."
