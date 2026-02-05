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
# List of source directories to process (one per line)
# For job arrays, each task processes one directory
SOURCES=(
    "/groups/guttman/projects/chip-dip/2021_Histone-ABCAM_Merge"
    # Add more directories here for job arrays:
    # "/groups/guttman/projects/chip-dip/another_dataset"
    # "/groups/guttman/projects/chip-dip/third_dataset"
)

SCRATCH_BASE="/resnick/scratch/mblanco"
FINAL_BASE="/resnick/groups/guttman/projects/chip-dip"
EXCLUDE=".snakemake/"

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
    python ./data-migration-tools/migrate_compress.py \
        --source "$source" \
        --dest "$destination" \
        --pigz \
        --pigz-threads 4 \
        --workers 4 \
        --exclude "$EXCLUDE" \
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
