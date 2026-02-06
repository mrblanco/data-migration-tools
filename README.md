# Data Migration and Compression Tools

Tools for compressing and migrating large datasets with support for parallel processing, resumable transfers, and verification.

## Features

- **Parallel compression** using Python multiprocessing or pigz
- **Resumable transfers** via rsync with `--partial`
- **Integrity verification** using SHA256 checksums
- **Extensive logging** with JSON state files for auditing
- **Detailed rsync logs** capturing per-file transfer status and errors
- **Partial transfer handling** - continue compressing successfully transferred files
- **Multiple algorithms** - gzip (fast) or xz (better compression)

## Installation

```bash
# Create conda environment
conda env create -f compress_files_env.yaml

# Activate
conda activate compress_files
```

## Tools

### compress_files.py

Compress files in-place by extension. Best for when you want to compress files without copying them elsewhere.

```bash
# Dry run - see what would be compressed
python compress_files.py -r /path/to/data --ext tsv clusters cluster --dry-run

# Compress with gzip, 4 workers
python compress_files.py -r /path/to/data --ext tsv clusters cluster --workers 4 --remove

# Use pigz for faster compression (recommended for large files)
python compress_files.py -r /path/to/data --ext tsv clusters cluster --pigz --pigz-threads 4 --remove

# Compress with xz (slower but better compression ratio)
python compress_files.py -r /path/to/data --ext tsv clusters cluster --algo xz --remove

# Verify integrity after compression
python compress_files.py -r /path/to/data --ext tsv clusters cluster --pigz --verify --remove
```

**Options:**

| Flag | Description |
|------|-------------|
| `-r, --root` | Root directory to search |
| `--ext` | File extensions to compress (default: tsv clusters cluster) |
| `--algo` | Compression algorithm: gzip or xz (default: gzip) |
| `--level` | Compression level 1-9 (default: 6) |
| `--workers` | Parallel worker processes (default: half of CPU count) |
| `--pigz` | Use pigz instead of Python gzip |
| `--pigz-threads` | Threads per pigz process (default: 4) |
| `--verify` | Verify compressed output matches original |
| `--remove` | Remove original files after successful compression |
| `--dry-run` | List files without compressing |

---

### migrate_compress.py

Copy directories to a destination (e.g., scratch space) and compress on the fly. Designed for large-scale data migrations with full logging and resume capability.

```bash
# Dry run - preview what would happen
python migrate_compress.py --source /data/original --dest /scratch/migration --dry-run

# Full migration with pigz and verification
python migrate_compress.py --source /data/original --dest /scratch/migration --pigz --verify

# Resume after interruption
python migrate_compress.py --source /data/original --dest /scratch/migration --pigz --verify --resume

# Process source as single directory (don't iterate subdirectories)
python migrate_compress.py --source /data/original --dest /scratch/migration --process-root --pigz

# Exclude specific folders from the transfer
python migrate_compress.py --source /data/original --dest /scratch/migration --exclude temp_folder cache logs

# Skip files that already exist in final destination (useful for re-running)
python migrate_compress.py --source /data/original --dest /scratch/migration --pigz --check-existing --check-path /final/destination
```

**Options:**

| Flag | Description |
|------|-------------|
| `-s, --source` | Source root directory |
| `-d, --dest` | Destination root directory (scratch) |
| `--ext` | File extensions to compress (default: tsv clusters cluster) |
| `--algo` | Compression algorithm: gzip or xz (default: gzip) |
| `--level` | Compression level 1-9 (default: 6) |
| `--workers` | Parallel compression workers (default: half of CPU count) |
| `--pigz` | Use pigz for faster gzip compression |
| `--pigz-threads` | Threads per pigz process (default: 4) |
| `--verify` | Verify compressed files via hash comparison |
| `--resume` | Skip already-completed directories |
| `--process-root` | Treat source as single directory |
| `--log-dir` | Directory for log files (default: dest) |
| `--stop-on-partial` | Stop if rsync has partial failures (default: continue) |
| `--exclude` | Exclude directories/files matching patterns (rsync syntax) |
| `--no-progress` | Disable progress bars (useful for non-interactive runs) |
| `--check-existing` | Skip files that already exist (compressed or not) in destination |
| `--check-path` | Path to check for existing files (default: --dest). Use for final destination |
| `--dry-run` | Show what would be done without changes |

**Exclude patterns:**

| Pattern | Excludes |
|---------|----------|
| `temp` | Any file/folder named "temp" |
| `temp/` | Only folders named "temp" |
| `*.log` | All .log files |
| `**/cache` | "cache" folder at any depth |
| `/top_level` | Only at root of source |

**Output files:**

- `migration_YYYYMMDD_HHMMSS.log` - Human-readable log
- `rsync_<dirname>_YYYYMMDD_HHMMSS.log` - Detailed rsync output per directory
- `migration_state.json` - JSON state file for resume and verification

**State file structure:**

```json
{
  "directories": {
    "/data/original/subdir1": {
      "status": "completed",
      "rsync_exit_code": 0,
      "rsync_log_file": "/scratch/migration/rsync_subdir1_20260126_161642.log",
      "rsync_errors": [],
      "total_source_size": 1234567890,
      "total_compressed_size": 456789012,
      "files": [
        {
          "source_path": "/scratch/migration/subdir1/file.tsv",
          "dest_path": "/scratch/migration/subdir1/file.tsv.gz",
          "original_size": 1000000,
          "compressed_size": 250000,
          "source_sha256": "abc123...",
          "decompressed_sha256": "abc123...",
          "status": "verified"
        }
      ]
    }
  }
}
```

**rsync exit codes:**

| Code | Meaning |
|------|---------|
| 0 | Success |
| 23 | Partial transfer due to error (some files failed) |
| 24 | Partial transfer due to vanished source files |

By default, the script continues compressing successfully transferred files when rsync exits with code 23 or 24. Use `--stop-on-partial` to halt instead.

---

### move_to_final.py

Move data from scratch to final destination with verification. Designed to be used after `migrate_compress.py` to complete a two-stage migration workflow.

```bash
# Dry run - see what would be moved
python move_to_final.py --source /scratch/data --dest /final/destination --dry-run

# Move with verification (removes source after successful transfer)
python move_to_final.py --source /scratch/data --dest /final/destination

# Copy mode - keep source files
python move_to_final.py --source /scratch/data --dest /final/destination --keep-source
```

**Options:**

| Flag | Description |
|------|-------------|
| `-s, --source` | Source directory (scratch location) |
| `-d, --dest` | Destination directory (final location) |
| `--log-dir` | Directory for log files (default: source parent) |
| `--keep-source` | Keep source files after transfer (copy mode) |
| `--dry-run` | Show what would be done without changes |

**Features:**
- Uses rsync with `--checksum` for verification
- `--remove-source-files` deletes source only after successful transfer
- Cleans up empty directories in source after move
- JSON log file records all operations for auditing

**Typical workflow:**

```bash
# Step 1: Compress and copy to scratch
python migrate_compress.py --source /original/data --dest /scratch/temp --pigz

# Step 2: Move from scratch to final destination
python move_to_final.py --source /scratch/temp --dest /final/location
```

## Performance Tuning

For large datasets, balance parallelism based on your hardware:

| Scenario | Recommendation |
|----------|----------------|
| Many small files | More workers, fewer pigz threads (`--workers 8 --pigz-threads 2`) |
| Few large files | Fewer workers, more pigz threads (`--workers 2 --pigz-threads 8`) |
| SSD storage | More workers |
| Spinning disks | Fewer workers to reduce seek overhead |

## License

MIT
