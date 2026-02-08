# S3 Backup Tool

Tool for backing up directories to AWS S3 with parallel uploads, resume capability, and support for storage classes like DEEP_ARCHIVE.

## Features

- **Parallel uploads** using concurrent threads
- **Resumable transfers** via JSON state file
- **Storage class support** - DEEP_ARCHIVE, GLACIER, STANDARD, etc.
- **Extensive logging** with progress tracking
- **MD5 checksums** for integrity verification (optional)
- **Exclusion patterns** for skipping files/directories

## Prerequisites

1. **AWS CLI** must be installed and configured:
   ```bash
   # Install AWS CLI
   conda install -c conda-forge awscli

   # Configure credentials
   aws configure
   ```

2. **Python 3.9+** with tqdm for progress bars:
   ```bash
   conda activate compress_files
   ```

## Usage

### Basic Upload

```bash
# Upload a directory to S3 bucket
python s3_backup.py --source /path/to/data --bucket my-bucket

# Specify a prefix (folder) in the bucket
python s3_backup.py --source /path/to/data --bucket my-bucket --prefix backups/2024
```

### Storage Classes

```bash
# Use DEEP_ARCHIVE for long-term archival (default)
python s3_backup.py --source /data --bucket my-bucket --storage-class DEEP_ARCHIVE

# Use GLACIER for cold storage with faster retrieval
python s3_backup.py --source /data --bucket my-bucket --storage-class GLACIER

# Use STANDARD for frequently accessed data
python s3_backup.py --source /data --bucket my-bucket --storage-class STANDARD
```

### Resume Interrupted Uploads

```bash
# Resume a previous upload that was interrupted
python s3_backup.py --source /data --bucket my-bucket --resume
```

### Parallel Uploads

```bash
# Use 8 parallel upload workers (default: 4)
python s3_backup.py --source /data --bucket my-bucket --workers 8
```

### Exclude Files

```bash
# Exclude specific patterns
python s3_backup.py --source /data --bucket my-bucket --exclude "*.log" temp/ ".snakemake/"
```

### Dry Run

```bash
# Preview what would be uploaded without actually uploading
python s3_backup.py --source /data --bucket my-bucket --dry-run
```

## Options

| Flag | Description |
|------|-------------|
| `-s, --source` | Source directory to backup (required) |
| `-b, --bucket` | S3 bucket name (required) |
| `-p, --prefix` | S3 key prefix (folder path in bucket) |
| `--storage-class` | S3 storage class (default: DEEP_ARCHIVE) |
| `--workers` | Number of parallel upload workers (default: 4) |
| `--resume` | Resume from previous state file |
| `--checksum` | Calculate and log MD5 checksums |
| `--exclude` | Exclude files/directories matching patterns |
| `--log-dir` | Directory for log files (default: current directory) |
| `--no-progress` | Disable progress bars |
| `--dry-run` | Preview uploads without executing |

## Storage Class Reference

| Class | Use Case | Retrieval Time |
|-------|----------|----------------|
| `STANDARD` | Frequently accessed data | Milliseconds |
| `STANDARD_IA` | Infrequently accessed, but rapid access needed | Milliseconds |
| `ONEZONE_IA` | Infrequent access, single AZ (lower cost) | Milliseconds |
| `GLACIER` | Archive with occasional access | Minutes to hours |
| `GLACIER_IR` | Archive with faster retrieval | Minutes |
| `DEEP_ARCHIVE` | Long-term archive, rarely accessed | 12-48 hours |

## Output Files

- `s3_backup_YYYYMMDD_HHMMSS.log` - Human-readable log
- `s3_backup_state.json` - JSON state file for resume and verification

## State File Structure

```json
{
  "source": "/path/to/data",
  "bucket": "my-bucket",
  "prefix": "backups/project",
  "storage_class": "DEEP_ARCHIVE",
  "start_time": "2024-01-26T16:00:00",
  "end_time": "2024-01-26T18:30:00",
  "status": "completed",
  "total_files": 1000,
  "uploaded_files": 998,
  "skipped_files": 0,
  "error_files": 2,
  "total_bytes": 5000000000,
  "uploaded_bytes": 4990000000,
  "files": {
    "/path/to/data/file1.tsv.gz": {
      "local_path": "/path/to/data/file1.tsv.gz",
      "s3_key": "backups/project/file1.tsv.gz",
      "size": 1000000,
      "md5": "abc123...",
      "status": "uploaded",
      "upload_time": "2024-01-26T16:05:00"
    }
  }
}
```

## SLURM Usage

For HPC clusters, use the included SLURM submission script:

```bash
# Submit a single backup job
sbatch submit_s3_backup.sh

# Or run directly (testing)
./submit_s3_backup.sh
```

Configure the script by editing the variables at the top or using environment variables:

```bash
# Override defaults
SOURCE_DIR=/path/to/data BUCKET=my-bucket sbatch submit_s3_backup.sh
```

## Performance Tips

| Scenario | Recommendation |
|----------|----------------|
| Many small files | More workers (--workers 8) |
| Few large files | Fewer workers (--workers 2) |
| Limited bandwidth | Fewer workers to avoid throttling |
| Fast network | More workers for parallelism |

## Cost Considerations

- **DEEP_ARCHIVE** is the cheapest storage but has:
  - 12-48 hour retrieval time
  - 180-day minimum storage duration
  - Per-GB retrieval fees

- **GLACIER** is a middle ground:
  - Minutes to hours retrieval
  - 90-day minimum storage
  - Lower retrieval fees than DEEP_ARCHIVE

- Consider **INTELLIGENT_TIERING** if access patterns are unknown

## Troubleshooting

### "AWS CLI is not installed or not configured"
```bash
# Install AWS CLI
conda install -c conda-forge awscli

# Configure credentials
aws configure
```

### "Cannot access bucket"
- Verify bucket name is correct
- Check IAM permissions include `s3:PutObject` and `s3:ListBucket`
- Verify AWS credentials are valid: `aws sts get-caller-identity`

### Resume not working
- Ensure `s3_backup_state.json` exists in the log directory
- Check that source and bucket parameters match the state file

## License

MIT
