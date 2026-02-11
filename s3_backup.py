#!/usr/bin/env python3
"""
S3 Backup Tool - Upload directories to AWS S3 with parallel processing and resume capability.

Features:
- Parallel uploads using concurrent threads
- Resumable transfers via JSON state file
- Support for S3 storage classes (DEEP_ARCHIVE, GLACIER, etc.)
- Check existing S3 objects to avoid re-uploads (critical for DEEP_ARCHIVE)
- Extensive logging with progress tracking
- Dry-run mode for previewing uploads

Usage:
    python s3_backup.py --source /path/to/data --bucket my-bucket --prefix backup/data
    python s3_backup.py --source /path/to/data --bucket my-bucket --storage-class DEEP_ARCHIVE
    python s3_backup.py --source /path/to/data --bucket my-bucket --resume  # Resume interrupted upload
    python s3_backup.py --source /path/to/data --bucket my-bucket --check-existing  # Skip files already in S3
"""

import argparse
import datetime
import hashlib
import json
import logging
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import List, Optional, Dict, Any

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    tqdm = None

# Valid S3 storage classes
STORAGE_CLASSES = [
    'STANDARD',
    'REDUCED_REDUNDANCY',
    'STANDARD_IA',
    'ONEZONE_IA',
    'INTELLIGENT_TIERING',
    'GLACIER',
    'DEEP_ARCHIVE',
    'GLACIER_IR',
]

CHUNK = 1024 * 1024  # 1MB buffer for checksums


def setup_logging(log_file: Path) -> logging.Logger:
    """Setup logging to both console and file."""
    logger = logging.getLogger('s3_backup')
    logger.setLevel(logging.DEBUG)

    # Console handler (INFO level)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter('%(message)s'))

    # File handler (DEBUG level)
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    ))

    logger.addHandler(console)
    logger.addHandler(file_handler)

    return logger


@dataclass
class FileRecord:
    """Record of a single file upload."""
    local_path: str
    s3_key: str
    size: int
    md5: Optional[str] = None
    status: str = 'pending'  # pending, uploading, uploaded, error, skipped
    upload_time: Optional[str] = None
    error_message: Optional[str] = None


@dataclass
class BackupState:
    """State of the entire backup operation."""
    source: str
    bucket: str
    prefix: str
    storage_class: str
    start_time: str
    end_time: Optional[str] = None
    status: str = 'in_progress'  # in_progress, completed, completed_with_errors
    total_files: int = 0
    uploaded_files: int = 0
    skipped_files: int = 0
    error_files: int = 0
    total_bytes: int = 0
    uploaded_bytes: int = 0
    files: Dict[str, dict] = field(default_factory=dict)

    def save(self, path: Path):
        """Save state to JSON file."""
        with open(path, 'w') as f:
            json.dump(asdict(self), f, indent=2)

    @classmethod
    def load(cls, path: Path) -> 'BackupState':
        """Load state from JSON file."""
        with open(path, 'r') as f:
            data = json.load(f)
        state = cls(
            source=data['source'],
            bucket=data['bucket'],
            prefix=data['prefix'],
            storage_class=data['storage_class'],
            start_time=data['start_time'],
        )
        state.end_time = data.get('end_time')
        state.status = data.get('status', 'in_progress')
        state.total_files = data.get('total_files', 0)
        state.uploaded_files = data.get('uploaded_files', 0)
        state.skipped_files = data.get('skipped_files', 0)
        state.error_files = data.get('error_files', 0)
        state.total_bytes = data.get('total_bytes', 0)
        state.uploaded_bytes = data.get('uploaded_bytes', 0)
        state.files = data.get('files', {})
        return state

    def is_uploaded(self, local_path: str) -> bool:
        """Check if a file has already been uploaded."""
        file_info = self.files.get(local_path)
        return file_info is not None and file_info.get('status') == 'uploaded'


def get_size_str(size_bytes: int) -> str:
    """Convert bytes to human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"


def calculate_md5(file_path: Path) -> str:
    """Calculate MD5 hash of a file."""
    md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        while chunk := f.read(CHUNK):
            md5.update(chunk)
    return md5.hexdigest()


def get_files_to_upload(
    source: Path,
    exclude: Optional[List[str]] = None
) -> List[Path]:
    """Get list of files to upload, respecting exclusions."""
    files = []
    exclude_set = set(exclude) if exclude else set()

    for f in source.rglob('*'):
        if f.is_file():
            # Check exclusions
            rel_path = f.relative_to(source)
            skip = False

            for pattern in exclude_set:
                # Simple pattern matching
                if pattern.endswith('/'):
                    # Directory pattern
                    dir_name = pattern.rstrip('/')
                    if any(part == dir_name for part in rel_path.parts[:-1]):
                        skip = True
                        break
                elif '*' in pattern:
                    # Glob pattern (simple)
                    import fnmatch
                    if fnmatch.fnmatch(f.name, pattern):
                        skip = True
                        break
                else:
                    # Exact match
                    if f.name == pattern or any(part == pattern for part in rel_path.parts):
                        skip = True
                        break

            if not skip:
                files.append(f)

    return files


def calculate_timeout(file_size_bytes: int, base_timeout: int = 1800, per_gb_timeout: int = 360) -> int:
    """
    Calculate dynamic timeout based on file size.

    Default: 30 min base + 6 min per GB
    - 1GB file: 36 min
    - 10GB file: 90 min
    - 50GB file: 5.5 hours
    - 100GB file: 10.5 hours

    Returns timeout in seconds.
    """
    gb_size = file_size_bytes / (1024 ** 3)
    return int(base_timeout + (gb_size * per_gb_timeout))


def upload_file(
    local_path: Path,
    bucket: str,
    s3_key: str,
    storage_class: str,
    dry_run: bool = False,
    calculate_checksum: bool = False,
    timeout_override: Optional[int] = None
) -> FileRecord:
    """
    Upload a single file to S3.

    Args:
        timeout_override: If provided, use this timeout in seconds.
                         If 0, disable timeout entirely.
                         If None, calculate dynamic timeout based on file size.

    Returns FileRecord with upload status.
    """
    file_size = local_path.stat().st_size
    record = FileRecord(
        local_path=str(local_path),
        s3_key=s3_key,
        size=file_size
    )

    if calculate_checksum:
        record.md5 = calculate_md5(local_path)

    if dry_run:
        record.status = 'dry_run'
        return record

    record.status = 'uploading'

    s3_uri = f"s3://{bucket}/{s3_key}"

    cmd = [
        'aws', 's3', 'cp',
        str(local_path),
        s3_uri,
        '--storage-class', storage_class,
    ]

    # Calculate timeout: use override, or dynamic based on file size
    if timeout_override == 0:
        timeout = None  # No timeout
        timeout_str = "no timeout"
    elif timeout_override is not None:
        timeout = timeout_override
        timeout_str = f"{timeout // 3600}h {(timeout % 3600) // 60}m"
    else:
        timeout = calculate_timeout(file_size)
        timeout_str = f"{timeout // 3600}h {(timeout % 3600) // 60}m"

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout
        )

        if result.returncode == 0:
            record.status = 'uploaded'
            record.upload_time = datetime.datetime.now().isoformat()
        else:
            record.status = 'error'
            record.error_message = result.stderr.strip() or result.stdout.strip()

    except subprocess.TimeoutExpired:
        record.status = 'error'
        record.error_message = f'Upload timed out after {timeout_str} (file size: {get_size_str(file_size)})'
    except Exception as e:
        record.status = 'error'
        record.error_message = str(e)

    return record


def check_aws_cli() -> bool:
    """Check if AWS CLI is installed and configured."""
    try:
        result = subprocess.run(
            ['aws', 'sts', 'get-caller-identity'],
            capture_output=True,
            text=True,
            timeout=30
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def check_bucket_access(bucket: str) -> bool:
    """Check if we have access to the S3 bucket."""
    try:
        result = subprocess.run(
            ['aws', 's3', 'ls', f's3://{bucket}/'],
            capture_output=True,
            text=True,
            timeout=30
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def list_existing_s3_objects(
    bucket: str,
    prefix: str,
    logger: logging.Logger
) -> Dict[str, int]:
    """
    List all existing objects in S3 under the given prefix.

    Returns a dict mapping S3 keys to their sizes.
    This is used to check what's already uploaded and avoid re-uploading
    (important for DEEP_ARCHIVE to avoid early deletion penalties).
    """
    existing = {}
    continuation_token = None

    logger.info(f"Querying S3 for existing objects in s3://{bucket}/{prefix}...")

    while True:
        cmd = [
            'aws', 's3api', 'list-objects-v2',
            '--bucket', bucket,
            '--prefix', prefix,
            '--output', 'json'
        ]

        if continuation_token:
            cmd.extend(['--continuation-token', continuation_token])

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout for large listings
            )

            if result.returncode != 0:
                logger.warning(f"Failed to list S3 objects: {result.stderr}")
                break

            data = json.loads(result.stdout)

            for obj in data.get('Contents', []):
                key = obj['Key']
                size = obj['Size']
                existing[key] = size

            # Check if there are more results
            if data.get('IsTruncated'):
                continuation_token = data.get('NextContinuationToken')
            else:
                break

        except subprocess.TimeoutExpired:
            logger.warning("Timeout while listing S3 objects")
            break
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse S3 listing: {e}")
            break
        except Exception as e:
            logger.warning(f"Error listing S3 objects: {e}")
            break

    logger.info(f"Found {len(existing)} existing objects in S3")
    return existing


def main(argv: List[str] = None) -> int:
    parser = argparse.ArgumentParser(
        description='Upload directories to AWS S3 with parallel processing and resume capability.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic upload to S3
  python s3_backup.py --source /data/project --bucket my-bucket --prefix backups/project

  # Upload with DEEP_ARCHIVE storage class
  python s3_backup.py --source /data/project --bucket my-bucket --storage-class DEEP_ARCHIVE

  # Resume an interrupted upload
  python s3_backup.py --source /data/project --bucket my-bucket --resume

  # Dry run to see what would be uploaded
  python s3_backup.py --source /data/project --bucket my-bucket --dry-run

  # Parallel upload with 8 workers
  python s3_backup.py --source /data/project --bucket my-bucket --workers 8

  # Resume with S3 check (recommended for DEEP_ARCHIVE after state file issues)
  python s3_backup.py --source /data/project --bucket my-bucket --check-existing --resume

  # Check what would be uploaded without uploading (safe for DEEP_ARCHIVE verification)
  python s3_backup.py --source /data/project --bucket my-bucket --check-only
        """
    )

    parser.add_argument('-s', '--source', type=Path, required=True,
                        help='Source directory to backup')
    parser.add_argument('-b', '--bucket', type=str, required=True,
                        help='S3 bucket name')
    parser.add_argument('-p', '--prefix', type=str, default='',
                        help='S3 key prefix (folder path in bucket)')
    parser.add_argument('--storage-class', type=str, default='DEEP_ARCHIVE',
                        choices=STORAGE_CLASSES,
                        help='S3 storage class (default: DEEP_ARCHIVE)')
    parser.add_argument('--workers', type=int, default=4,
                        help='Number of parallel upload workers (default: 4)')
    parser.add_argument('--resume', action='store_true',
                        help='Resume from previous state file')
    parser.add_argument('--checksum', action='store_true',
                        help='Calculate and log MD5 checksums')
    parser.add_argument('--exclude', nargs='+', default=None,
                        help='Exclude files/directories matching patterns')
    parser.add_argument('--log-dir', type=Path, default=None,
                        help='Directory for log files (default: current directory)')
    parser.add_argument('--no-progress', action='store_true',
                        help='Disable progress bars')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be uploaded without actually uploading')
    parser.add_argument('--check-existing', action='store_true',
                        help='Query S3 for existing objects and skip them (recommended for DEEP_ARCHIVE to avoid early deletion penalties)')
    parser.add_argument('--check-only', action='store_true',
                        help='Query S3 and report what would be uploaded, then exit (no uploads). Use this to verify before resuming DEEP_ARCHIVE backups.')
    parser.add_argument('--timeout', type=int, default=None,
                        help='Upload timeout in seconds per file. Default: dynamic (30min + 6min/GB). Use 0 to disable timeout.')

    args = parser.parse_args(argv)

    # --check-only implies --check-existing
    if args.check_only:
        args.check_existing = True

    # Validate source
    if not args.source.exists():
        print(f"Error: Source path does not exist: {args.source}", file=sys.stderr)
        return 1

    if not args.source.is_dir():
        print(f"Error: Source must be a directory: {args.source}", file=sys.stderr)
        return 1

    # Setup logging
    log_dir = args.log_dir or Path.cwd()
    log_dir.mkdir(parents=True, exist_ok=True)

    # Use source directory name for unique log/state files (handles parallel jobs)
    source_name = args.source.name  # basename of source directory
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f's3_backup_{source_name}_{timestamp}.log'
    state_file = log_dir / f's3_backup_state_{source_name}.json'

    logger = setup_logging(log_file)

    logger.info("=" * 60)
    logger.info("S3 Backup Tool")
    logger.info("=" * 60)

    # Check AWS CLI
    if not args.dry_run:
        logger.info("Checking AWS CLI configuration...")
        if not check_aws_cli():
            logger.error("AWS CLI is not installed or not configured")
            logger.error("Run 'aws configure' to set up credentials")
            return 1

        logger.info("Checking bucket access...")
        if not check_bucket_access(args.bucket):
            logger.error(f"Cannot access bucket: {args.bucket}")
            logger.error("Check bucket name and permissions")
            return 1

    logger.info(f"Source: {args.source}")
    logger.info(f"Bucket: {args.bucket}")
    logger.info(f"Prefix: {args.prefix or '(root)'}")
    logger.info(f"Storage class: {args.storage_class}")
    logger.info(f"Workers: {args.workers}")
    if args.exclude:
        logger.info(f"Excluding: {args.exclude}")
    if args.dry_run:
        logger.info("DRY RUN - no files will be uploaded")

    # Load or create state
    if args.resume and state_file.exists():
        logger.info(f"Resuming from state file: {state_file}")
        state = BackupState.load(state_file)

        # Validate state matches current parameters
        if state.source != str(args.source):
            logger.warning(f"Source mismatch: state has {state.source}, using {args.source}")
        if state.bucket != args.bucket:
            logger.error(f"Bucket mismatch: state has {state.bucket}, command has {args.bucket}")
            return 1
    else:
        state = BackupState(
            source=str(args.source),
            bucket=args.bucket,
            prefix=args.prefix,
            storage_class=args.storage_class,
            start_time=datetime.datetime.now().isoformat()
        )

    # Get files to upload
    logger.info("Scanning source directory...")
    all_files = get_files_to_upload(args.source, args.exclude)
    logger.info(f"Found {len(all_files)} files")

    # Calculate total size
    total_size = sum(f.stat().st_size for f in all_files)
    logger.info(f"Total size: {get_size_str(total_size)}")

    state.total_files = len(all_files)
    state.total_bytes = total_size

    # Check S3 for existing objects if requested
    # This is important for DEEP_ARCHIVE to avoid early deletion penalties
    existing_s3_objects = {}
    if args.check_existing and not args.dry_run:
        existing_s3_objects = list_existing_s3_objects(
            bucket=args.bucket,
            prefix=args.prefix,
            logger=logger
        )

    # Filter out already uploaded files
    files_to_upload = []
    skipped_from_state = 0
    skipped_from_s3 = 0

    for f in all_files:
        # Check state file first (faster)
        if state.is_uploaded(str(f)):
            skipped_from_state += 1
            state.skipped_files += 1
            continue

        # Check S3 if --check-existing was used
        if existing_s3_objects:
            rel_path = f.relative_to(args.source)
            if args.prefix:
                s3_key = f"{args.prefix.rstrip('/')}/{rel_path.as_posix()}"
            else:
                s3_key = rel_path.as_posix()

            if s3_key in existing_s3_objects:
                # File exists in S3 - check size matches
                local_size = f.stat().st_size
                s3_size = existing_s3_objects[s3_key]
                if local_size == s3_size:
                    skipped_from_s3 += 1
                    state.skipped_files += 1
                    # Update state to mark as uploaded (for future runs)
                    state.files[str(f)] = {
                        'local_path': str(f),
                        's3_key': s3_key,
                        'size': local_size,
                        'status': 'uploaded',
                        'upload_time': 'pre-existing'
                    }
                    continue
                else:
                    logger.warning(f"Size mismatch for {s3_key}: local={local_size}, S3={s3_size}")

        files_to_upload.append(f)

    if skipped_from_state > 0:
        logger.info(f"Skipping {skipped_from_state} files (from state file)")
    if skipped_from_s3 > 0:
        logger.info(f"Skipping {skipped_from_s3} files (already in S3)")

    logger.info(f"Files to upload: {len(files_to_upload)}")

    if not files_to_upload:
        logger.info("No files to upload!")
        state.status = 'completed'
        state.end_time = datetime.datetime.now().isoformat()
        state.save(state_file)
        return 0

    # Calculate size of files to upload (needed for --check-only report)
    upload_size = sum(f.stat().st_size for f in files_to_upload)

    # --check-only: report what would be uploaded and exit
    if args.check_only:
        logger.info("")
        logger.info("=" * 60)
        logger.info("CHECK-ONLY MODE - No uploads will be performed")
        logger.info("=" * 60)
        logger.info(f"Source: {args.source}")
        logger.info(f"Destination: s3://{args.bucket}/{args.prefix}")
        logger.info(f"Storage class: {args.storage_class}")
        logger.info("")
        logger.info("Summary:")
        logger.info(f"  Total files in source: {len(all_files)}")
        logger.info(f"  Total size in source: {get_size_str(total_size)}")
        logger.info(f"  Already in S3: {skipped_from_s3}")
        logger.info(f"  In state file: {skipped_from_state}")
        logger.info(f"  Files to upload: {len(files_to_upload)}")
        logger.info(f"  Size to upload: {get_size_str(upload_size)}")
        logger.info("")

        if files_to_upload:
            logger.info("Files that would be uploaded:")
            # Show first 20 files, then summarize
            for i, f in enumerate(files_to_upload[:20]):
                rel_path = f.relative_to(args.source)
                size = f.stat().st_size
                logger.info(f"  {rel_path} ({get_size_str(size)})")
            if len(files_to_upload) > 20:
                logger.info(f"  ... and {len(files_to_upload) - 20} more files")

        logger.info("")
        logger.info("To proceed with upload, run without --check-only:")
        logger.info(f"  python s3_backup.py --source {args.source} --bucket {args.bucket} --prefix {args.prefix} --check-existing --resume")
        logger.info("=" * 60)

        # Save state with pre-existing files marked (so they're skipped on actual run)
        state.save(state_file)
        return 0

    # Determine if we should show progress bars
    show_progress = TQDM_AVAILABLE and not args.no_progress

    # Progress tracking
    uploaded_bytes = 0
    uploaded_count = 0
    error_count = 0
    lock = threading.Lock()

    def upload_with_progress(file_path: Path) -> FileRecord:
        """Upload a file and track progress."""
        nonlocal uploaded_bytes, uploaded_count, error_count

        # Calculate S3 key
        rel_path = file_path.relative_to(args.source)
        if args.prefix:
            s3_key = f"{args.prefix.rstrip('/')}/{rel_path.as_posix()}"
        else:
            s3_key = rel_path.as_posix()

        record = upload_file(
            local_path=file_path,
            bucket=args.bucket,
            s3_key=s3_key,
            storage_class=args.storage_class,
            dry_run=args.dry_run,
            calculate_checksum=args.checksum,
            timeout_override=args.timeout
        )

        with lock:
            state.files[str(file_path)] = asdict(record)

            if record.status in ('uploaded', 'dry_run'):
                uploaded_bytes += record.size
                uploaded_count += 1
                state.uploaded_files += 1
                state.uploaded_bytes += record.size
            else:
                error_count += 1
                state.error_files += 1

            # Save state periodically
            state.save(state_file)

        return record

    # Setup progress bar
    if show_progress:
        pbar = tqdm(
            total=upload_size,
            unit='B',
            unit_scale=True,
            desc="Uploading"
        )
    else:
        pbar = None

    logger.info("=" * 60)
    logger.info("Starting uploads...")
    logger.info("=" * 60)

    # Upload files in parallel
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(upload_with_progress, f): f
            for f in files_to_upload
        }

        for future in as_completed(futures):
            file_path = futures[future]
            try:
                record = future.result()

                if pbar:
                    pbar.update(record.size)

                if record.status in ('uploaded', 'dry_run'):
                    logger.info(f"OK {file_path.name} -> s3://{args.bucket}/{record.s3_key} ({get_size_str(record.size)})")
                else:
                    logger.error(f"ERROR {file_path.name}: {record.error_message}")

            except Exception as e:
                logger.error(f"Exception uploading {file_path}: {e}")
                error_count += 1

    if pbar:
        pbar.close()

    # Final summary
    state.end_time = datetime.datetime.now().isoformat()
    if error_count > 0:
        state.status = 'completed_with_errors'
    else:
        state.status = 'completed'
    state.save(state_file)

    logger.info("=" * 60)
    logger.info("Backup Summary")
    logger.info("=" * 60)
    logger.info(f"Total files: {state.total_files}")
    logger.info(f"Uploaded: {state.uploaded_files}")
    logger.info(f"Skipped (already uploaded): {state.skipped_files}")
    logger.info(f"Errors: {state.error_files}")
    logger.info(f"Total size: {get_size_str(state.total_bytes)}")
    logger.info(f"Uploaded size: {get_size_str(state.uploaded_bytes)}")
    logger.info(f"Storage class: {args.storage_class}")
    logger.info(f"Destination: s3://{args.bucket}/{args.prefix}")
    logger.info("")
    logger.info(f"Log file: {log_file}")
    logger.info(f"State file: {state_file}")

    if state.error_files > 0:
        logger.warning(f"Completed with {state.error_files} errors")
        logger.info("Run with --resume to retry failed uploads")
        return 1

    logger.info("Backup completed successfully!")
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
