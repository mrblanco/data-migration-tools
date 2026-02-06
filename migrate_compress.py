#!/usr/bin/env python3
"""
Migration script: Copy directories to scratch, compress on the fly, with extensive logging.

Workflow:
1. Enumerate subdirectories in source
2. For each directory: rsync to scratch destination
3. Compress matching files in destination
4. Log all operations with checksums for verification
5. Support resumption if interrupted

Usage:
python migrate_compress.py --source /data/original --dest /scratch/migration --ext tsv clusters cluster --dry-run
python migrate_compress.py --source /data/original --dest /scratch/migration --ext tsv clusters cluster --pigz --resume
"""
from __future__ import annotations

import argparse
import datetime
import gzip
import hashlib
import json
import logging
import lzma
import multiprocessing
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field, asdict
from functools import partial
from pathlib import Path
from typing import List, Optional, Dict, Any

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    tqdm = None

CHUNK = 1024 * 1024  # 1MB buffer

# Configure logging
def setup_logging(log_file: Path) -> logging.Logger:
    """Setup logging to both console and file."""
    logger = logging.getLogger('migrate_compress')
    logger.setLevel(logging.DEBUG)

    # Console handler (INFO level)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger.addHandler(console)

    # File handler (DEBUG level for detailed logs)
    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger.addHandler(file_handler)

    return logger


@dataclass
class FileRecord:
    """Record of a single file operation."""
    source_path: str
    dest_path: str
    original_size: int
    compressed_size: Optional[int] = None
    source_sha256: Optional[str] = None
    compressed_sha256: Optional[str] = None
    decompressed_sha256: Optional[str] = None
    status: str = 'pending'  # pending, copied, compressed, verified, error
    error_message: Optional[str] = None
    rsync_exit_code: Optional[int] = None
    timestamp: str = field(default_factory=lambda: datetime.datetime.now().isoformat())


@dataclass
class DirectoryRecord:
    """Record of a directory migration."""
    source_dir: str
    dest_dir: str
    status: str = 'pending'  # pending, syncing, compressing, completed, error, partial
    rsync_command: Optional[str] = None
    rsync_exit_code: Optional[int] = None
    rsync_log_file: Optional[str] = None
    rsync_errors: List[str] = field(default_factory=list)
    files: List[FileRecord] = field(default_factory=list)
    total_source_size: int = 0
    total_compressed_size: int = 0
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    error_message: Optional[str] = None


# rsync exit codes
RSYNC_EXIT_CODES = {
    0: "Success",
    1: "Syntax or usage error",
    2: "Protocol incompatibility",
    3: "Errors selecting input/output files, dirs",
    4: "Requested action not supported",
    5: "Error starting client-server protocol",
    6: "Daemon unable to append to log-file",
    10: "Error in socket I/O",
    11: "Error in file I/O",
    12: "Error in rsync protocol data stream",
    13: "Errors with program diagnostics",
    14: "Error in IPC code",
    20: "Received SIGUSR1 or SIGINT",
    21: "Some error returned by waitpid()",
    22: "Error allocating core memory buffers",
    23: "Partial transfer due to error",
    24: "Partial transfer due to vanished source files",
    25: "The --max-delete limit stopped deletions",
    30: "Timeout in data send/receive",
    35: "Timeout waiting for daemon connection",
}


class MigrationLog:
    """JSON-based migration log for tracking and resumption."""

    def __init__(self, log_path: Path):
        self.log_path = log_path
        self.directories: Dict[str, DirectoryRecord] = {}
        self._load()

    def _load(self):
        """Load existing log if present."""
        if self.log_path.exists():
            with open(self.log_path, 'r') as f:
                data = json.load(f)
                for dir_path, dir_data in data.get('directories', {}).items():
                    files = [FileRecord(**fr) for fr in dir_data.pop('files', [])]
                    self.directories[dir_path] = DirectoryRecord(**dir_data, files=files)

    def save(self):
        """Save log to disk."""
        data = {
            'last_updated': datetime.datetime.now().isoformat(),
            'directories': {k: asdict(v) for k, v in self.directories.items()}
        }
        # Write to temp file first, then rename for atomicity
        tmp_path = self.log_path.with_suffix('.tmp')
        with open(tmp_path, 'w') as f:
            json.dump(data, f, indent=2)
        tmp_path.replace(self.log_path)

    def get_or_create_dir(self, source_dir: str, dest_dir: str) -> DirectoryRecord:
        """Get existing record or create new one."""
        if source_dir not in self.directories:
            self.directories[source_dir] = DirectoryRecord(
                source_dir=source_dir,
                dest_dir=dest_dir
            )
        return self.directories[source_dir]

    def is_completed(self, source_dir: str) -> bool:
        """Check if directory was already successfully processed."""
        if source_dir in self.directories:
            return self.directories[source_dir].status == 'completed'
        return False


def sha256_of_file(path: Path) -> str:
    """Calculate SHA256 hash of a file."""
    h = hashlib.sha256()
    with path.open('rb') as f:
        while True:
            chunk = f.read(CHUNK)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def get_size_str(size_bytes: int) -> str:
    """Convert bytes to human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"


def rsync_directory(
    source: Path,
    dest: Path,
    logger: logging.Logger,
    log_dir: Path,
    dry_run: bool = False,
    exclude: Optional[List[str]] = None
) -> tuple[int, str, Path, List[str]]:
    """
    Rsync a directory with resumable options.

    Returns (exit_code, command_string, rsync_log_path, error_lines)
    """
    # Ensure destination parent exists
    dest.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        'rsync',
        '-av',              # archive mode, verbose
        '--partial',        # keep partially transferred files for resume
        '--progress',       # show progress
        '--checksum',       # use checksum to verify transfers
        '--human-readable',
        '--itemize-changes',  # detailed output of changes
    ]

    # Add exclude patterns
    if exclude:
        for pattern in exclude:
            cmd.append(f'--exclude={pattern}')

    cmd.extend([
        str(source) + '/',  # trailing slash = contents of source
        str(dest) + '/'
    ])

    if dry_run:
        cmd.insert(1, '--dry-run')

    cmd_str = ' '.join(cmd)
    logger.info(f"Running: {cmd_str}")

    # Create rsync-specific log file
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    rsync_log_path = log_dir / f'rsync_{source.name}_{timestamp}.log'

    error_lines = []

    with open(rsync_log_path, 'w') as log_file:
        log_file.write(f"Command: {cmd_str}\n")
        log_file.write(f"Started: {datetime.datetime.now().isoformat()}\n")
        log_file.write("=" * 60 + "\n\n")

        # Run rsync with output captured and streamed
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1  # Line buffered
        )

        for line in process.stdout:
            log_file.write(line)
            log_file.flush()

            # Detect error lines for summary
            line_lower = line.lower()
            if any(err in line_lower for err in ['error', 'permission denied', 'failed', 'cannot']):
                error_lines.append(line.strip())
                logger.warning(f"rsync: {line.strip()}")

        process.wait()

        log_file.write("\n" + "=" * 60 + "\n")
        log_file.write(f"Finished: {datetime.datetime.now().isoformat()}\n")
        log_file.write(f"Exit code: {process.returncode}\n")
        if process.returncode in RSYNC_EXIT_CODES:
            log_file.write(f"Exit meaning: {RSYNC_EXIT_CODES[process.returncode]}\n")

    logger.info(f"rsync log written to: {rsync_log_path}")

    return process.returncode, cmd_str, rsync_log_path, error_lines


def rsync_root_files(
    source: Path,
    dest: Path,
    logger: logging.Logger,
    log_dir: Path,
    dry_run: bool = False,
    exclude: Optional[List[str]] = None
) -> tuple[int, str, Path, List[str]]:
    """
    Rsync only the files (not directories) from the source root to destination.

    This handles files that are directly in the source directory but not in subdirectories.
    Returns (exit_code, command_string, rsync_log_path, error_lines)
    """
    # Check if there are any files in the root (not directories)
    root_files = [f for f in source.iterdir() if f.is_file()]
    if not root_files:
        logger.info("No root-level files to copy")
        return 0, "", Path("/dev/null"), []

    logger.info(f"Found {len(root_files)} root-level files to copy")

    # Ensure destination exists
    dest.mkdir(parents=True, exist_ok=True)

    cmd = [
        'rsync',
        '-av',              # archive mode, verbose
        '--partial',        # keep partially transferred files for resume
        '--progress',       # show progress
        '--checksum',       # use checksum to verify transfers
        '--human-readable',
        '--itemize-changes',  # detailed output of changes
        '--exclude=*/',     # exclude all directories (only copy files)
    ]

    # Add exclude patterns
    if exclude:
        for pattern in exclude:
            cmd.append(f'--exclude={pattern}')

    cmd.extend([
        str(source) + '/',  # trailing slash = contents of source
        str(dest) + '/'
    ])

    if dry_run:
        cmd.insert(1, '--dry-run')

    cmd_str = ' '.join(cmd)
    logger.info(f"Running: {cmd_str}")

    # Create rsync-specific log file
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    rsync_log_path = log_dir / f'rsync_root_files_{timestamp}.log'

    error_lines = []

    with open(rsync_log_path, 'w') as log_file:
        log_file.write(f"Command: {cmd_str}\n")
        log_file.write(f"Started: {datetime.datetime.now().isoformat()}\n")
        log_file.write(f"Root-level files: {len(root_files)}\n")
        log_file.write("=" * 60 + "\n\n")

        # Run rsync with output captured and streamed
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1  # Line buffered
        )

        for line in process.stdout:
            log_file.write(line)
            log_file.flush()

            # Detect error lines for summary
            line_lower = line.lower()
            if any(err in line_lower for err in ['error', 'permission denied', 'failed', 'cannot']):
                error_lines.append(line.strip())
                logger.warning(f"rsync: {line.strip()}")

        process.wait()

        log_file.write("\n" + "=" * 60 + "\n")
        log_file.write(f"Finished: {datetime.datetime.now().isoformat()}\n")
        log_file.write(f"Exit code: {process.returncode}\n")
        if process.returncode in RSYNC_EXIT_CODES:
            log_file.write(f"Exit meaning: {RSYNC_EXIT_CODES[process.returncode]}\n")

    logger.info(f"rsync log written to: {rsync_log_path}")

    return process.returncode, cmd_str, rsync_log_path, error_lines


def compress_root_files(
    dest: Path,
    extensions: List[str],
    algo: str,
    level: int,
    use_pigz: bool,
    pigz_threads: int,
    workers: int,
    verify: bool,
    logger: logging.Logger,
    show_progress: bool = True
) -> tuple[int, int, int]:
    """
    Compress root-level files in the destination directory.

    Returns (total_source_size, total_compressed_size, error_count)
    """
    files_to_compress = []
    normalized_ext = {e.lower().lstrip('.') for e in extensions}

    for f in dest.iterdir():
        if f.is_file():
            suffix = f.suffix.lower().lstrip('.')
            if suffix in normalized_ext:
                files_to_compress.append(f)

    if not files_to_compress:
        logger.info("No root-level files to compress")
        return 0, 0, 0

    logger.info(f"Found {len(files_to_compress)} root-level files to compress")

    # Prepare worker arguments
    work_items = [
        (f, f, algo, level, use_pigz, pigz_threads, verify)
        for f in files_to_compress
    ]

    total_source_size = 0
    total_compressed_size = 0
    error_count = 0

    # Calculate total size for progress bar
    total_bytes = sum(f.stat().st_size for f in files_to_compress)

    with multiprocessing.Pool(processes=workers) as pool:
        # Setup progress bar for file compression
        if show_progress and TQDM_AVAILABLE:
            pbar = tqdm(
                total=total_bytes,
                unit='B',
                unit_scale=True,
                desc="Compressing root files",
                leave=False
            )
        else:
            pbar = None

        for file_record in pool.imap_unordered(compress_file_worker, work_items):
            total_source_size += file_record.original_size
            if file_record.compressed_size:
                total_compressed_size += file_record.compressed_size

            # Update progress bar
            if pbar:
                pbar.update(file_record.original_size)

            if file_record.status in ('compressed', 'verified'):
                ratio = (1 - file_record.compressed_size / file_record.original_size) * 100 if file_record.original_size > 0 else 0
                logger.info(f"OK {file_record.source_path} -> {get_size_str(file_record.compressed_size)} ({ratio:.1f}% reduction)")
            elif file_record.status == 'skipped':
                logger.debug(f"SKIP {file_record.source_path} (already compressed)")
            else:
                logger.error(f"ERROR {file_record.source_path}: {file_record.error_message}")
                error_count += 1

        if pbar:
            pbar.close()

    return total_source_size, total_compressed_size, error_count


def compress_file_worker(
    args: tuple[Path, Path, str, int, bool, int, bool]
) -> FileRecord:
    """
    Worker function to compress a single file.

    Args tuple: (source_in_dest, final_path, algo, level, use_pigz, pigz_threads, verify)
    """
    source_in_dest, final_path, algo, level, use_pigz, pigz_threads, verify = args

    record = FileRecord(
        source_path=str(source_in_dest),
        dest_path=str(final_path),
        original_size=source_in_dest.stat().st_size
    )

    try:
        # Calculate source hash if verifying
        if verify:
            record.source_sha256 = sha256_of_file(source_in_dest)

        # Determine output path
        if algo == 'gzip':
            out_suffix = '.gz'
        elif algo == 'xz':
            out_suffix = '.xz'
        else:
            raise ValueError(f'Unsupported algorithm: {algo}')

        compressed_path = source_in_dest.with_name(source_in_dest.name + out_suffix)
        tmp_path = compressed_path.with_suffix(compressed_path.suffix + '.tmp')

        # Skip if already compressed
        if compressed_path.exists():
            record.status = 'skipped'
            record.dest_path = str(compressed_path)
            record.compressed_size = compressed_path.stat().st_size
            return record

        # Compress
        if algo == 'gzip':
            if use_pigz:
                with tmp_path.open('wb') as f_out:
                    subprocess.run(
                        ['pigz', '-c', f'-p{pigz_threads}', f'-{level}', str(source_in_dest)],
                        stdout=f_out,
                        check=True
                    )
            else:
                with source_in_dest.open('rb') as f_in:
                    with gzip.open(tmp_path, 'wb', compresslevel=level) as gz:
                        shutil.copyfileobj(f_in, gz, CHUNK)
        else:  # xz
            with source_in_dest.open('rb') as f_in:
                with lzma.open(tmp_path, 'wb', preset=level) as xz:
                    shutil.copyfileobj(f_in, xz, CHUNK)

        # Atomic rename
        tmp_path.replace(compressed_path)
        record.compressed_size = compressed_path.stat().st_size
        record.dest_path = str(compressed_path)
        record.status = 'compressed'

        # Verify by decompressing and hashing
        if verify:
            h = hashlib.sha256()
            open_func = gzip.open if algo == 'gzip' else lzma.open
            with open_func(compressed_path, 'rb') as f:
                while True:
                    chunk = f.read(CHUNK)
                    if not chunk:
                        break
                    h.update(chunk)
            record.decompressed_sha256 = h.hexdigest()

            if record.decompressed_sha256 != record.source_sha256:
                record.status = 'error'
                record.error_message = 'Verification failed: hash mismatch'
                compressed_path.unlink(missing_ok=True)
                return record

            record.status = 'verified'

        # Remove uncompressed copy in dest (keep compressed version)
        source_in_dest.unlink()

    except Exception as e:
        record.status = 'error'
        record.error_message = str(e)

    return record


def find_compressible_files(directory: Path, extensions: List[str]) -> List[Path]:
    """Find all files with matching extensions in directory."""
    normalized = {e.lower().lstrip('.') for e in extensions}
    files = []
    for p in directory.rglob('*'):
        if p.is_file():
            suffix = p.suffix.lower().lstrip('.')
            if suffix in normalized:
                files.append(p)
    return files


def process_directory(
    source_dir: Path,
    dest_dir: Path,
    extensions: List[str],
    algo: str,
    level: int,
    use_pigz: bool,
    pigz_threads: int,
    workers: int,
    verify: bool,
    dry_run: bool,
    logger: logging.Logger,
    migration_log: MigrationLog,
    log_dir: Path,
    continue_on_partial: bool = True,
    exclude: Optional[List[str]] = None,
    show_progress: bool = True
) -> DirectoryRecord:
    """Process a single directory: rsync then compress."""

    record = migration_log.get_or_create_dir(str(source_dir), str(dest_dir))
    record.start_time = datetime.datetime.now().isoformat()

    logger.info(f"{'[DRY RUN] ' if dry_run else ''}Processing directory: {source_dir}")

    # Step 1: Rsync
    record.status = 'syncing'
    migration_log.save()

    exit_code, cmd_str, rsync_log_path, error_lines = rsync_directory(
        source_dir, dest_dir, logger, log_dir, dry_run, exclude
    )
    record.rsync_command = cmd_str
    record.rsync_exit_code = exit_code
    record.rsync_log_file = str(rsync_log_path)
    record.rsync_errors = error_lines

    # Determine if we should continue based on exit code
    # Exit codes 23 and 24 are partial transfers - some files succeeded
    is_partial = exit_code in (23, 24)
    is_fatal = exit_code != 0 and not is_partial

    if is_fatal:
        record.status = 'error'
        exit_meaning = RSYNC_EXIT_CODES.get(exit_code, "Unknown error")
        record.error_message = f'rsync failed with exit code {exit_code}: {exit_meaning}'
        record.end_time = datetime.datetime.now().isoformat()
        migration_log.save()
        logger.error(f"rsync failed for {source_dir}: exit code {exit_code} ({exit_meaning})")
        logger.error(f"See rsync log for details: {rsync_log_path}")
        if error_lines:
            logger.error(f"Errors encountered ({len(error_lines)} total):")
            for err in error_lines[:10]:  # Show first 10 errors
                logger.error(f"  {err}")
            if len(error_lines) > 10:
                logger.error(f"  ... and {len(error_lines) - 10} more errors")
        return record

    if is_partial:
        exit_meaning = RSYNC_EXIT_CODES.get(exit_code, "Unknown error")
        logger.warning(f"rsync partial transfer for {source_dir}: exit code {exit_code} ({exit_meaning})")
        logger.warning(f"See rsync log for details: {rsync_log_path}")
        if error_lines:
            logger.warning(f"Files with errors ({len(error_lines)} total):")
            for err in error_lines[:10]:
                logger.warning(f"  {err}")
            if len(error_lines) > 10:
                logger.warning(f"  ... and {len(error_lines) - 10} more errors")

        if not continue_on_partial:
            record.status = 'partial'
            record.error_message = f'rsync partial transfer: {exit_meaning}. {len(error_lines)} errors.'
            record.end_time = datetime.datetime.now().isoformat()
            migration_log.save()
            return record

        logger.info("Continuing with compression of successfully transferred files...")

    if dry_run:
        record.status = 'dry_run'
        record.end_time = datetime.datetime.now().isoformat()
        migration_log.save()
        return record

    # Step 2: Find and compress files
    record.status = 'compressing'
    migration_log.save()

    files_to_compress = find_compressible_files(dest_dir, extensions)
    logger.info(f"Found {len(files_to_compress)} files to compress in {dest_dir}")

    if not files_to_compress:
        record.status = 'completed'
        record.end_time = datetime.datetime.now().isoformat()
        migration_log.save()
        return record

    # Prepare worker arguments
    work_items = [
        (f, f, algo, level, use_pigz, pigz_threads, verify)
        for f in files_to_compress
    ]

    # Process files in parallel
    record.files = []
    record.total_source_size = 0
    record.total_compressed_size = 0

    # Calculate total size for progress bar
    total_bytes = sum(f.stat().st_size for f in files_to_compress)

    with multiprocessing.Pool(processes=workers) as pool:
        # Setup progress bar for file compression
        if show_progress and TQDM_AVAILABLE:
            pbar = tqdm(
                total=total_bytes,
                unit='B',
                unit_scale=True,
                desc=f"Compressing {dest_dir.name}",
                leave=False
            )
        else:
            pbar = None

        for file_record in pool.imap_unordered(compress_file_worker, work_items):
            record.files.append(file_record)
            record.total_source_size += file_record.original_size
            if file_record.compressed_size:
                record.total_compressed_size += file_record.compressed_size

            # Update progress bar
            if pbar:
                pbar.update(file_record.original_size)

            if file_record.status in ('compressed', 'verified'):
                ratio = (1 - file_record.compressed_size / file_record.original_size) * 100 if file_record.original_size > 0 else 0
                logger.info(f"OK {file_record.source_path} -> {get_size_str(file_record.compressed_size)} ({ratio:.1f}% reduction)")
            elif file_record.status == 'skipped':
                logger.debug(f"SKIP {file_record.source_path} (already compressed)")
            else:
                logger.error(f"ERROR {file_record.source_path}: {file_record.error_message}")

            # Save progress periodically
            migration_log.save()

        if pbar:
            pbar.close()

    # Check for errors
    errors = [f for f in record.files if f.status == 'error']
    if errors or is_partial:
        record.status = 'completed_with_errors'
        error_parts = []
        if is_partial:
            error_parts.append(f'rsync partial transfer ({len(record.rsync_errors)} files)')
        if errors:
            error_parts.append(f'{len(errors)} compression failures')
        record.error_message = '; '.join(error_parts)
    else:
        record.status = 'completed'

    record.end_time = datetime.datetime.now().isoformat()
    migration_log.save()

    # Summary
    if record.total_source_size > 0:
        ratio = (1 - record.total_compressed_size / record.total_source_size) * 100
        logger.info(
            f"Directory complete: {get_size_str(record.total_source_size)} -> "
            f"{get_size_str(record.total_compressed_size)} ({ratio:.1f}% reduction)"
        )

    return record


def matches_exclude_pattern(name: str, patterns: Optional[List[str]]) -> bool:
    """Check if a directory name matches any exclude pattern."""
    if not patterns:
        return False
    for pattern in patterns:
        # Remove trailing slash for comparison
        pattern_clean = pattern.rstrip('/')
        # Direct match
        if name == pattern_clean:
            return True
        # Wildcard match (simple glob-style)
        if pattern_clean.startswith('*') and name.endswith(pattern_clean[1:]):
            return True
        if pattern_clean.endswith('*') and name.startswith(pattern_clean[:-1]):
            return True
    return False


def get_subdirectories(root: Path, max_depth: int = 1, exclude: Optional[List[str]] = None) -> List[Path]:
    """Get immediate subdirectories of root, excluding specified patterns."""
    if max_depth == 0:
        return [root]

    subdirs = []
    for p in root.iterdir():
        if p.is_dir():
            # Skip directories matching exclude patterns
            if matches_exclude_pattern(p.name, exclude):
                continue
            subdirs.append(p)

    # Sort for consistent ordering
    return sorted(subdirs)


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(
        description='Migrate and compress data directory by directory',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run to see what would happen
  python migrate_compress.py --source /data/original --dest /scratch/migration --dry-run

  # Full migration with pigz compression and verification
  python migrate_compress.py --source /data/original --dest /scratch/migration --pigz --verify

  # Resume interrupted migration
  python migrate_compress.py --source /data/original --dest /scratch/migration --pigz --resume
        """
    )

    parser.add_argument('--source', '-s', type=Path, required=True,
                        help='Source root directory')
    parser.add_argument('--dest', '-d', type=Path, required=True,
                        help='Destination root directory (scratch)')
    parser.add_argument('--ext', nargs='+', default=['tsv', 'clusters', 'cluster'],
                        help='File extensions to compress (default: tsv clusters cluster)')
    parser.add_argument('--algo', choices=['gzip', 'xz'], default='gzip',
                        help='Compression algorithm (default: gzip)')
    parser.add_argument('--level', type=int, default=6,
                        help='Compression level 1-9 (default: 6)')
    parser.add_argument('--pigz', action='store_true',
                        help='Use pigz for faster gzip compression')
    parser.add_argument('--pigz-threads', type=int, default=4,
                        help='Threads per pigz process (default: 4)')
    parser.add_argument('--workers', type=int, default=max(1, multiprocessing.cpu_count() // 2),
                        help='Parallel compression workers (default: half of CPU count)')
    parser.add_argument('--verify', action='store_true',
                        help='Verify compressed files by decompressing and comparing hashes')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without making changes')
    parser.add_argument('--resume', action='store_true',
                        help='Resume from previous run, skipping completed directories')
    parser.add_argument('--log-dir', type=Path, default=None,
                        help='Directory for log files (default: dest directory)')
    parser.add_argument('--process-root', action='store_true',
                        help='Process source as single directory instead of iterating subdirs')
    parser.add_argument('--stop-on-partial', action='store_true',
                        help='Stop if rsync has partial transfer errors (default: continue with successful files)')
    parser.add_argument('--exclude', nargs='+', default=None,
                        help='Exclude directories/files matching these patterns (passed to rsync --exclude)')
    parser.add_argument('--no-progress', action='store_true',
                        help='Disable progress bars (useful for non-interactive runs)')

    args = parser.parse_args(argv)

    # Validate paths
    if not args.source.exists():
        print(f"Error: Source path does not exist: {args.source}", file=sys.stderr)
        return 1

    # Setup logging
    log_dir = args.log_dir or args.dest
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'migration_{timestamp}.log'
    json_log_file = log_dir / 'migration_state.json'

    logger = setup_logging(log_file)
    logger.info(f"Migration started: {args.source} -> {args.dest}")
    logger.info(f"Extensions to compress: {args.ext}")
    logger.info(f"Algorithm: {args.algo}, Level: {args.level}, Pigz: {args.pigz}")
    if args.exclude:
        logger.info(f"Excluding patterns: {args.exclude}")
    logger.info(f"Log file: {log_file}")
    logger.info(f"State file: {json_log_file}")

    # Load or create migration log
    migration_log = MigrationLog(json_log_file)

    # Get directories to process
    if args.process_root:
        directories = [args.source]
    else:
        directories = get_subdirectories(args.source, exclude=args.exclude)

    logger.info(f"Found {len(directories)} directories to process")
    if args.exclude:
        logger.info(f"(Excluded directories matching: {args.exclude})")

    # Determine if we should show progress bars
    show_progress = TQDM_AVAILABLE and not args.no_progress
    if args.no_progress:
        logger.info("Progress bars disabled")
    elif not TQDM_AVAILABLE:
        logger.info("Install tqdm for progress bars: conda install tqdm")

    # Process each directory
    total_source = 0
    total_compressed = 0
    completed = 0
    errors = 0
    skipped = 0

    # Handle root-level files first (when not using --process-root)
    if not args.process_root:
        logger.info("=" * 60)
        logger.info("Processing root-level files")
        logger.info("=" * 60)

        # Rsync root-level files
        exit_code, _, rsync_log, error_lines = rsync_root_files(
            source=args.source,
            dest=args.dest,
            logger=logger,
            log_dir=log_dir,
            dry_run=args.dry_run,
            exclude=args.exclude
        )

        if exit_code != 0 and exit_code not in (23, 24):
            logger.error(f"rsync root files failed with exit code {exit_code}")
            if error_lines:
                for err in error_lines[:5]:
                    logger.error(f"  {err}")

        # Compress root-level files (if not dry run)
        if not args.dry_run:
            root_source, root_compressed, root_errors = compress_root_files(
                dest=args.dest,
                extensions=args.ext,
                algo=args.algo,
                level=args.level,
                use_pigz=args.pigz,
                pigz_threads=args.pigz_threads,
                workers=args.workers,
                verify=args.verify,
                logger=logger,
                show_progress=show_progress
            )
            total_source += root_source
            total_compressed += root_compressed
            if root_errors > 0:
                errors += 1

        logger.info("Root-level files complete")
        logger.info("=" * 60)

    # Setup main progress bar for directories
    if show_progress:
        dir_pbar = tqdm(
            directories,
            desc="Overall progress",
            unit="dir",
            position=0
        )
        dir_iterator = dir_pbar
    else:
        dir_iterator = directories
        dir_pbar = None

    for i, source_subdir in enumerate(dir_iterator, 1):
        relative_path = source_subdir.relative_to(args.source) if not args.process_root else Path('.')
        dest_subdir = args.dest / relative_path

        # Check for resume
        if args.resume and migration_log.is_completed(str(source_subdir)):
            logger.info(f"[{i}/{len(directories)}] Skipping (already completed): {source_subdir}")
            skipped += 1
            continue

        logger.info(f"[{i}/{len(directories)}] Processing: {source_subdir}")

        # Update progress bar description
        if dir_pbar:
            dir_pbar.set_postfix_str(source_subdir.name)

        record = process_directory(
            source_dir=source_subdir,
            dest_dir=dest_subdir,
            extensions=args.ext,
            algo=args.algo,
            level=args.level,
            use_pigz=args.pigz,
            pigz_threads=args.pigz_threads,
            workers=args.workers,
            verify=args.verify,
            dry_run=args.dry_run,
            logger=logger,
            migration_log=migration_log,
            log_dir=log_dir,
            continue_on_partial=not args.stop_on_partial,
            exclude=args.exclude,
            show_progress=show_progress
        )

        total_source += record.total_source_size
        total_compressed += record.total_compressed_size

        if record.status in ('completed', 'dry_run'):
            completed += 1
        elif record.status in ('completed_with_errors', 'partial'):
            completed += 1
            errors += 1
        else:
            errors += 1

    # Close progress bar
    if dir_pbar:
        dir_pbar.close()

    # Final summary
    logger.info("=" * 60)
    logger.info("Migration Summary")
    logger.info("=" * 60)
    logger.info(f"Directories processed: {completed}")
    logger.info(f"Directories skipped (resume): {skipped}")
    logger.info(f"Directories with errors: {errors}")

    if total_source > 0 and not args.dry_run:
        ratio = (1 - total_compressed / total_source) * 100
        saved = total_source - total_compressed
        logger.info(f"Total original size: {get_size_str(total_source)}")
        logger.info(f"Total compressed size: {get_size_str(total_compressed)}")
        logger.info(f"Space saved: {get_size_str(saved)} ({ratio:.1f}%)")

    logger.info(f"Detailed log: {log_file}")
    logger.info(f"State file (for resume): {json_log_file}")

    return 1 if errors > 0 else 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
