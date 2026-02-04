#!/usr/bin/env python3
"""
Move data from scratch to final destination with verification and logging.

Workflow:
1. rsync from scratch to final destination with checksum verification
2. Remove source files only after successful transfer
3. Clean up empty directories in scratch
4. Log all operations for auditing

Usage:
python move_to_final.py --source /scratch/data --dest /final/destination --dry-run
python move_to_final.py --source /scratch/data --dest /final/destination
"""
from __future__ import annotations

import argparse
import datetime
import json
import logging
import subprocess
import sys
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import List, Optional, Dict

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


@dataclass
class MoveRecord:
    """Record of a move operation."""
    source: str
    dest: str
    status: str = 'pending'  # pending, moving, cleaning, completed, error
    rsync_command: str = ''
    rsync_exit_code: Optional[int] = None
    rsync_log_file: Optional[str] = None
    rsync_errors: List[str] = field(default_factory=list)
    files_moved: int = 0
    directories_cleaned: int = 0
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    error_message: Optional[str] = None


def setup_logging(log_file: Path) -> logging.Logger:
    """Setup logging to both console and file."""
    logger = logging.getLogger('move_to_final')
    logger.setLevel(logging.DEBUG)

    # Clear existing handlers
    logger.handlers.clear()

    # Console handler (INFO level)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger.addHandler(console)

    # File handler (DEBUG level)
    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger.addHandler(file_handler)

    return logger


def get_size_str(size_bytes: int) -> str:
    """Convert bytes to human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"


def rsync_move(
    source: Path,
    dest: Path,
    logger: logging.Logger,
    log_dir: Path,
    dry_run: bool = False,
    remove_source: bool = True
) -> tuple[int, str, Path, List[str], int]:
    """
    Rsync with verification and optional source removal.

    Returns (exit_code, command_string, rsync_log_path, error_lines, files_count)
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

    if remove_source and not dry_run:
        cmd.append('--remove-source-files')

    if dry_run:
        cmd.append('--dry-run')

    cmd.extend([
        str(source) + '/',  # trailing slash = contents of source
        str(dest) + '/'
    ])

    cmd_str = ' '.join(cmd)
    logger.info(f"Running: {cmd_str}")

    # Create rsync-specific log file
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    rsync_log_path = log_dir / f'rsync_move_{source.name}_{timestamp}.log'

    error_lines = []
    files_count = 0

    with open(rsync_log_path, 'w') as log_file:
        log_file.write(f"Command: {cmd_str}\n")
        log_file.write(f"Started: {datetime.datetime.now().isoformat()}\n")
        log_file.write(f"Source: {source}\n")
        log_file.write(f"Destination: {dest}\n")
        log_file.write(f"Remove source files: {remove_source}\n")
        log_file.write(f"Dry run: {dry_run}\n")
        log_file.write("=" * 60 + "\n\n")

        # Run rsync with output captured and streamed
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )

        for line in process.stdout:
            log_file.write(line)
            log_file.flush()

            # Count transferred files (lines starting with > or < indicate transfers)
            if line.startswith('>f') or line.startswith('<f'):
                files_count += 1

            # Detect error lines
            line_lower = line.lower()
            if any(err in line_lower for err in ['error', 'permission denied', 'failed', 'cannot']):
                error_lines.append(line.strip())
                logger.warning(f"rsync: {line.strip()}")

        process.wait()

        log_file.write("\n" + "=" * 60 + "\n")
        log_file.write(f"Finished: {datetime.datetime.now().isoformat()}\n")
        log_file.write(f"Exit code: {process.returncode}\n")
        log_file.write(f"Files transferred: {files_count}\n")
        if process.returncode in RSYNC_EXIT_CODES:
            log_file.write(f"Exit meaning: {RSYNC_EXIT_CODES[process.returncode]}\n")

    logger.info(f"rsync log written to: {rsync_log_path}")

    return process.returncode, cmd_str, rsync_log_path, error_lines, files_count


def cleanup_empty_dirs(source: Path, logger: logging.Logger, dry_run: bool = False) -> int:
    """Remove empty directories from source after file removal."""
    count = 0

    if dry_run:
        # Just count what would be removed
        for dirpath in sorted(source.rglob('*'), reverse=True):
            if dirpath.is_dir() and not any(dirpath.iterdir()):
                logger.info(f"[DRY RUN] Would remove empty directory: {dirpath}")
                count += 1
        # Check source itself
        if source.is_dir() and not any(source.iterdir()):
            logger.info(f"[DRY RUN] Would remove empty source directory: {source}")
            count += 1
        return count

    # Actually remove empty directories (bottom-up)
    for dirpath in sorted(source.rglob('*'), reverse=True):
        if dirpath.is_dir():
            try:
                if not any(dirpath.iterdir()):
                    dirpath.rmdir()
                    logger.debug(f"Removed empty directory: {dirpath}")
                    count += 1
            except OSError as e:
                logger.warning(f"Could not remove directory {dirpath}: {e}")

    # Try to remove source directory itself if empty
    try:
        if source.is_dir() and not any(source.iterdir()):
            source.rmdir()
            logger.info(f"Removed empty source directory: {source}")
            count += 1
    except OSError as e:
        logger.warning(f"Could not remove source directory {source}: {e}")

    return count


def save_record(record: MoveRecord, log_path: Path):
    """Save move record to JSON file."""
    data = {
        'last_updated': datetime.datetime.now().isoformat(),
        'move': asdict(record)
    }
    tmp_path = log_path.with_suffix('.tmp')
    with open(tmp_path, 'w') as f:
        json.dump(data, f, indent=2)
    tmp_path.replace(log_path)


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(
        description='Move data from scratch to final destination with verification',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run to see what would happen
  python move_to_final.py --source /scratch/data --dest /final/destination --dry-run

  # Move with verification (removes source after successful transfer)
  python move_to_final.py --source /scratch/data --dest /final/destination

  # Move without removing source (copy mode)
  python move_to_final.py --source /scratch/data --dest /final/destination --keep-source
        """
    )

    parser.add_argument('--source', '-s', type=Path, required=True,
                        help='Source directory (scratch location)')
    parser.add_argument('--dest', '-d', type=Path, required=True,
                        help='Destination directory (final location)')
    parser.add_argument('--log-dir', type=Path, default=None,
                        help='Directory for log files (default: source parent directory)')
    parser.add_argument('--keep-source', action='store_true',
                        help='Keep source files after transfer (copy instead of move)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without making changes')

    args = parser.parse_args(argv)

    # Validate source
    if not args.source.exists():
        print(f"Error: Source path does not exist: {args.source}", file=sys.stderr)
        return 1

    # Setup logging
    log_dir = args.log_dir or args.source.parent
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'move_{args.source.name}_{timestamp}.log'
    json_log_file = log_dir / f'move_{args.source.name}_{timestamp}.json'

    logger = setup_logging(log_file)

    # Create record
    record = MoveRecord(
        source=str(args.source),
        dest=str(args.dest),
        start_time=datetime.datetime.now().isoformat()
    )

    logger.info("=" * 60)
    logger.info("Move to Final Destination")
    logger.info("=" * 60)
    logger.info(f"Source: {args.source}")
    logger.info(f"Destination: {args.dest}")
    logger.info(f"Keep source: {args.keep_source}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info(f"Log file: {log_file}")
    logger.info(f"State file: {json_log_file}")
    logger.info("=" * 60)

    # Step 1: rsync move
    record.status = 'moving'
    save_record(record, json_log_file)

    exit_code, cmd_str, rsync_log_path, error_lines, files_count = rsync_move(
        source=args.source,
        dest=args.dest,
        logger=logger,
        log_dir=log_dir,
        dry_run=args.dry_run,
        remove_source=not args.keep_source
    )

    record.rsync_command = cmd_str
    record.rsync_exit_code = exit_code
    record.rsync_log_file = str(rsync_log_path)
    record.rsync_errors = error_lines
    record.files_moved = files_count

    # Check rsync result
    is_partial = exit_code in (23, 24)
    is_fatal = exit_code != 0 and not is_partial

    if is_fatal:
        exit_meaning = RSYNC_EXIT_CODES.get(exit_code, "Unknown error")
        record.status = 'error'
        record.error_message = f'rsync failed with exit code {exit_code}: {exit_meaning}'
        record.end_time = datetime.datetime.now().isoformat()
        save_record(record, json_log_file)

        logger.error(f"Move failed: exit code {exit_code} ({exit_meaning})")
        logger.error(f"See rsync log for details: {rsync_log_path}")
        if error_lines:
            logger.error(f"Errors ({len(error_lines)} total):")
            for err in error_lines[:10]:
                logger.error(f"  {err}")
        return 1

    if is_partial:
        exit_meaning = RSYNC_EXIT_CODES.get(exit_code, "Unknown error")
        logger.warning(f"Partial transfer: exit code {exit_code} ({exit_meaning})")
        logger.warning(f"Some files may not have been moved. Check: {rsync_log_path}")

    logger.info(f"Files transferred: {files_count}")

    # Step 2: Cleanup empty directories
    if not args.keep_source:
        record.status = 'cleaning'
        save_record(record, json_log_file)

        logger.info("Cleaning up empty directories...")
        dirs_cleaned = cleanup_empty_dirs(args.source, logger, args.dry_run)
        record.directories_cleaned = dirs_cleaned
        logger.info(f"Empty directories removed: {dirs_cleaned}")

    # Complete
    if is_partial:
        record.status = 'completed_with_errors'
        record.error_message = f'Partial transfer ({len(error_lines)} errors)'
    else:
        record.status = 'completed'

    record.end_time = datetime.datetime.now().isoformat()
    save_record(record, json_log_file)

    # Summary
    logger.info("=" * 60)
    logger.info("Move Summary")
    logger.info("=" * 60)
    logger.info(f"Status: {record.status}")
    logger.info(f"Files moved: {record.files_moved}")
    logger.info(f"Directories cleaned: {record.directories_cleaned}")
    logger.info(f"rsync log: {rsync_log_path}")
    logger.info(f"State file: {json_log_file}")
    logger.info("=" * 60)

    return 1 if is_partial else 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
