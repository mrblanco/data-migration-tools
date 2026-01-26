#!/usr/bin/env python3
"""
Find and compress files by extension under a root directory.

Usage examples:
python compress_files.py -r "C:\\data\\to_migrate" --ext tsv clusters cluster --algo gzip --workers 4 --dry-run
python compress_files.py -r "C:\\data\\to_migrate" --ext tsv clusters cluster --algo xz --workers 2 --remove --verify
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import lzma
import multiprocessing
import os
import shutil
import sys
from functools import partial
from gzip import GzipFile
from pathlib import Path
from typing import Iterable, List, Tuple

CHUNK = 1024 * 1024

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def find_files(root: Path, exts: List[str]) -> Iterable[Path]:
    normalized = {e.lower().lstrip('.') for e in exts}
    for p in root.rglob('*'):
        if p.is_file():
            suffix = p.suffix.lower().lstrip('.')
            if suffix in normalized:
                yield p


def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        while True:
            chunk = f.read(CHUNK)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def compress_gzip(src: Path, dst: Path, level: int = 6, use_pigz: bool = False, pigz_threads: int = 4) -> None:
    if use_pigz:
        import subprocess
        with dst.open('wb') as f_out:
            subprocess.run(
                ['pigz', '-c', f'-p{pigz_threads}', f'-{level}', str(src)],
                stdout=f_out,
                check=True
            )
    else:
        with src.open('rb') as f_in, GzipFile(filename=src.name, mode='wb', fileobj=dst.open('wb'), compresslevel=level) as gz:
            while True:
                chunk = f_in.read(CHUNK)
                if not chunk:
                    break
                gz.write(chunk)


def compress_xz(src: Path, dst: Path, preset: int = 6) -> None:
    compressor = lzma.LZMACompressor(format=lzma.FORMAT_XZ, preset=preset)
    with src.open('rb') as f_in, dst.open('wb') as f_out:
        while True:
            chunk = f_in.read(CHUNK)
            if not chunk:
                break
            data = compressor.compress(chunk)
            if data:
                f_out.write(data)
        tail = compressor.flush()
        if tail:
            f_out.write(tail)


def safe_replace(tmp_path: Path, final_path: Path) -> None:
    tmp_path.replace(final_path)


def process_file(path: Path, algo: str, level: int, verify: bool, remove: bool, use_pigz: bool = False, pigz_threads: int = 4) -> Tuple[str, str, str]:
    try:
        if algo == 'gzip':
            out_suffix = '.gz'
        elif algo == 'xz':
            out_suffix = '.xz'
        else:
            raise ValueError('Unsupported algorithm')

        out_path = path.with_name(path.name + out_suffix)
        if out_path.exists():
            return (str(path), 'skipped', 'destination exists')

        tmp_path = out_path.with_suffix(out_path.suffix + '.tmp')

        orig_hash = None
        if verify:
            orig_hash = sha256_of_file(path)

        if algo == 'gzip':
            compress_gzip(path, tmp_path, level, use_pigz, pigz_threads)
        else:
            compress_xz(path, tmp_path, level)

        safe_replace(tmp_path, out_path)

        if verify:
            # verify by decompressing stream and hashing
            h = hashlib.sha256()
            if algo == 'gzip':
                import gzip
                with gzip.open(out_path, 'rb') as f:
                    while True:
                        chunk = f.read(CHUNK)
                        if not chunk:
                            break
                        h.update(chunk)
            else:
                import lzma as _lz
                with _lz.open(out_path, 'rb') as f:
                    while True:
                        chunk = f.read(CHUNK)
                        if not chunk:
                            break
                        h.update(chunk)
            if h.hexdigest() != orig_hash:
                out_path.unlink(missing_ok=True)
                return (str(path), 'failed', 'verification mismatch')

        if remove:
            try:
                path.unlink()
            except Exception as e:
                return (str(path), 'warning', f'compressed but failed to remove original: {e}')

        return (str(path), 'ok', str(out_path))
    except Exception as e:
        return (str(path), 'error', str(e))


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description='Find and compress files by extension')
    parser.add_argument('-r', '--root', required=True, help='Root directory to search')
    parser.add_argument('--ext', nargs='+', default=['tsv', 'clusters', 'cluster'], help='Extensions to match')
    parser.add_argument('--algo', choices=['gzip', 'xz'], default='gzip', help='Compression algorithm')
    parser.add_argument('--level', type=int, default=6, help='Compression level/preset (1-9)')
    parser.add_argument('--workers', type=int, default=max(1, multiprocessing.cpu_count() // 2), help='Parallel worker processes')
    parser.add_argument('--dry-run', action='store_true', help='Only list files and sizes, do not compress')
    parser.add_argument('--remove', action='store_true', help='Remove original files after successful compression')
    parser.add_argument('--verify', action='store_true', help='Verify compressed output matches original (slower)')
    parser.add_argument('--pigz', action='store_true', help='Use pigz instead of Python gzip (much faster, requires pigz installed)')
    parser.add_argument('--pigz-threads', type=int, default=4, help='Number of threads per pigz process (default: 4)')
    args = parser.parse_args(argv)

    root = Path(args.root)
    if not root.exists():
        logging.error('Root path does not exist: %s', root)
        return 2

    files = list(find_files(root, args.ext))
    total_size = sum(p.stat().st_size for p in files)
    logging.info('Found %d files matching %s (total size %.2f GB)', len(files), args.ext, total_size / (1024**3))

    if args.dry_run:
        for p in files:
            logging.info('DRY %s (%.2f MB)', p, p.stat().st_size / (1024**2))
        return 0

    if not files:
        logging.info('No files to process.')
        return 0

    pool = multiprocessing.Pool(processes=args.workers)
    try:
        func = partial(process_file, algo=args.algo, level=args.level, verify=args.verify, remove=args.remove, use_pigz=args.pigz, pigz_threads=args.pigz_threads)
        for path, status, info in pool.imap_unordered(func, files):
            if status == 'ok':
                logging.info('OK   %s -> %s', path, info)
            elif status == 'skipped':
                logging.info('SKIP %s: %s', path, info)
            elif status == 'warning':
                logging.warning('WARN %s: %s', path, info)
            else:
                logging.error('%s %s: %s', status.upper(), path, info)
    finally:
        pool.close()
        pool.join()

    logging.info('Completed')
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
