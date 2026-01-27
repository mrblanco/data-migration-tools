# compress_files.py

Small utility to find and compress files by extension under a root directory.

Quick examples (PowerShell):

```powershell
# Dry-run: list matching files and sizes
python compress_files.py -r "C:\\path\\to\\data" --ext tsv clusters cluster --dry-run

# Compress with gzip using 4 workers, keep originals
python compress_files.py -r "C:\\path\\to\\data" --ext tsv clusters cluster --algo gzip --workers 4

# Compress with xz, verify integrity, and remove originals
python compress_files.py -r "C:\\path\\to\\data" --ext tsv clusters cluster --algo xz --verify --remove --workers 2
```

Notes:
- Test on a small subset with `--dry-run` first.
- `--verify` will hash original and decompressed output (slower, double IO) but ensures correctness before removing originals.
- For very large datasets consider running on a subset and monitoring I/O load.
