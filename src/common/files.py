from __future__ import annotations

from pathlib import Path


def latest_file_by_name(directory: Path, pattern: str) -> Path:
    """
    Deterministic latest-file selection by filename ordering (not mtime).

    Why:
    - mtime can change after copy/chown/rebuild
    - filenames like hn_raw_YYYYMMDD_HHMMSS sort correctly
    """
    files = sorted(directory.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files found in {directory} matching {pattern}")
    return files[-1]
