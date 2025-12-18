from __future__ import annotations

import logging
from pathlib import Path


def get_logger(name: str, log_file: str) -> logging.Logger:
    """
    Logger to stdout + ./logs/<file>.

    Why:
    - reproducible logs for debugging
    - same format across phases
    """
    Path("logs").mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # idempotent config

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(Path("logs") / log_file)
    fh.setFormatter(fmt)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger
