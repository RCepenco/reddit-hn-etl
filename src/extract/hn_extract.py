import json
import os
import time
from datetime import datetime
from typing import List, Dict

import requests

BASE_URL = "https://hacker-news.firebaseio.com/v0"
TIMEOUT_SEC = 15

RAW_DIR = "data/raw/hn"
LOG_DIR = "logs"


def log_line(message: str) -> None:
    """
    Minimal production-style logger.
    Writes logs to stdout and to logs/hn_extract.log
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    line = f"{datetime.utcnow().isoformat()}Z | {message}"
    print(line)
    with open(os.path.join(LOG_DIR, "hn_extract.log"), "a", encoding="utf-8") as f:
        f.write(line + "\n")


def http_get_json(url: str, retries: int = 3, backoff_sec: float = 1.0):
    """
    HTTP GET with retries.
    Retries on transient HTTP/network failures.
    """
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=TIMEOUT_SEC)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt < retries:
                log_line(
                    f"HTTP retry {attempt}/{retries} url={url} error={exc}"
                )
                time.sleep(backoff_sec * attempt)
            else:
                raise last_error


def fetch_top_story_ids(limit: int) -> List[int]:
    """Fetch top story IDs from Hacker News."""
    ids = http_get_json(f"{BASE_URL}/topstories.json")
    return ids[:limit]


def fetch_item(item_id: int) -> Dict:
    """Fetch single Hacker News item (RAW)."""
    return http_get_json(f"{BASE_URL}/item/{item_id}.json")


def save_raw(records: List[Dict]) -> str:
    """Save RAW records as timestamped JSON file."""
    os.makedirs(RAW_DIR, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(RAW_DIR, f"hn_raw_{ts}.json")

    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return path


def run(limit: int = 50) -> None:
    """
    Airflow-ready entrypoint.
    Executes full Extract cycle.
    """
    log_line("HN Extract started")

    ids = fetch_top_story_ids(limit)
    if not ids:
        log_line("No IDs returned from HN API, nothing to save")
        return

    records: List[Dict] = []

    for idx, item_id in enumerate(ids, start=1):
        item = fetch_item(item_id)
        if not item:
            log_line(f"[{idx}/{len(ids)}] skipped id={item_id} (empty response)")
            continue

        records.append(item)
        log_line(
            f"[{idx}/{len(ids)}] fetched id={item_id} title={item.get('title')}"
        )

    output_path = save_raw(records)
    log_line(f"Saved {len(records)} records to {output_path}")


if __name__ == "__main__":
    run()
