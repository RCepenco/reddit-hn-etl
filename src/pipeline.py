from __future__ import annotations

from src.extract.hn_extract import run as extract_hn
from src.load.hn_load import run as load_hn
from src.transform.hn_transform import run as transform_hn


def main() -> None:
    print("ETL pipeline started")
    extract_hn(limit=50)
    transform_hn()
    load_hn()
    print("ETL pipeline finished")


if __name__ == "__main__":
    main()
