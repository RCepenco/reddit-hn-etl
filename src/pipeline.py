from extract.hn_extract import run as hn_extract_run
from transform.hn_transform import run as hn_transform_run


def main():
    print("ETL pipeline started")
    hn_extract_run(limit=50)
    hn_transform_run()


if __name__ == "__main__":
    main()

