"""Download the public Olist dataset from Kaggle and extract known CSV files."""

from __future__ import annotations

import argparse
import os
import shutil
import tempfile
import urllib.request
import zipfile
from pathlib import Path

from olist_schema import DATASETS_BY_FILENAME
from validate_olist import validate_directory


DEFAULT_URL = "https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce"


def is_complete(data_dir: Path) -> bool:
    return all((data_dir / filename).is_file() for filename in DATASETS_BY_FILENAME)


def download(data_dir: Path, url: str, force: bool = False) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    if is_complete(data_dir) and not force:
        print("All Olist files already exist; download skipped")
        validate_directory(data_dir)
        return

    with tempfile.TemporaryDirectory(prefix="olist-download-") as temp_dir:
        archive_path = Path(temp_dir) / "olist.zip"
        request = urllib.request.Request(url, headers={"User-Agent": "olist-medallion/1.0"})
        print(f"Downloading Olist dataset from {url}")
        with urllib.request.urlopen(request, timeout=180) as response:
            with archive_path.open("wb") as target:
                shutil.copyfileobj(response, target)

        with zipfile.ZipFile(archive_path) as archive:
            members = {Path(name).name: name for name in archive.namelist()}
            missing = sorted(set(DATASETS_BY_FILENAME) - set(members))
            if missing:
                raise ValueError(f"Archive is missing expected files: {missing}")

            for filename in DATASETS_BY_FILENAME:
                destination = data_dir / filename
                with archive.open(members[filename]) as source:
                    with destination.open("wb") as target:
                        shutil.copyfileobj(source, target)
                print(f"Extracted {destination}")

    validate_directory(data_dir)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(
            os.getenv(
                "OLIST_DATA_DIR",
                str(Path(os.getenv("AIRFLOW_HOME", "/opt/airflow")) / "data" / "raw"),
            )
        ),
    )
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    download(args.data_dir, args.url, args.force)


if __name__ == "__main__":
    main()
