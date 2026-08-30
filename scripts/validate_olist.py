"""Validate that all expected Olist CSV files and headers are present."""

from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path

try:
    from .olist_schema import DATASETS
except ImportError:  # Direct execution: python scripts/validate_olist.py
    from olist_schema import DATASETS


def validate_directory(data_dir: Path) -> dict[str, int]:
    errors: list[str] = []
    row_counts: dict[str, int] = {}

    for dataset in DATASETS:
        path = data_dir / dataset.filename
        if not path.is_file():
            errors.append(f"Missing file: {path}")
            continue

        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            header = next(reader, None)
            expected = list(dataset.columns)
            if header != expected:
                errors.append(
                    f"Unexpected header in {dataset.filename}: {header!r}; expected {expected!r}"
                )
                continue
            row_count = sum(1 for _ in reader)

        if row_count == 0:
            errors.append(f"File contains no data rows: {path}")
        else:
            row_counts[dataset.filename] = row_count

    if errors:
        raise ValueError("Olist validation failed:\n- " + "\n- ".join(errors))

    return row_counts


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
    args = parser.parse_args()

    counts = validate_directory(args.data_dir)
    print(f"Validated {len(counts)} Olist files")
    for filename, row_count in counts.items():
        print(f"  {filename}: {row_count:,} rows")


if __name__ == "__main__":
    main()
