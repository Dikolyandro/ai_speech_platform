from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _bootstrap_backend_path() -> None:
    backend_dir = Path(__file__).resolve().parents[1]
    if str(backend_dir) not in sys.path:
        sys.path.insert(0, str(backend_dir))


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Profile a file with the Spark Big Data service.")
    parser.add_argument("csv_path", help="Path to a CSV, CSV.GZ, JSON, JSONL, JSON.GZ, or Parquet file.")
    parser.add_argument("--format", default=None, help="Optional explicit format: csv, csv.gz, json, jsonl, json.gz, parquet.")
    parser.add_argument("--parquet-dir", default=None, help="Optional output directory for Parquet export.")
    parser.add_argument("--top-n", type=int, default=10, help="Top categorical values per column.")
    args = parser.parse_args()

    _bootstrap_backend_path()
    from app.services.spark_bigdata_service import BigDataProfilingError, profile_bigdata_file

    try:
        result = profile_bigdata_file(
            args.csv_path,
            file_format=args.format,
            processed_output_dir=args.parquet_dir,
            top_n=args.top_n,
        )
    except BigDataProfilingError as exc:
        print(json.dumps(exc.to_dict(), ensure_ascii=False, indent=2), file=sys.stderr)
        raise SystemExit(1) from None

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
