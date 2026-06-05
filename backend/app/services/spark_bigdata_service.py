from __future__ import annotations

import os
import platform
import csv
import io
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampType,
)


SUPPORTED_FORMATS = {"csv", "csv.gz", "json", "jsonl", "json.gz", "parquet"}
logger = logging.getLogger(__name__)


class BigDataProfilingError(RuntimeError):
    """Structured error raised when Spark profiling cannot complete."""

    def __init__(self, message: str, *, stage: str, details: Optional[dict[str, Any]] = None):
        super().__init__(message)
        self.stage = stage
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": str(self),
            "stage": self.stage,
            "details": self.details,
        }


@dataclass(frozen=True)
class SparkBigDataProfiler:
    app_name: str = "ai-analytics-bigdata-profiler"
    master: str = "local[*]"

    def get_spark(self) -> SparkSession:
        _ensure_local_spark_env()
        return (
            SparkSession.builder.appName(self.app_name)
            .master(self.master)
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )

    def profile_file(
        self,
        raw_file_path: str | Path,
        *,
        file_format: str | None = None,
        processed_output_dir: str | Path | None = None,
        top_n: int = 10,
    ) -> dict[str, Any]:
        raw_path = Path(raw_file_path).expanduser().resolve()
        if not raw_path.exists() or not raw_path.is_file():
            raise BigDataProfilingError(
                f"Big Data file not found: {raw_path}",
                stage="validate_input",
                details={"raw_file_path": str(raw_path)},
            )
        detected_format = _normalize_format(file_format or _detect_file_format(raw_path))

        safe_top_n = max(1, min(int(top_n or 10), 100))
        spark = self.get_spark()

        try:
            df = self._read_dataset(spark, raw_path, detected_format)
            row_count = int(df.count())
            columns = list(df.columns)
            schema_json = self._schema_json(df)
            profile_json = self._profile_dataframe(df, row_count=row_count, top_n=safe_top_n)

            result: dict[str, Any] = {
                "row_count": row_count,
                "column_count": len(columns),
                "schema_json": schema_json,
                "profile_json": profile_json,
                "processed_path": None,
            }

            if processed_output_dir is not None:
                processed_path = Path(processed_output_dir).expanduser().resolve()
                processed_path.mkdir(parents=True, exist_ok=True)
                self._write_parquet(df, processed_path)
                result["processed_path"] = str(processed_path)

            return result
        except BigDataProfilingError:
            raise
        except Exception as exc:
            if detected_format in {"json", "jsonl", "json.gz"}:
                message = (
                    "Spark JSON profiling failed. Ensure JSON input is line-delimited "
                    "JSON/JSONL for this local Big Data MVP."
                )
            else:
                message = f"Spark profiling failed: {exc}"
            raise BigDataProfilingError(
                message,
                stage="profile_file",
                details={
                    "raw_file_path": str(raw_path),
                    "format": detected_format,
                    "type": type(exc).__name__,
                    "cause": str(exc),
                },
            ) from exc

    def profile_csv(
        self,
        raw_csv_path: str | Path,
        *,
        processed_output_dir: str | Path | None = None,
        top_n: int = 10,
    ) -> dict[str, Any]:
        return self.profile_file(
            raw_csv_path,
            file_format="csv",
            processed_output_dir=processed_output_dir,
            top_n=top_n,
        )

    def create_chat_sample_csv(
        self,
        raw_file_path: str | Path,
        *,
        file_format: str | None = None,
        columns: list[str],
        row_limit: int,
        sampling_mode: str = "first",
        filters: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        raw_path = Path(raw_file_path).expanduser().resolve()
        if not raw_path.exists() or not raw_path.is_file():
            raise BigDataProfilingError(
                f"Big Data file not found: {raw_path}",
                stage="validate_input",
                details={"raw_file_path": str(raw_path)},
            )

        detected_format = _normalize_format(file_format or _detect_file_format(raw_path))
        safe_row_limit = max(1, min(int(row_limit or 5000), 50000))
        safe_sampling_mode = "random" if sampling_mode == "random" else "first"
        safe_filters = filters or []

        spark = self.get_spark()
        try:
            df = self._read_dataset(spark, raw_path, detected_format)
            available = set(df.columns)
            missing_columns = [name for name in columns if name not in available]
            if missing_columns:
                raise BigDataProfilingError(
                    "Selected columns are not present in the Big Data file.",
                    stage="select_columns",
                    details={"missing_columns": missing_columns},
                )

            for item in safe_filters:
                df = self._apply_filter(df, item)

            selected_columns = columns or list(df.columns)
            if safe_sampling_mode == "random":
                df = df.orderBy(F.rand())
            df = df.select(*[_spark_col(name) for name in selected_columns]).limit(safe_row_limit)

            rows = df.collect()
            csv_text = _rows_to_csv_text(selected_columns, rows)
            return {
                "csv_text": csv_text,
                "row_count": len(rows),
                "columns": selected_columns,
                "filters": safe_filters,
                "sampling_mode": safe_sampling_mode,
                "row_limit": safe_row_limit,
            }
        except BigDataProfilingError:
            raise
        except Exception as exc:
            raise BigDataProfilingError(
                f"Spark chat sample creation failed: {exc}",
                stage="create_chat_sample",
                details={
                    "raw_file_path": str(raw_path),
                    "format": detected_format,
                    "type": type(exc).__name__,
                    "cause": str(exc),
                },
            ) from exc

    def export_to_parquet(
        self,
        raw_file_path: str | Path,
        *,
        file_format: str | None = None,
        processed_output_dir: str | Path,
        dataset_id: int | None = None,
    ) -> dict[str, Any]:
        raw_path = Path(raw_file_path).expanduser().resolve()
        if not raw_path.exists() or not raw_path.is_file():
            raise BigDataProfilingError(
                f"Big Data file not found: {raw_path}",
                stage="validate_input",
                details={"raw_file_path": str(raw_path), "dataset_id": dataset_id},
            )

        detected_format = _normalize_format(file_format or _detect_file_format(raw_path))
        processed_path = Path(processed_output_dir).expanduser().resolve()
        processed_path.mkdir(parents=True, exist_ok=True)

        logger.info(
            "bigdata.parquet_export.start dataset_id=%s format=%s raw_path=%s reader=%s output_path=%s",
            dataset_id,
            detected_format,
            raw_path,
            _reader_name_for_format(detected_format),
            processed_path,
        )

        spark = self.get_spark()
        try:
            df = self._read_dataset(spark, raw_path, detected_format)
            self._write_parquet(df, processed_path)
            logger.info(
                "bigdata.parquet_export.success dataset_id=%s format=%s output_path=%s",
                dataset_id,
                detected_format,
                processed_path,
            )
            return {
                "processed_path": str(processed_path),
                "format": detected_format,
                "raw_file_path": str(raw_path),
                "reader": _reader_name_for_format(detected_format),
            }
        except BigDataProfilingError:
            logger.exception(
                "bigdata.parquet_export.failure dataset_id=%s format=%s raw_path=%s output_path=%s",
                dataset_id,
                detected_format,
                raw_path,
                processed_path,
            )
            raise
        except Exception as exc:
            logger.exception(
                "bigdata.parquet_export.failure dataset_id=%s format=%s raw_path=%s output_path=%s",
                dataset_id,
                detected_format,
                raw_path,
                processed_path,
            )
            raise BigDataProfilingError(
                f"Spark Parquet export failed: {exc}",
                stage="write_parquet",
                details={
                    "dataset_id": dataset_id,
                    "raw_file_path": str(raw_path),
                    "processed_output_dir": str(processed_path),
                    "format": detected_format,
                    "reader": _reader_name_for_format(detected_format),
                    "type": type(exc).__name__,
                    "cause": str(exc),
                },
            ) from exc

    def _apply_filter(self, df: DataFrame, item: dict[str, Any]) -> DataFrame:
        column = str(item.get("column") or "")
        operator = str(item.get("operator") or "")
        value = item.get("value")
        if column not in set(df.columns):
            raise BigDataProfilingError(
                "Filter column is not present in the Big Data file.",
                stage="filter",
                details={"column": column},
            )

        col = _spark_col(column)
        if operator == "=":
            condition = col == F.lit(value)
        elif operator == "!=":
            condition = col != F.lit(value)
        elif operator == ">":
            condition = col > F.lit(value)
        elif operator == ">=":
            condition = col >= F.lit(value)
        elif operator == "<":
            condition = col < F.lit(value)
        elif operator == "<=":
            condition = col <= F.lit(value)
        elif operator == "contains":
            condition = col.cast("string").contains(str(value))
        else:
            raise BigDataProfilingError(
                "Unsupported filter operator.",
                stage="filter",
                details={"operator": operator},
            )
        return df.where(condition)

    def _read_dataset(self, spark: SparkSession, raw_path: Path, file_format: str) -> DataFrame:
        if file_format in {"csv", "csv.gz"}:
            return self._read_csv(spark, raw_path)
        if file_format in {"json", "jsonl", "json.gz"}:
            return self._read_json(spark, raw_path, file_format=file_format)
        if file_format == "parquet":
            return self._read_parquet(spark, raw_path)
        raise BigDataProfilingError(
            f"Unsupported Big Data file format: {file_format}",
            stage="detect_format",
            details={"raw_file_path": str(raw_path), "format": file_format},
        )

    def _read_csv(self, spark: SparkSession, raw_path: Path) -> DataFrame:
        try:
            return (
                spark.read.option("header", "true")
                .option("inferSchema", "true")
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .option("multiLine", "true")
                .option("escape", '"')
                .csv(str(raw_path))
            )
        except Exception as exc:
            raise BigDataProfilingError(
                f"Spark could not read CSV: {exc}",
                stage="read_csv",
                details={"raw_file_path": str(raw_path), "type": type(exc).__name__},
            ) from exc

    def _read_json(self, spark: SparkSession, raw_path: Path, *, file_format: str) -> DataFrame:
        try:
            df = (
                spark.read.option("multiLine", "false")
                .option("mode", "FAILFAST")
                .json(str(raw_path))
            )
        except Exception as exc:
            raise BigDataProfilingError(
                "Spark could not read JSON. Use line-delimited JSON/JSONL for this local Big Data MVP.",
                stage="read_json",
                details={
                    "raw_file_path": str(raw_path),
                    "format": file_format,
                    "type": type(exc).__name__,
                    "cause": str(exc),
                },
            ) from exc
        if not df.columns:
            raise BigDataProfilingError(
                "Spark could not infer JSON schema. Use line-delimited JSON/JSONL records.",
                stage="read_json",
                details={"raw_file_path": str(raw_path), "format": file_format},
            )
        return df

    def _read_parquet(self, spark: SparkSession, raw_path: Path) -> DataFrame:
        try:
            df = spark.read.parquet(str(raw_path))
        except Exception as exc:
            raise BigDataProfilingError(
                f"Spark could not read Parquet: {exc}",
                stage="read_parquet",
                details={"raw_file_path": str(raw_path), "type": type(exc).__name__},
            ) from exc
        if not df.columns:
            raise BigDataProfilingError(
                "Spark could not infer Parquet schema.",
                stage="read_parquet",
                details={"raw_file_path": str(raw_path)},
            )
        return df

    def _schema_json(self, df: DataFrame) -> dict[str, Any]:
        fields = []
        for field in df.schema.fields:
            fields.append(
                {
                    "name": field.name,
                    "type": field.dataType.simpleString(),
                    "nullable": bool(field.nullable),
                }
            )
        return {"columns": fields}

    def _profile_dataframe(self, df: DataFrame, *, row_count: int, top_n: int) -> dict[str, Any]:
        null_counts = self._null_counts(df)
        columns = []

        for field in df.schema.fields:
            name = field.name
            dtype = field.dataType
            item: dict[str, Any] = {
                "name": name,
                "type": dtype.simpleString(),
                "null_count": int(null_counts.get(name, 0)),
            }

            if _is_numeric_type(dtype):
                item["numeric"] = self._numeric_stats(df, name)
            elif _is_categorical_type(dtype):
                item["top_values"] = self._top_values(df, name, top_n=top_n)

            columns.append(item)

        return {
            "row_count": row_count,
            "columns": columns,
        }

    def _null_counts(self, df: DataFrame) -> dict[str, int]:
        exprs = [
            F.sum(
                F.when(
                    _spark_col(c).isNull()
                    | (F.trim(_spark_col(c).cast("string")) == ""),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias(c)
            for c in df.columns
        ]
        if not exprs:
            return {}
        row = df.select(*exprs).collect()[0].asDict()
        return {str(k): int(v or 0) for k, v in row.items()}

    def _numeric_stats(self, df: DataFrame, column: str) -> dict[str, Any]:
        row = (
            df.select(
                F.min(_spark_col(column)).alias("min"),
                F.max(_spark_col(column)).alias("max"),
                F.avg(_spark_col(column)).alias("avg"),
            )
            .collect()[0]
            .asDict()
        )
        return {
            "min": _json_safe_value(row.get("min")),
            "max": _json_safe_value(row.get("max")),
            "avg": _json_safe_value(row.get("avg")),
        }

    def _top_values(self, df: DataFrame, column: str, *, top_n: int) -> list[dict[str, Any]]:
        rows = (
            df.where(_spark_col(column).isNotNull() & (F.trim(_spark_col(column).cast("string")) != ""))
            .groupBy(_spark_col(column).cast("string").alias("value"))
            .count()
            .orderBy(F.desc("count"), F.asc("value"))
            .limit(top_n)
            .collect()
        )
        return [{"value": r["value"], "count": int(r["count"])} for r in rows]

    def _write_parquet(self, df: DataFrame, processed_path: Path) -> None:
        try:
            df.write.mode("overwrite").parquet(str(processed_path))
        except Exception as exc:
            message = str(exc)
            if "winutils" not in message and "HADOOP_HOME" not in message:
                raise
            fallback_file = processed_path / "data.parquet"
            df.toPandas().to_parquet(fallback_file, index=False)


def profile_bigdata_file(
    raw_file_path: str | Path,
    *,
    file_format: str | None = None,
    processed_output_dir: str | Path | None = None,
    top_n: int = 10,
    profiler: SparkBigDataProfiler | None = None,
) -> dict[str, Any]:
    svc = profiler or SparkBigDataProfiler()
    return svc.profile_file(
        raw_file_path,
        file_format=file_format,
        processed_output_dir=processed_output_dir,
        top_n=top_n,
    )


def profile_bigdata_csv(
    raw_csv_path: str | Path,
    *,
    processed_output_dir: str | Path | None = None,
    top_n: int = 10,
    profiler: SparkBigDataProfiler | None = None,
) -> dict[str, Any]:
    return profile_bigdata_file(
        raw_csv_path,
        file_format="csv",
        processed_output_dir=processed_output_dir,
        top_n=top_n,
        profiler=profiler,
    )


def create_bigdata_chat_sample_csv(
    raw_file_path: str | Path,
    *,
    file_format: str | None = None,
    columns: list[str],
    row_limit: int,
    sampling_mode: str = "first",
    filters: list[dict[str, Any]] | None = None,
    profiler: SparkBigDataProfiler | None = None,
) -> dict[str, Any]:
    svc = profiler or SparkBigDataProfiler()
    return svc.create_chat_sample_csv(
        raw_file_path,
        file_format=file_format,
        columns=columns,
        row_limit=row_limit,
        sampling_mode=sampling_mode,
        filters=filters,
    )


def export_bigdata_to_parquet(
    raw_file_path: str | Path,
    *,
    file_format: str | None = None,
    processed_output_dir: str | Path,
    dataset_id: int | None = None,
    profiler: SparkBigDataProfiler | None = None,
) -> dict[str, Any]:
    svc = profiler or SparkBigDataProfiler(app_name="ai-analytics-bigdata-parquet-export")
    return svc.export_to_parquet(
        raw_file_path,
        file_format=file_format,
        processed_output_dir=processed_output_dir,
        dataset_id=dataset_id,
    )


def _detect_file_format(raw_path: Path) -> str:
    name = raw_path.name.lower()
    for suffix, file_format in {
        ".csv.gz": "csv.gz",
        ".json.gz": "json.gz",
        ".parquet": "parquet",
        ".jsonl": "jsonl",
        ".json": "json",
        ".csv": "csv",
    }.items():
        if name.endswith(suffix):
            return file_format
    raise BigDataProfilingError(
        f"Unsupported Big Data file format: {raw_path.name}",
        stage="detect_format",
        details={"raw_file_path": str(raw_path)},
    )


def _reader_name_for_format(file_format: str) -> str:
    if file_format in {"csv", "csv.gz"}:
        return "spark.read.csv"
    if file_format in {"json", "jsonl", "json.gz"}:
        return "spark.read.json"
    if file_format == "parquet":
        return "spark.read.parquet"
    return "unsupported"


def _normalize_format(file_format: str) -> str:
    normalized = (file_format or "").strip().lower()
    if normalized not in SUPPORTED_FORMATS:
        raise BigDataProfilingError(
            f"Unsupported Big Data file format: {file_format}",
            stage="detect_format",
            details={"format": file_format},
        )
    return normalized


def _is_numeric_type(dtype: Any) -> bool:
    return isinstance(
        dtype,
        (
            ByteType,
            ShortType,
            IntegerType,
            LongType,
            FloatType,
            DoubleType,
            DecimalType,
        ),
    )


def _is_categorical_type(dtype: Any) -> bool:
    return isinstance(dtype, (StringType, DateType, TimestampType))


def _spark_col(name: str):
    escaped = (name or "").replace("`", "``")
    return F.col(f"`{escaped}`")


def _rows_to_csv_text(columns: list[str], rows: list[Any]) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(columns)
    for row in rows:
        values = []
        for name in columns:
            value = row[name]
            if isinstance(value, (dict, list, tuple)):
                values.append(str(value))
            elif value is None:
                values.append("")
            elif hasattr(value, "isoformat"):
                values.append(value.isoformat())
            else:
                values.append(value)
        writer.writerow(values)
    return output.getvalue()


def _json_safe_value(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    try:
        import decimal

        if isinstance(value, decimal.Decimal):
            return float(value)
    except Exception:
        pass
    return value


def _ensure_local_spark_env() -> None:
    if not os.environ.get("SPARK_LOCAL_IP"):
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    if not os.environ.get("PYSPARK_PYTHON"):
        os.environ["PYSPARK_PYTHON"] = _windows_short_path(Path(os.sys.executable))
    if not os.environ.get("SPARK_HOME"):
        import pyspark

        os.environ["SPARK_HOME"] = _windows_short_path(Path(pyspark.__file__).resolve().parent)


def _windows_short_path(path: Path) -> str:
    resolved = str(path)
    if platform.system().lower() != "windows":
        return resolved
    try:
        import ctypes

        buffer = ctypes.create_unicode_buffer(260)
        result = ctypes.windll.kernel32.GetShortPathNameW(resolved, buffer, len(buffer))
        if result:
            return buffer.value
    except Exception:
        pass
    return resolved
