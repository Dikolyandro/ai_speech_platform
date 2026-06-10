from __future__ import annotations

import asyncio
import csv
import gzip
import io
import json
import os
import re
import shutil
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.security import get_current_user
from app.db.models import BigDataDataset, Dataset, DatasetTableMeta, User
from app.db.session import get_db
from app.schemas.bigdata import BigDataChatSampleCreate, BigDataDatasetList, BigDataDatasetOut
from app.services.csv_import_service import import_csv_into_dataset
from app.services.spark_bigdata_service import BigDataProfilingError, create_bigdata_chat_sample_csv
from app.services.security_service import (
    BIGDATA_EXTENSIONS,
    BIGDATA_MAX_UPLOAD_BYTES,
    audit_log,
    ensure_path_within,
    validate_basic_file_signature,
)


router = APIRouter(prefix="/bigdata", tags=["Big Data"])


def _lt(lang: str | None, ru: str, en: str, kk: str) -> str:
    if lang == "en":
        return en
    if lang == "kk":
        return kk
    return ru


class RenameBigDataDatasetReq(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)

BACKEND_DIR = Path(__file__).resolve().parents[3]
BIGDATA_DIR = BACKEND_DIR / "data" / "bigdata"
RAW_DIR = BIGDATA_DIR / "raw"
PROFILE_SCRIPT = BACKEND_DIR / "scripts" / "profile_bigdata_csv.py"
PROFILE_TIMEOUT_SECONDS = int(os.getenv("BIGDATA_PROFILE_TIMEOUT_SECONDS", "1800"))
CHAT_SAMPLE_MAX_ROWS = int(os.getenv("BIGDATA_CHAT_SAMPLE_MAX_ROWS", "5000"))
CHAT_SAMPLE_HARD_MAX_ROWS = int(os.getenv("BIGDATA_CHAT_SAMPLE_HARD_MAX_ROWS", "50000"))
CHAT_SAMPLE_MAX_FILTERS = int(os.getenv("BIGDATA_CHAT_SAMPLE_MAX_FILTERS", "20"))
SUPPORTED_FORMATS = {
    ".csv": "csv",
    ".csv.gz": "csv.gz",
    ".json": "json",
    ".jsonl": "jsonl",
    ".json.gz": "json.gz",
    ".parquet": "parquet",
}


def _allowed_local_roots() -> list[Path]:
    configured = os.getenv("BIGDATA_ALLOWED_LOCAL_ROOTS", "")
    roots = [BIGDATA_DIR]
    for item in configured.split(os.pathsep):
        value = item.strip()
        if value:
            roots.append(Path(value))
    return roots


def _safe_dataset_name(raw: str | None, fallback: str) -> str:
    value = (raw or fallback or "bigdata_dataset").strip()
    value = re.sub(r"\s+", " ", value)
    return value[:255] or "bigdata_dataset"


def _safe_original_filename(raw: str | None) -> str:
    filename = Path(raw or "bigdata_dataset").name.strip()
    filename = re.sub(r"[^A-Za-z0-9._ -]+", "_", filename)
    filename = re.sub(r"\s+", "_", filename)
    return filename[:180] or "bigdata_dataset"


def _detect_upload_format(filename: str) -> str:
    lower = filename.lower()
    for suffix, format_name in sorted(SUPPORTED_FORMATS.items(), key=lambda item: len(item[0]), reverse=True):
        if lower.endswith(suffix):
            return format_name
    supported = ", ".join(SUPPORTED_FORMATS.values())
    raise HTTPException(status_code=400, detail=f"unsupported Big Data file format. Supported formats: {supported}")


def _format_suffix(format_name: str) -> str:
    for suffix, supported_format in SUPPORTED_FORMATS.items():
        if supported_format == format_name:
            return suffix
    raise HTTPException(status_code=400, detail=f"unsupported Big Data file format: {format_name}")


def _format_display_stem(filename: str, format_name: str) -> str:
    suffix = _format_suffix(format_name)
    if filename.lower().endswith(suffix):
        return filename[: -len(suffix)]
    return Path(filename).stem


def _ensure_supported_upload(file: UploadFile) -> str:
    filename = Path(file.filename or "").name
    return _detect_upload_format(filename)


def _validate_local_source_path(source_path: str) -> tuple[Path, str]:
    try:
        path = Path(source_path).expanduser().resolve()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"invalid local file path: {exc}") from None
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=400, detail=f"local file not found: {source_path}")
    path = ensure_path_within(path, _allowed_local_roots())
    format_name = _detect_upload_format(path.name)
    return path, format_name


def _user_raw_dir(user_id: int) -> Path:
    path = RAW_DIR / str(user_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


async def _save_upload(file: UploadFile, *, user_id: int) -> tuple[Path, str]:
    format_name = _ensure_supported_upload(file)
    safe_filename = _safe_original_filename(file.filename)
    target = _user_raw_dir(user_id) / f"{uuid.uuid4().hex}_{safe_filename}"
    if not target.name.lower().endswith(_format_suffix(format_name)):
        target = target.with_name(f"{target.name}{_format_suffix(format_name)}")
    total = 0
    first_chunk = b""
    with open(target, "wb") as f:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            if not first_chunk:
                first_chunk = chunk
            total += len(chunk)
            if total > BIGDATA_MAX_UPLOAD_BYTES:
                target.unlink(missing_ok=True)
                raise HTTPException(status_code=413, detail=f"upload exceeds {BIGDATA_MAX_UPLOAD_BYTES} bytes")
            f.write(chunk)
    if target.stat().st_size == 0:
        target.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail="uploaded Big Data file is empty")
    # Validate high-signal signatures without loading the full Big Data file into memory.
    if format_name in {"json", "jsonl", "json.gz", "parquet"}:
        if format_name == "parquet":
            with open(target, "rb") as f:
                head = f.read(4)
                f.seek(max(0, target.stat().st_size - 4))
                tail = f.read(4)
            validate_basic_file_signature(head + tail, target.name, BIGDATA_EXTENSIONS)
        elif format_name in {"json", "jsonl"}:
            validate_basic_file_signature(first_chunk, target.name, BIGDATA_EXTENSIONS)
    return target.resolve(), format_name


async def _get_owned_bigdata_dataset(
    db: AsyncSession,
    dataset_id: int,
    user_id: int,
) -> BigDataDataset:
    ds = (
        await db.execute(
            select(BigDataDataset).where(
                BigDataDataset.id == dataset_id,
                BigDataDataset.user_id == user_id,
            )
        )
    ).scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="big data dataset not found")
    return ds


def _apply_profile_result(ds: BigDataDataset, result: dict[str, Any]) -> None:
    ds.row_count = result.get("row_count")
    ds.column_count = result.get("column_count")
    ds.schema_json = result.get("schema_json")
    ds.profile_json = result.get("profile_json")
    processed_path = result.get("processed_path")
    if processed_path:
        ds.processed_path = str(processed_path)
    ds.status = "processed"
    ds.error = None


def _schema_column_names(ds: BigDataDataset) -> list[str]:
    raw_columns = (ds.schema_json or {}).get("columns") if isinstance(ds.schema_json, dict) else None
    if not isinstance(raw_columns, list):
        return []
    names: list[str] = []
    for item in raw_columns:
        if isinstance(item, dict) and isinstance(item.get("name"), str):
            names.append(item["name"])
    return names


def _validate_chat_sample_request(ds: BigDataDataset, body: BigDataChatSampleCreate) -> tuple[list[str], list[dict[str, Any]]]:
    if ds.status != "processed":
        raise HTTPException(status_code=400, detail="Big Data dataset must be processed before creating a chat sample")

    available_columns = _schema_column_names(ds)
    if not available_columns:
        raise HTTPException(status_code=400, detail="Big Data schema is missing. Run Spark processing first.")
    available = set(available_columns)

    selected_columns = body.columns or available_columns
    missing_columns = [name for name in selected_columns if name not in available]
    if missing_columns:
        raise HTTPException(status_code=400, detail=f"unknown sample columns: {', '.join(missing_columns[:10])}")
    if len(selected_columns) == 0:
        raise HTTPException(status_code=400, detail="select at least one column")

    if body.row_limit > CHAT_SAMPLE_HARD_MAX_ROWS:
        raise HTTPException(status_code=400, detail=f"row_limit must be <= {CHAT_SAMPLE_HARD_MAX_ROWS}")
    if len(body.filters) > CHAT_SAMPLE_MAX_FILTERS:
        raise HTTPException(status_code=400, detail=f"at most {CHAT_SAMPLE_MAX_FILTERS} filters are allowed")

    filters: list[dict[str, Any]] = []
    for item in body.filters:
        if item.column not in available:
            raise HTTPException(status_code=400, detail=f"unknown filter column: {item.column}")
        filters.append({"column": item.column, "operator": item.operator, "value": item.value})
    return selected_columns, filters


def _safe_delete_path(raw_path: str | None) -> None:
    if not raw_path:
        return
    try:
        base = BIGDATA_DIR.resolve()
        path = Path(raw_path).expanduser().resolve()
        if not path.is_relative_to(base):
            return
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        elif path.exists():
            path.unlink(missing_ok=True)
    except Exception:
        return


def _parse_json_output(text: str) -> dict[str, Any]:
    payload = (text or "").strip()
    if not payload:
        return {}
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        start = payload.find("{")
        if start < 0:
            return {}
        decoder = json.JSONDecoder()
        try:
            obj, _ = decoder.raw_decode(payload[start:])
            return obj if isinstance(obj, dict) else {}
        except json.JSONDecodeError:
            return {}


def _run_profile_job(
    raw_path: str,
    *,
    file_format: str,
    top_n: int,
) -> dict[str, Any]:
    if not PROFILE_SCRIPT.exists():
        raise RuntimeError(f"Big Data profiling script not found: {PROFILE_SCRIPT}")

    command = [
        sys.executable,
        str(PROFILE_SCRIPT),
        raw_path,
        "--format",
        file_format,
        "--top-n",
        str(top_n),
    ]

    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")

    completed = subprocess.run(
        command,
        cwd=str(BACKEND_DIR),
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=PROFILE_TIMEOUT_SECONDS,
        check=False,
    )
    if completed.returncode != 0:
        error_payload = _parse_json_output(completed.stderr) or _parse_json_output(completed.stdout)
        if error_payload:
            raise RuntimeError(error_payload.get("error") or json.dumps(error_payload, ensure_ascii=False))
        message = (completed.stderr or completed.stdout or "Spark profiling process failed").strip()
        raise RuntimeError(message[:4000])

    result = _parse_json_output(completed.stdout)
    if not result:
        message = (completed.stderr or completed.stdout or "Spark profiling did not return JSON").strip()
        raise RuntimeError(message[:4000])
    return result


def _open_text_dataset(path: Path, format_name: str):
    if format_name in {"csv.gz", "json.gz"}:
        return gzip.open(path, "rt", encoding="utf-8", errors="replace", newline="")
    return open(path, "r", encoding="utf-8", errors="replace", newline="")


def _sample_csv_text_from_bigdata(path: Path, format_name: str, *, max_rows: int) -> tuple[str, int]:
    max_rows = max(1, min(int(max_rows or CHAT_SAMPLE_MAX_ROWS), CHAT_SAMPLE_MAX_ROWS))
    output = io.StringIO()

    if format_name in {"csv", "csv.gz"}:
        with _open_text_dataset(path, format_name) as f:
            reader = csv.reader(f)
            writer = csv.writer(output)
            header_written = False
            rows_written = 0
            for row in reader:
                if not row:
                    continue
                if not header_written:
                    writer.writerow(row)
                    header_written = True
                    continue
                writer.writerow(row)
                rows_written += 1
                if rows_written >= max_rows:
                    break
        if not header_written or rows_written == 0:
            raise HTTPException(status_code=400, detail="Big Data CSV sample is empty")
        return output.getvalue(), rows_written

    with _open_text_dataset(path, format_name) as f:
        rows: list[dict[str, Any]] = []
        field_order: list[str] = []
        seen_fields: set[str] = set()
        for line_no, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw:
                continue
            try:
                item = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise HTTPException(
                    status_code=400,
                    detail=f"JSON sample must be line-delimited objects. Error on line {line_no}: {exc.msg}",
                ) from None
            if not isinstance(item, dict):
                raise HTTPException(
                    status_code=400,
                    detail=f"JSON sample rows must be objects. Found {type(item).__name__} on line {line_no}",
                )
            flat: dict[str, Any] = {}
            for key, value in item.items():
                name = str(key)
                if isinstance(value, (dict, list)):
                    flat[name] = json.dumps(value, ensure_ascii=False)
                elif value is None:
                    flat[name] = ""
                else:
                    flat[name] = value
                if name not in seen_fields:
                    seen_fields.add(name)
                    field_order.append(name)
            rows.append(flat)
            if len(rows) >= max_rows:
                break

    if not rows or not field_order:
        raise HTTPException(status_code=400, detail="Big Data JSON sample is empty")

    writer = csv.DictWriter(output, fieldnames=field_order, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return output.getvalue(), len(rows)


@router.post("/datasets/upload", response_model=BigDataDatasetOut)
async def upload_bigdata_dataset(
    file: UploadFile = File(...),
    name: str | None = Form(default=None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> BigDataDataset:
    raw_path, format_name = await _save_upload(file, user_id=user.id)
    safe_filename = _safe_original_filename(file.filename)
    display_name = _safe_dataset_name(name, _format_display_stem(safe_filename, format_name))

    ds = BigDataDataset(
        user_id=user.id,
        name=display_name,
        raw_path=str(raw_path),
        format=format_name,
        status="uploaded",
        is_private=True,
    )
    db.add(ds)
    try:
        await db.commit()
        await db.refresh(ds)
    except Exception:
        await db.rollback()
        _safe_delete_path(str(raw_path))
        raise
    audit_log("bigdata.upload", user_id=user.id, dataset_id=ds.id, filename=safe_filename, format=format_name, path=str(raw_path))
    return ds


@router.post("/datasets/register-local", response_model=BigDataDatasetOut)
async def register_local_bigdata_dataset(
    source_path: str = Form(...),
    name: str | None = Form(default=None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> BigDataDataset:
    raw_path, format_name = _validate_local_source_path(source_path)
    safe_filename = _safe_original_filename(raw_path.name)
    display_name = _safe_dataset_name(name, _format_display_stem(safe_filename, format_name))

    ds = BigDataDataset(
        user_id=user.id,
        name=display_name,
        raw_path=str(raw_path),
        format=format_name,
        status="uploaded",
        is_private=True,
    )
    db.add(ds)
    await db.commit()
    await db.refresh(ds)
    audit_log("bigdata.register_local", user_id=user.id, dataset_id=ds.id, filename=safe_filename, format=format_name, path=str(raw_path))
    return ds


@router.get("/datasets", response_model=BigDataDatasetList)
async def list_bigdata_datasets(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, list[BigDataDataset]]:
    rows = (
        await db.execute(
            select(BigDataDataset)
            .where(BigDataDataset.user_id == user.id)
            .order_by(BigDataDataset.id.desc())
        )
    ).scalars().all()
    return {"datasets": list(rows)}


@router.get("/datasets/{dataset_id}", response_model=BigDataDatasetOut)
async def get_bigdata_dataset(
    dataset_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> BigDataDataset:
    return await _get_owned_bigdata_dataset(db, dataset_id, user.id)


@router.patch("/datasets/{dataset_id}", response_model=BigDataDatasetOut)
async def rename_bigdata_dataset(
    dataset_id: int,
    body: RenameBigDataDatasetReq,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> BigDataDataset:
    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="dataset name is required")
    ds = await _get_owned_bigdata_dataset(db, dataset_id, user.id)
    ds.name = name[:255]
    await db.commit()
    await db.refresh(ds)
    audit_log("bigdata.rename", user_id=user.id, dataset_id=dataset_id)
    return ds


@router.post("/datasets/{dataset_id}/profile", response_model=BigDataDatasetOut)
async def profile_bigdata_dataset(
    dataset_id: int,
    top_n: int = 10,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> BigDataDataset:
    ds = await _get_owned_bigdata_dataset(db, dataset_id, user.id)

    ds.status = "processing"
    ds.error = None
    await db.commit()
    await db.refresh(ds)

    try:
        result = await asyncio.to_thread(
            _run_profile_job,
            ds.raw_path,
            file_format=ds.format,
            top_n=top_n,
        )
        _apply_profile_result(ds, result)
        await db.commit()
        await db.refresh(ds)
        audit_log("bigdata.profile", user_id=user.id, dataset_id=dataset_id, row_count=ds.row_count, column_count=ds.column_count)
        return ds
    except Exception as exc:
        ds.status = "failed"
        ds.error = str(exc)
        await db.commit()
        raise HTTPException(status_code=500, detail=f"Big Data profiling failed: {exc}") from None


@router.post("/datasets/{dataset_id}/create-chat-sample")
async def create_chat_sample_from_bigdata_dataset(
    dataset_id: int,
    body: BigDataChatSampleCreate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    ds = await _get_owned_bigdata_dataset(db, dataset_id, user.id)
    raw_path = Path(ds.raw_path).expanduser().resolve()
    if not raw_path.exists() or not raw_path.is_file():
        raise HTTPException(status_code=400, detail="Big Data raw file is not available on this machine")

    selected_columns, filters = _validate_chat_sample_request(ds, body)
    try:
        sample_result = await asyncio.to_thread(
            create_bigdata_chat_sample_csv,
            raw_path,
            file_format=ds.format,
            columns=selected_columns,
            row_limit=body.row_limit,
            sampling_mode=body.sampling_mode,
            filters=filters,
        )
    except BigDataProfilingError as exc:
        raise HTTPException(status_code=500, detail=f"Big Data chat sample failed: {exc}") from None

    csv_text = str(sample_result.get("csv_text") or "")
    rows_sampled = int(sample_result.get("row_count") or 0)
    if rows_sampled <= 0:
        raise HTTPException(status_code=400, detail="No rows matched the sample settings")

    sample_name = _safe_dataset_name(body.name, f"{ds.name} chat sample")
    normal_ds = Dataset(name=sample_name[:255], workspace_id="default", user_id=user.id)
    db.add(normal_ds)
    await db.commit()
    await db.refresh(normal_ds)

    try:
        import_result = await import_csv_into_dataset(
            db,
            normal_ds.id,
            csv_text,
            delimiter=",",
            has_header=True,
            max_rows=rows_sampled,
            drop_existing=True,
        )
    except Exception:
        await db.delete(normal_ds)
        await db.commit()
        raise

    meta_row = (
        await db.execute(select(DatasetTableMeta).where(DatasetTableMeta.dataset_id == normal_ds.id))
    ).scalar_one_or_none()
    if meta_row:
        raw_meta = meta_row.columns_json if isinstance(meta_row.columns_json, dict) else {"columns": meta_row.columns_json or []}
        raw_meta["bigdata_sample"] = {
            "source_bigdata_id": ds.id,
            "source_dataset_name": ds.name,
            "rows_sampled": rows_sampled,
            "selected_columns": selected_columns,
            "applied_filters": filters,
            "sampling_mode": body.sampling_mode,
            "row_limit": body.row_limit,
        }
        meta_row.columns_json = raw_meta
        await db.commit()

    audit_log(
        "bigdata.chat_sample_create",
        user_id=user.id,
        dataset_id=ds.id,
        created_dataset_id=normal_ds.id,
        rows_sampled=rows_sampled,
        columns=len(selected_columns),
        filters=len(filters),
    )

    return {
        "dataset_id": normal_ds.id,
        "name": normal_ds.name,
        "source_bigdata_id": ds.id,
        "source_dataset_name": ds.name,
        "rows_sampled": rows_sampled,
        "row_count": rows_sampled,
        "selected_columns": selected_columns,
        "applied_filters": filters,
        "sampling_mode": body.sampling_mode,
        "row_limit": body.row_limit,
        "table_name": import_result.get("table_name"),
        "note": _lt(
            getattr(user, "preferred_language", "ru"),
            "SQL-выборка создана. Выберите ее в чате, чтобы задавать вопросы на естественном языке.",
            "A SQL sample was created. Select it in chat to ask natural-language questions.",
            "SQL үлгісі жасалды. Табиғи тілде сұрақ қою үшін оны чатта таңдаңыз.",
        ),
    }


@router.delete("/datasets/{dataset_id}")
async def delete_bigdata_dataset(
    dataset_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    ds = await _get_owned_bigdata_dataset(db, dataset_id, user.id)
    raw_path = ds.raw_path
    processed_path = ds.processed_path

    await db.delete(ds)
    await db.commit()

    _safe_delete_path(raw_path)
    _safe_delete_path(processed_path)
    audit_log("bigdata.delete", user_id=user.id, dataset_id=dataset_id)

    return {"ok": True, "dataset_id": dataset_id}
