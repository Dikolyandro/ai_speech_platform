from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

from fastapi import HTTPException, UploadFile

logger = logging.getLogger("app.security")


NORMAL_DATASET_MAX_UPLOAD_BYTES = int(62.5 * 1024 * 1024)
DOCUMENT_MAX_UPLOAD_BYTES = 25 * 1024 * 1024
BIGDATA_MAX_UPLOAD_BYTES = 5 * 1024 * 1024 * 1024

NORMAL_DATASET_EXTENSIONS = {".csv", ".json", ".xlsx"}
DOCUMENT_EXTENSIONS = {".txt", ".md", ".csv", ".json"}
BIGDATA_EXTENSIONS = {".csv", ".csv.gz", ".json", ".jsonl", ".json.gz", ".parquet"}


def audit_log(action: str, *, user_id: int | None = None, dataset_id: int | None = None, **extra: object) -> None:
    """Security audit trail for sensitive dataset actions; never log raw row data or secrets."""
    logger.info("action=%s user_id=%s dataset_id=%s extra=%s", action, user_id, dataset_id, extra)


def safe_upload_filename(filename: str | None) -> str:
    return Path(filename or "upload").name


def validate_upload_extension(filename: str | None, allowed: Iterable[str]) -> str:
    safe_name = safe_upload_filename(filename)
    lower = safe_name.lower()
    for suffix in sorted(set(allowed), key=len, reverse=True):
        if lower.endswith(suffix):
            return suffix
    raise HTTPException(status_code=400, detail=f"unsupported file type: {safe_name}")


def validate_basic_file_signature(raw: bytes, filename: str | None, allowed: Iterable[str]) -> str:
    suffix = validate_upload_extension(filename, allowed)
    sample = raw[:4096].lstrip()
    if suffix == ".json" and not sample.startswith((b"{", b"[")):
        raise HTTPException(status_code=400, detail="invalid JSON file signature")
    if suffix == ".xlsx" and not raw.startswith(b"PK"):
        raise HTTPException(status_code=400, detail="invalid XLSX file signature")
    if suffix == ".parquet" and not (raw.startswith(b"PAR1") or raw[-4:] == b"PAR1"):
        raise HTTPException(status_code=400, detail="invalid Parquet file signature")
    return suffix


async def read_upload_limited(file: UploadFile, *, max_bytes: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = await file.read(1024 * 1024)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise HTTPException(status_code=413, detail=f"upload exceeds {max_bytes} bytes")
        chunks.append(chunk)
    return b"".join(chunks)


def ensure_path_within(path: Path, allowed_roots: Iterable[Path]) -> Path:
    resolved = path.expanduser().resolve()
    for root in allowed_roots:
        try:
            resolved.relative_to(root.expanduser().resolve())
            return resolved
        except ValueError:
            continue
    raise HTTPException(status_code=400, detail="local file path is outside allowed dataset storage roots")
