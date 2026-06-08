from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Chunk, Dataset, DatasetTableMeta, Document, SavedQuery
from app.db.session import get_db
from app.services.dataset_overview_service import (
    build_dataset_overview,
    get_dataset_summary_from_meta_json,
    unwrap_dataset_columns_json,
)
from app.services.dataset_suggestion_service import build_dataset_suggestions
from app.services.document_index_service import index_document_text
from app.auth.security import get_current_user
from app.db.models import User
from app.services.security_service import (
    DOCUMENT_EXTENSIONS,
    DOCUMENT_MAX_UPLOAD_BYTES,
    audit_log,
    read_upload_limited,
    validate_upload_extension,
)

router = APIRouter(prefix="/datasets", tags=["datasets"])


class CreateDatasetReq(BaseModel):
    name: str
    workspace_id: str = "default"
    is_private: bool = True


class AddDocReq(BaseModel):
    title: str = ""
    text: str
    chunk_size: int = 500


class RenameDatasetReq(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)


def _extract_bigdata_sample_meta(raw: Any) -> dict[str, Any] | None:
    if isinstance(raw, dict) and isinstance(raw.get("bigdata_sample"), dict):
        return raw["bigdata_sample"]
    return None


@router.get("")
async def list_datasets(
    workspace_id: str = "default",
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    r = await db.execute(
        select(Dataset)
        .where(Dataset.user_id == user.id, Dataset.workspace_id == workspace_id)
        .order_by(Dataset.id.desc())
    )
    items = r.scalars().all()
    out = []
    for d in items:
        meta_r = await db.execute(select(DatasetTableMeta).where(DatasetTableMeta.dataset_id == d.id))
        meta = meta_r.scalar_one_or_none()
        table_name = meta.table_name if meta else f"ds_{d.id}_data"
        row_count = None
        try:
            c = await db.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
            row_count = int(c.scalar() or 0)
        except Exception:
            row_count = None
        out.append(
            {
                "id": d.id,
                "name": d.name,
                "workspace_id": d.workspace_id,
                "created_at": d.created_at.isoformat() if d.created_at else None,
                "table_name": table_name,
                "is_private": bool(getattr(d, "is_private", True)),
                "columns": unwrap_dataset_columns_json(meta.columns_json)
                if meta and meta.columns_json
                else None,
                "row_count": row_count,
            }
        )
    return {"datasets": out}


@router.post("")
async def create_dataset(
    req: CreateDatasetReq,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ds = Dataset(name=req.name, workspace_id=req.workspace_id, user_id=user.id, is_private=bool(req.is_private))
    db.add(ds)
    await db.commit()
    await db.refresh(ds)
    audit_log("dataset.create", user_id=user.id, dataset_id=ds.id, workspace_id=ds.workspace_id, is_private=ds.is_private)
    return {"dataset_id": ds.id}


@router.patch("/{dataset_id}")
async def rename_dataset(
    dataset_id: int,
    req: RenameDatasetReq,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    name = req.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="dataset name is required")
    ds = (
        await db.execute(select(Dataset).where(Dataset.id == dataset_id, Dataset.user_id == user.id))
    ).scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="dataset not found")
    ds.name = name[:255]
    await db.commit()
    await db.refresh(ds)
    audit_log("dataset.rename", user_id=user.id, dataset_id=dataset_id)
    return {
        "id": ds.id,
        "name": ds.name,
        "workspace_id": ds.workspace_id,
        "created_at": ds.created_at.isoformat() if ds.created_at else None,
        "is_private": bool(getattr(ds, "is_private", True)),
    }


@router.get("/{dataset_id}/overview")
async def get_dataset_overview(
    dataset_id: int,
    workspace_id: str = "default",
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Return stored ``dataset_summary`` from import metadata, or build one on the fly
    from column profiles + current row count.
    """
    ds = (
        await db.execute(
            select(Dataset).where(
                Dataset.id == dataset_id,
                Dataset.user_id == user.id,
                Dataset.workspace_id == workspace_id,
            )
        )
    ).scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="dataset not found")
    meta_r = await db.execute(select(DatasetTableMeta).where(DatasetTableMeta.dataset_id == dataset_id))
    meta = meta_r.scalar_one_or_none()
    raw = meta.columns_json if meta else None
    table_name = meta.table_name if meta else f"ds_{dataset_id}_data"
    row_count = None
    try:
        c = await db.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
        row_count = int(c.scalar() or 0)
    except Exception:
        row_count = None
    if raw:
        summary = get_dataset_summary_from_meta_json(raw)
        if summary:
            return {"dataset_id": dataset_id, "overview": summary, "source": "stored"}
        cols = unwrap_dataset_columns_json(raw)
        if cols:
            overview = build_dataset_overview(cols, row_count=row_count)
            return {"dataset_id": dataset_id, "overview": overview, "source": "generated"}
    raise HTTPException(status_code=404, detail="no column metadata for this dataset")


@router.get("/{dataset_id}/chat-context")
async def get_dataset_chat_context(
    dataset_id: int,
    workspace_id: str = "default",
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Lightweight dataset context for chat onboarding.

    It reuses importer metadata and does not run profiling or touch the NL-to-SQL
    pipeline. Ownership is checked before returning schema details.
    """
    ds = (
        await db.execute(
            select(Dataset).where(
                Dataset.id == dataset_id,
                Dataset.user_id == user.id,
                Dataset.workspace_id == workspace_id,
            )
        )
    ).scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="dataset not found")

    meta = (
        await db.execute(select(DatasetTableMeta).where(DatasetTableMeta.dataset_id == dataset_id))
    ).scalar_one_or_none()
    raw = meta.columns_json if meta else None
    columns = unwrap_dataset_columns_json(raw)
    table_name = meta.table_name if meta else f"ds_{dataset_id}_data"
    row_count = None
    try:
        c = await db.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
        row_count = int(c.scalar() or 0)
    except Exception:
        row_count = None

    overview = get_dataset_summary_from_meta_json(raw) if raw else None
    if not overview and columns:
        overview = build_dataset_overview(columns, row_count=row_count)

    return {
        "dataset_id": ds.id,
        "name": ds.name,
        "workspace_id": ds.workspace_id,
        "is_private": bool(getattr(ds, "is_private", True)),
        "table_name": table_name,
        "row_count": row_count,
        "columns": columns,
        "overview": overview,
        "bigdata_sample": _extract_bigdata_sample_meta(raw),
    }


@router.get("/{dataset_id}/suggestions")
async def get_dataset_suggestions(
    dataset_id: int,
    workspace_id: str = "default",
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    ds = (
        await db.execute(
            select(Dataset).where(
                Dataset.id == dataset_id,
                Dataset.user_id == user.id,
                Dataset.workspace_id == workspace_id,
            )
        )
    ).scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="dataset not found")
    meta = (
        await db.execute(select(DatasetTableMeta).where(DatasetTableMeta.dataset_id == dataset_id))
    ).scalar_one_or_none()
    raw = meta.columns_json if meta else None
    columns = unwrap_dataset_columns_json(raw)
    summary = get_dataset_summary_from_meta_json(raw) if raw else None
    return {
        "dataset_id": dataset_id,
        "suggestions": build_dataset_suggestions(
            dataset_id=dataset_id,
            columns=columns,
            dataset_summary=summary,
        ),
    }


@router.delete("/{dataset_id}")
async def delete_dataset(
    dataset_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    ds = (
        await db.execute(select(Dataset).where(Dataset.id == dataset_id, Dataset.user_id == user.id))
    ).scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="dataset not found")
    doc_ids = (
        await db.execute(select(Chunk.document_id).where(Chunk.dataset_id == dataset_id, Chunk.document_id.is_not(None)))
    ).scalars().all()
    # Drop only the owned dataset table name generated by our importer.
    await db.execute(text(f"DROP TABLE IF EXISTS `ds_{dataset_id}_data`"))
    await db.execute(delete(DatasetTableMeta).where(DatasetTableMeta.dataset_id == dataset_id))
    await db.execute(delete(Chunk).where(Chunk.dataset_id == dataset_id))
    if doc_ids:
        await db.execute(delete(Document).where(Document.id.in_(list(set(doc_ids)))))
    await db.execute(delete(SavedQuery).where(SavedQuery.dataset_id == dataset_id, SavedQuery.user_id == user.id))
    await db.execute(delete(Dataset).where(Dataset.id == dataset_id, Dataset.user_id == user.id))
    await db.commit()
    audit_log("dataset.delete", user_id=user.id, dataset_id=dataset_id)
    return {"ok": True, "dataset_id": dataset_id}


@router.post("/{dataset_id}/documents")
async def add_document(
    dataset_id: int,
    req: AddDocReq,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ds = (
        await db.execute(select(Dataset).where(Dataset.id == dataset_id, Dataset.user_id == user.id))
    ).scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="dataset not found")
    return await index_document_text(
        db,
        dataset_id,
        title=req.title,
        text=req.text,
        chunk_size=req.chunk_size,
    )


@router.post("/{dataset_id}/documents/upload")
async def upload_document_file(
    dataset_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    file: UploadFile = File(...),
    chunk_size: int = Form(500),
) -> dict:
    ds = (
        await db.execute(select(Dataset).where(Dataset.id == dataset_id, Dataset.user_id == user.id))
    ).scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="dataset not found")
    if not (100 <= int(chunk_size) <= 5000):
        raise HTTPException(status_code=400, detail="chunk_size must be between 100 and 5000")
    validate_upload_extension(file.filename, DOCUMENT_EXTENSIONS)
    raw = await read_upload_limited(file, max_bytes=DOCUMENT_MAX_UPLOAD_BYTES)
    if not raw:
        raise HTTPException(status_code=400, detail="empty file")
    try:
        body = raw.decode("utf-8")
    except UnicodeDecodeError:
        body = raw.decode("utf-8", errors="replace")
    title = Path(file.filename or "document").name
    audit_log("dataset.document_upload", user_id=user.id, dataset_id=dataset_id, filename=title, bytes=len(raw))
    return await index_document_text(db, dataset_id, title=title, text=body, chunk_size=chunk_size)
