from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel
from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Dataset, DatasetTableMeta
from app.db.session import get_db
from app.services.document_index_service import index_document_text

router = APIRouter(prefix="/datasets", tags=["datasets"])


class CreateDatasetReq(BaseModel):
    name: str
    workspace_id: str = "default"


class AddDocReq(BaseModel):
    title: str = ""
    text: str
    chunk_size: int = 500


@router.get("")
async def list_datasets(
    workspace_id: str = "default",
    db: AsyncSession = Depends(get_db),
) -> dict:
    r = await db.execute(
        select(Dataset).where(Dataset.workspace_id == workspace_id).order_by(Dataset.id.desc())
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
                "columns": meta.columns_json if meta else None,
                "row_count": row_count,
            }
        )
    return {"datasets": out}


@router.post("")
async def create_dataset(req: CreateDatasetReq, db: AsyncSession = Depends(get_db)):
    ds = Dataset(name=req.name, workspace_id=req.workspace_id)
    db.add(ds)
    await db.commit()
    await db.refresh(ds)
    return {"dataset_id": ds.id}


@router.delete("/{dataset_id}")
async def delete_dataset(dataset_id: int, db: AsyncSession = Depends(get_db)) -> dict:
    ds = (await db.execute(select(Dataset).where(Dataset.id == dataset_id))).scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="dataset not found")
    await db.execute(text(f"DROP TABLE IF EXISTS `ds_{dataset_id}_data`"))
    await db.execute(delete(Dataset).where(Dataset.id == dataset_id))
    await db.commit()
    return {"ok": True, "dataset_id": dataset_id}


@router.post("/{dataset_id}/documents")
async def add_document(dataset_id: int, req: AddDocReq, db: AsyncSession = Depends(get_db)):
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
    db: AsyncSession = Depends(get_db),
    file: UploadFile = File(...),
    chunk_size: int = Form(500),
) -> dict:
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="empty file")
    try:
        body = raw.decode("utf-8")
    except UnicodeDecodeError:
        body = raw.decode("utf-8", errors="replace")
    title = Path(file.filename or "document").name
    return await index_document_text(db, dataset_id, title=title, text=body, chunk_size=chunk_size)
