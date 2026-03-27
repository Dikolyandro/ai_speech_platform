from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.csv_import_service import import_csv_into_dataset
from app.auth.security import get_current_user
from app.db.models import Dataset, User
from sqlalchemy import select

router = APIRouter(prefix="/datasets", tags=["Dataset Import"])


class ImportCSVRequest(BaseModel):
    csv_text: str
    delimiter: str = ","
    has_header: bool = True
    max_rows: int = 5000
    drop_existing: bool = True


@router.post("/{dataset_id}/import_csv")
async def import_csv(
    dataset_id: int,
    req: ImportCSVRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    ds = (
        await db.execute(select(Dataset).where(Dataset.id == dataset_id, Dataset.user_id == user.id))
    ).scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="dataset not found")
    return await import_csv_into_dataset(
        db,
        dataset_id,
        req.csv_text,
        delimiter=req.delimiter,
        has_header=req.has_header,
        max_rows=req.max_rows,
        drop_existing=req.drop_existing,
    )


@router.post("/{dataset_id}/import_csv_file")
async def import_csv_file(
    dataset_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    file: UploadFile = File(...),
    delimiter: str = Form(","),
    has_header: bool = Form(True),
    max_rows: int = Form(5000),
    drop_existing: bool = Form(True),
) -> dict[str, Any]:
    ds = (
        await db.execute(select(Dataset).where(Dataset.id == dataset_id, Dataset.user_id == user.id))
    ).scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="dataset not found")
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="empty file")
    try:
        csv_text = raw.decode("utf-8")
    except UnicodeDecodeError:
        csv_text = raw.decode("utf-8", errors="replace")
    return await import_csv_into_dataset(
        db,
        dataset_id,
        csv_text,
        delimiter=delimiter,
        has_header=has_header,
        max_rows=max_rows,
        drop_existing=drop_existing,
    )
