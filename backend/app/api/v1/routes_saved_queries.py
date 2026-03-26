from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import SavedQuery
from app.db.session import get_db

router = APIRouter(prefix="/saved-queries", tags=["Saved queries"])


class CreateSavedQueryBody(BaseModel):
    title: str = Field(default="Saved query", max_length=255)
    query_text: str = Field(..., min_length=1)
    sql_text: Optional[str] = None
    answer_text: Optional[str] = None
    result_json: Optional[dict[str, Any]] = None
    dataset_id: Optional[int] = None
    workspace_id: str = "default"


class PatchSavedQueryBody(BaseModel):
    title: str = Field(..., min_length=1, max_length=255)


def _out(q: SavedQuery) -> dict[str, Any]:
    return {
        "id": q.id,
        "workspace_id": q.workspace_id,
        "title": q.title,
        "query_text": q.query_text,
        "sql_text": q.sql_text,
        "answer_text": q.answer_text,
        "result_json": q.result_json,
        "dataset_id": q.dataset_id,
        "created_at": q.created_at.isoformat() if q.created_at else None,
    }


@router.post("")
async def create_saved_query(body: CreateSavedQueryBody, db: AsyncSession = Depends(get_db)) -> dict[str, Any]:
    q = SavedQuery(
        workspace_id=body.workspace_id,
        title=(body.title or "Saved query").strip(),
        query_text=body.query_text,
        sql_text=body.sql_text,
        answer_text=body.answer_text,
        result_json=body.result_json,
        dataset_id=body.dataset_id,
    )
    db.add(q)
    await db.commit()
    await db.refresh(q)
    return _out(q)


@router.get("")
async def list_saved_queries(
    workspace_id: str = "default",
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    r = await db.execute(
        select(SavedQuery)
        .where(SavedQuery.workspace_id == workspace_id)
        .order_by(SavedQuery.id.desc())
    )
    rows = r.scalars().all()
    return {"queries": [_out(q) for q in rows]}


@router.patch("/{query_id}")
async def patch_saved_query(
    query_id: int,
    body: PatchSavedQueryBody,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    q = (await db.execute(select(SavedQuery).where(SavedQuery.id == query_id))).scalar_one_or_none()
    if not q:
        raise HTTPException(status_code=404, detail="saved query not found")
    q.title = body.title.strip()
    await db.commit()
    await db.refresh(q)
    return _out(q)


@router.delete("/{query_id}")
async def delete_saved_query(query_id: int, db: AsyncSession = Depends(get_db)) -> dict[str, str]:
    q = (await db.execute(select(SavedQuery).where(SavedQuery.id == query_id))).scalar_one_or_none()
    if not q:
        raise HTTPException(status_code=404, detail="saved query not found")
    await db.execute(delete(SavedQuery).where(SavedQuery.id == query_id))
    await db.commit()
    return {"ok": "true"}
