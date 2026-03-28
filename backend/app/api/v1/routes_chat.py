from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import ChatMessage, ChatSession
from app.db.session import get_db
from app.auth.security import get_current_user
from app.db.models import User

router = APIRouter(prefix="/chat", tags=["Chat"])


class CreateSessionBody(BaseModel):
    title: str = "New chat"
    workspace_id: str = "default"


class PatchSessionBody(BaseModel):
    title: str = Field(..., min_length=1, max_length=255)


class CreateMessageBody(BaseModel):
    role: Literal["user", "assistant"]
    content: str = Field(..., min_length=1)
    meta_json: Optional[dict[str, Any]] = None


def _session_out(s: ChatSession) -> dict[str, Any]:
    return {
        "id": s.id,
        "workspace_id": s.workspace_id,
        "title": s.title,
        "created_at": s.created_at.isoformat() if s.created_at else None,
        "updated_at": s.updated_at.isoformat() if s.updated_at else None,
    }


def _message_out(m: ChatMessage) -> dict[str, Any]:
    return {
        "id": m.id,
        "session_id": m.session_id,
        "role": m.role,
        "content": m.content,
        "meta_json": m.meta_json,
        "created_at": m.created_at.isoformat() if m.created_at else None,
    }


@router.post("/sessions")
async def create_session(
    body: CreateSessionBody,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    s = ChatSession(user_id=user.id, workspace_id=body.workspace_id, title=body.title.strip() or "New chat")
    db.add(s)
    await db.commit()
    await db.refresh(s)
    return _session_out(s)


@router.get("/sessions")
async def list_sessions(
    workspace_id: str = "default",
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    r = await db.execute(
        select(ChatSession)
        .where(ChatSession.user_id == user.id, ChatSession.workspace_id == workspace_id)
        .order_by(ChatSession.updated_at.desc())
    )
    rows = r.scalars().all()
    return {"sessions": [_session_out(s) for s in rows]}


@router.patch("/sessions/{session_id}")
async def rename_session(
    session_id: int,
    body: PatchSessionBody,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    s = (
        await db.execute(select(ChatSession).where(ChatSession.id == session_id, ChatSession.user_id == user.id))
    ).scalar_one_or_none()
    if not s:
        raise HTTPException(status_code=404, detail="session not found")
    s.title = body.title.strip()
    s.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(s)
    return _session_out(s)


@router.delete("/sessions/{session_id}")
async def delete_session(
    session_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, str]:
    s = (
        await db.execute(select(ChatSession).where(ChatSession.id == session_id, ChatSession.user_id == user.id))
    ).scalar_one_or_none()
    if not s:
        raise HTTPException(status_code=404, detail="session not found")
    await db.execute(delete(ChatSession).where(ChatSession.id == session_id))
    await db.commit()
    return {"ok": "true"}


@router.get("/sessions/{session_id}/messages")
async def list_messages(
    session_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    s = (
        await db.execute(select(ChatSession).where(ChatSession.id == session_id, ChatSession.user_id == user.id))
    ).scalar_one_or_none()
    if not s:
        raise HTTPException(status_code=404, detail="session not found")
    r = await db.execute(
        select(ChatMessage)
        .where(ChatMessage.session_id == session_id)
        .order_by(ChatMessage.id.asc())
    )
    rows = r.scalars().all()
    return {"messages": [_message_out(m) for m in rows]}


@router.post("/sessions/{session_id}/messages")
async def append_message(
    session_id: int,
    body: CreateMessageBody,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    s = (
        await db.execute(select(ChatSession).where(ChatSession.id == session_id, ChatSession.user_id == user.id))
    ).scalar_one_or_none()
    if not s:
        raise HTTPException(status_code=404, detail="session not found")
    m = ChatMessage(
        session_id=session_id,
        role=body.role,
        content=body.content,
        meta_json=body.meta_json,
    )
    db.add(m)
    s.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(m)
    return _message_out(m)
