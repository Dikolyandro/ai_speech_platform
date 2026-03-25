"""Загрузка текста транскрипции по job_id (после POST /api/v1/asr/transcribe)."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException

from app.db.models import Job, Transcript


async def get_transcript_text(db: AsyncSession, job_id: int) -> str:
    job = (await db.execute(select(Job).where(Job.id == job_id))).scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail=f"job {job_id} not found")
    if job.status == "failed":
        err = (job.error or "").strip() or "unknown error"
        raise HTTPException(status_code=400, detail=f"transcription failed: {err}")
    if job.status != "done":
        raise HTTPException(
            status_code=400,
            detail=f"transcription not ready (status={job.status}); retry later",
        )
    tr = (await db.execute(select(Transcript).where(Transcript.job_id == job_id))).scalar_one_or_none()
    if not tr or not (tr.text or "").strip():
        raise HTTPException(status_code=404, detail="transcript not found")
    return (tr.text or "").strip()
