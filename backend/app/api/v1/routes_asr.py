import asyncio
import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, File, UploadFile
from sqlalchemy import select

from app.auth.security import get_current_user
from app.core.config import settings
from app.db.models import Job, Transcript, User
from app.db.session import AsyncSessionLocal
from app.services.i18n_service import normalize_preferred_language
from app.services.openai_asr_service import transcribe
from app.services.storage_local import save_bytes

router = APIRouter(prefix="/asr", tags=["ASR"])

SUPPORTED_AUDIO_SUFFIXES = {".wav", ".mp3", ".m4a", ".webm", ".mp4", ".mpeg", ".mpga", ".oga", ".ogg", ".flac"}


async def _create_job(user_id: int) -> Job:
    async with AsyncSessionLocal() as session:
        job = Job(user_id=user_id, type="transcribe", status="running")
        session.add(job)
        await session.commit()
        await session.refresh(job)
        return job


async def _mark_job_failed(job_id: int, error: str, input_uri: str | None = None) -> None:
    async with AsyncSessionLocal() as session:
        job = (await session.execute(select(Job).where(Job.id == job_id))).scalar_one()
        if input_uri:
            job.input_uri = input_uri
        job.status = "failed"
        job.error = error[:2000]
        await session.commit()


@router.post("/transcribe")
async def transcribe_audio(
    file: UploadFile = File(...),
    user: User = Depends(get_current_user),
):
    job = await _create_job(user.id)

    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in SUPPORTED_AUDIO_SUFFIXES:
        suffix = ".wav"

    raw = await file.read()
    if not raw:
        msg = "empty audio file"
        await _mark_job_failed(job.id, msg)
        return {"job_id": job.id, "status": "failed", "error": msg}

    max_bytes = int(getattr(settings, "ASR_MAX_AUDIO_BYTES", 10 * 1024 * 1024))
    if len(raw) > max_bytes:
        msg = f"audio file is too large; max {max_bytes} bytes"
        await _mark_job_failed(job.id, msg)
        return {"job_id": job.id, "status": "failed", "error": msg}

    filename = f"{job.id}_{uuid.uuid4()}{suffix}"
    audio_path = save_bytes(raw, filename)

    try:
        text = await asyncio.wait_for(
            asyncio.to_thread(
                transcribe,
                audio_path,
                language_hint=normalize_preferred_language(getattr(user, "preferred_language", "ru")),
            ),
            timeout=int(getattr(settings, "ASR_TIMEOUT_SECONDS", 60)),
        )
        async with AsyncSessionLocal() as session:
            stored_job = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
            stored_job.input_uri = audio_path
            stored_job.status = "done"
            session.add(Transcript(job_id=job.id, text=text))
            await session.commit()
    except asyncio.TimeoutError:
        msg = "ASR transcription timed out. Try a shorter recording."
        await _mark_job_failed(job.id, msg, input_uri=audio_path)
        return {"job_id": job.id, "status": "failed", "error": msg}
    except Exception as exc:
        msg = str(exc) or exc.__class__.__name__
        await _mark_job_failed(job.id, msg, input_uri=audio_path)
        return {"job_id": job.id, "status": "failed", "error": msg}

    return {"job_id": job.id, "status": "done", "text": text}


@router.get("/jobs/{job_id}")
async def get_job(job_id: int, user: User = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        job = (
            await session.execute(select(Job).where(Job.id == job_id, Job.user_id == user.id))
        ).scalar_one()

        if job.status == "done":
            transcript = (
                await session.execute(select(Transcript).where(Transcript.job_id == job_id))
            ).scalar_one()
            return {"status": job.status, "text": transcript.text}

        if job.status == "failed":
            return {"status": job.status, "error": job.error}

        return {"status": job.status}
