import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, UploadFile, File
from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.db.models import Job, Transcript, User
from app.services.storage_local import save_bytes
from app.services.openai_asr_service import transcribe
from app.auth.security import get_current_user
from app.services.i18n_service import normalize_preferred_language

router = APIRouter(prefix="/asr", tags=["ASR"])

@router.post("/transcribe")
async def transcribe_audio(
    file: UploadFile = File(...),
    user: User = Depends(get_current_user),
):
    # 1) создаём job
    async with AsyncSessionLocal() as session:
        job = Job(user_id=user.id, type="transcribe", status="running")
        session.add(job)
        await session.commit()
        await session.refresh(job)

    # 2) сохраняем аудио локально (сохраняем расширение: mp3, m4a, webm, wav …)
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in (".wav", ".mp3", ".m4a", ".webm", ".mp4", ".mpeg", ".mpga", ".oga", ".ogg", ".flac"):
        suffix = ".wav"
    filename = f"{job.id}_{uuid.uuid4()}{suffix}"
    audio_path = save_bytes(await file.read(), filename)

    # 3) вызываем OpenAI транскрибацию
    try:
        text = transcribe(audio_path, language_hint=normalize_preferred_language(getattr(user, "preferred_language", "ru")))
        async with AsyncSessionLocal() as session:
            # обновляем job
            j = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
            j.input_uri = audio_path
            j.status = "done"
            session.add(Transcript(job_id=job.id, text=text))
            await session.commit()
    except Exception as e:
        async with AsyncSessionLocal() as session:
            j = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
            j.status = "failed"
            j.error = str(e)
            await session.commit()
        return {"job_id": job.id, "status": "failed", "error": str(e)}

    return {"job_id": job.id, "status": "done", "text": text}

@router.get("/jobs/{job_id}")
async def get_job(job_id: int, user: User = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        job = (
            await session.execute(select(Job).where(Job.id == job_id, Job.user_id == user.id))
        ).scalar_one()

        if job.status == "done":
            tr = (await session.execute(
                select(Transcript).where(Transcript.job_id == job_id)
            )).scalar_one()
            return {"status": job.status, "text": tr.text}

        if job.status == "failed":
            return {"status": job.status, "error": job.error}

        return {"status": job.status}
