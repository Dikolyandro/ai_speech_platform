import tempfile
from app.celery_app import celery
from app.services.openai_asr_service import transcribe
from app.services.storage_minio import download_file
from app.db.session import AsyncSessionLocal
from app.db.models import Job, Transcript
from sqlalchemy import select, update

@celery.task(name="transcribe_openai")
def transcribe_openai(job_id: int):
    import asyncio

    async def run():
        async with AsyncSessionLocal() as session:
            job = (await session.execute(
                select(Job).where(Job.id == job_id)
            )).scalar_one()

            await session.execute(
                update(Job).where(Job.id == job_id).values(status="running")
            )
            await session.commit()

            with tempfile.NamedTemporaryFile(suffix=".wav") as tmp:
                bucket, obj = job.input_uri.split("/", 1)
                download_file(bucket, obj, tmp.name)

                text = transcribe(tmp.name)

                session.add(Transcript(job_id=job_id, text=text))
                await session.execute(
                    update(Job).where(Job.id == job_id).values(status="done")
                )
                await session.commit()

    asyncio.run(run())
