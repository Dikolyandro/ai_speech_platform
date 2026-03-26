from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Chunk, ChunkEmbedding, Dataset, Document
from app.services.embedding_service import Embedder


async def index_document_text(
    db: AsyncSession,
    dataset_id: int,
    title: str,
    text: str,
    chunk_size: int = 500,
) -> dict:
    ds = (await db.execute(select(Dataset).where(Dataset.id == dataset_id))).scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="dataset not found")

    doc = Document(title=title or "", text=text or "")
    db.add(doc)
    await db.commit()
    await db.refresh(doc)

    chunks = []
    idx = 0
    step = max(50, chunk_size)
    body = text or ""

    for i in range(0, len(body), step):
        part = body[i : i + step].strip()
        if part:
            chunks.append(
                Chunk(
                    dataset_id=dataset_id,
                    document_id=doc.id,
                    chunk_index=idx,
                    text=part,
                    meta_json=None,
                )
            )
            idx += 1

    if not chunks:
        raise HTTPException(status_code=400, detail="empty text after chunking")

    db.add_all(chunks)
    await db.commit()

    rows = (
        await db.execute(
            select(Chunk)
            .where(Chunk.dataset_id == dataset_id, Chunk.document_id == doc.id)
            .order_by(Chunk.chunk_index)
        )
    ).scalars().all()

    embedder = Embedder()
    arr = embedder.encode([c.text for c in rows])
    dim = int(arr.shape[1])

    embeds = [
        ChunkEmbedding(
            chunk_id=ch.id,
            model_version="bge-m3",
            dim=dim,
            vector=vec.astype("float32").tobytes(),
        )
        for ch, vec in zip(rows, arr)
    ]
    db.add_all(embeds)
    await db.commit()

    return {"document_id": doc.id, "chunks_indexed": len(rows)}
