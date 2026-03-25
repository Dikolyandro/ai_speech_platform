from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.db.session import get_db
from app.db.models import Dataset, Document, Chunk, ChunkEmbedding
from app.services.embedding_service import Embedder

router = APIRouter(prefix="/datasets", tags=["datasets"])

class CreateDatasetReq(BaseModel):
    name: str
    workspace_id: str = "default"


class AddDocReq(BaseModel):
    title: str = ""
    text: str
    chunk_size: int = 500


@router.post("")
async def create_dataset(req: CreateDatasetReq, db: AsyncSession = Depends(get_db)):
    ds = Dataset(name=req.name, workspace_id=req.workspace_id)
    db.add(ds)
    await db.commit()
    await db.refresh(ds)
    return {"dataset_id": ds.id}


@router.post("/{dataset_id}/documents")
async def add_document(dataset_id: int, req: AddDocReq, db: AsyncSession = Depends(get_db)):
    # 1) check dataset exists
    ds = (await db.execute(select(Dataset).where(Dataset.id == dataset_id))).scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="dataset not found")

    # 2) save document
    doc = Document(title=req.title, text=req.text)
    db.add(doc)
    await db.commit()
    await db.refresh(doc)

    # 3) chunking
    chunks = []
    idx = 0
    step = max(50, req.chunk_size)  # защита от слишком маленького
    text = req.text or ""

    for i in range(0, len(text), step):
        part = text[i:i + step].strip()
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

    # 4) reload chunks (чтобы были id)
    rows = (await db.execute(
        select(Chunk)
        .where(Chunk.dataset_id == dataset_id, Chunk.document_id == doc.id)
        .order_by(Chunk.chunk_index)
    )).scalars().all()

    # 5) embeddings
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
