from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.db.session import get_db
from app.db.models import Dataset, Document, Chunk, ChunkEmbedding
from app.services.embedding_service import Embedder

router = APIRouter(prefix="/datasets", tags=["Datasets"])

class CreateDatasetReq(BaseModel):
    name: str
    workspace_id: str = "default"

class AddDocReq(BaseModel):
    title: str = ""
    text: str
    chunk_size: int = 500  # очень грубо, потом улучшим

@router.post("")
async def create_dataset(req: CreateDatasetReq, db: AsyncSession = Depends(get_db)):
    ds = Dataset(name=req.name, workspace_id=req.workspace_id)
    db.add(ds)
    await db.commit()
    await db.refresh(ds)
    return {"dataset_id": ds.id}

@router.post("/{dataset_id}/documents")
async def add_document(dataset_id: int, req: AddDocReq, db: AsyncSession = Depends(get_db)):
    doc = Document(title=req.title, text=req.text)
    db.add(doc)
    await db.commit()
    await db.refresh(doc)

    # naive chunking by characters (MVP)
    text = req.text
    chunks = []
    idx = 0
    step = req.chunk_size
    for i in range(0, len(text), step):
        part = text[i:i+step].strip()
        if part:
            ch = Chunk(dataset_id=dataset_id, document_id=doc.id, chunk_index=idx, text=part, meta_json=None)
            chunks.append(ch)
            idx += 1

    db.add_all(chunks)
    await db.commit()

    # embeddings
    embedder = Embedder()
    arr = embedder.encode([c.text for c in chunks])
    dim = int(arr.shape[1])

    # нужно обновить chunks с id
    # перезагружаем их из БД
    stmt = select(Chunk).where(Chunk.dataset_id == dataset_id, Chunk.document_id == doc.id).order_by(Chunk.chunk_index)
    rows = (await db.execute(stmt)).scalars().all()

    embeds = []
    for ch, vec in zip(rows, arr):
        embeds.append(
            ChunkEmbedding(
                chunk_id=ch.id,
                model_version="bge-m3",
                dim=dim,
                vector=vec.astype("float32").tobytes(),
            )
        )
    db.add_all(embeds)
    await db.commit()

    return {"document_id": doc.id, "chunks_indexed": len(rows)}
